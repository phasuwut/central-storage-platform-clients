import json
import tempfile
import unittest
from io import BytesIO
from pathlib import Path
from unittest.mock import Mock, patch
from urllib.error import HTTPError

from central_storage_compute.client import ComputeApiError, ComputeClient


class ComputeClientTests(unittest.TestCase):
    def test_manifest_contains_identity_only(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "resume.json"
            ComputeClient._save_manifest(path, "file-1", 10, "checksum", {(0, 9)})
            value = json.loads(path.read_text())
            self.assertEqual(value, {"fileId": "file-1", "sizeBytes": 10, "checksum": "checksum", "completed": [[0, 9]]})
            self.assertNotIn("cpt_secret", path.read_text())
            self.assertNotIn("X-Amz-Signature", path.read_text())

    def test_manifest_is_discarded_when_identity_changes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "resume.json"
            ComputeClient._save_manifest(path, "file-1", 10, None, {(0, 9)})
            self.assertEqual(ComputeClient._load_manifest(path, "file-2", 10, None), set())
            self.assertEqual(ComputeClient._load_manifest(path, "file-1", 11, None), set())

    def test_token_prefix_is_required(self) -> None:
        with self.assertRaises(ValueError):
            ComputeClient("https://api.example.invalid", "secret")

    def test_default_timeout_allows_slow_multipart_completion(self) -> None:
        client = ComputeClient("https://api.example.invalid", "cpt_token.secret")
        self.assertEqual(client.timeout, 3_600.0)

    def test_timeout_must_be_positive(self) -> None:
        with self.assertRaisesRegex(ValueError, "timeout must be greater than zero"):
            ComputeClient("https://api.example.invalid", "cpt_token.secret", timeout=0)

    def test_multipart_part_uses_the_configured_timeout(self) -> None:
        client = ComputeClient("https://api.example.invalid", "cpt_token.secret", timeout=600)
        response = Mock(status=200, headers={})
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=None)

        with patch("central_storage_compute.client.urllib.request.urlopen", return_value=response) as open_url:
            self.assertEqual(client._put_bytes("https://s3.example/part", b"data"), "")

        self.assertEqual(open_url.call_args.kwargs["timeout"], 600)

    def test_api_url_normalises_a_common_api_prefix(self) -> None:
        client = ComputeClient("https://api.example.invalid/api/v1", "cpt_token.secret")
        self.assertEqual(client.api_url, "https://api.example.invalid")

    def test_api_url_rejects_compute_route_or_wildcard(self) -> None:
        with self.assertRaisesRegex(ValueError, "do not include /api/v1/compute routes or wildcards"):
            ComputeClient("https://api.example.invalid/api/v1/compute/*", "cpt_token.secret")

    def test_structured_http_error_keeps_safe_api_details_and_request_id(self) -> None:
        client = ComputeClient("https://api.example.invalid", "cpt_token.secret")
        response = HTTPError(
            "https://api.example.invalid/api/v1/compute/uploads/create",
            403,
            "Forbidden",
            {"X-Request-Id": "req-header"},
            BytesIO(b'{"error":{"code":"CSRF_ORIGIN_MISMATCH","message":"Origin is not allowed","requestId":"req-body"}}'),
        )

        with patch("central_storage_compute.client.urllib.request.urlopen", side_effect=response):
            with self.assertRaisesRegex(Exception, "CSRF_ORIGIN_MISMATCH.*Origin is not allowed.*req-body") as raised:
                client._request("POST", "/api/v1/compute/uploads/create", {})

        self.assertEqual(raised.exception.status, 403)
        self.assertEqual(raised.exception.code, "CSRF_ORIGIN_MISMATCH")
        self.assertEqual(raised.exception.request_id, "req-body")

    def test_unstructured_forbidden_response_is_actionable_without_leaking_body(self) -> None:
        client = ComputeClient("https://api.example.invalid", "cpt_token.secret")
        response = HTTPError("https://api.example.invalid", 403, "Forbidden", {}, BytesIO(b"<html>blocked</html>"))

        with patch("central_storage_compute.client.urllib.request.urlopen", side_effect=response):
            with self.assertRaisesRegex(Exception, "Request was forbidden; verify the API URL") as raised:
                client._request("POST", "/api/v1/compute/uploads/create", {})

        self.assertEqual(raised.exception.code, "HTTP_ERROR")

    def test_single_upload_reports_safe_s3_error_code_and_request_id(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "output.txt"
            source.write_text("hello")
            client = ComputeClient("https://api.example.invalid", "cpt_token.secret")
            response = Mock(
                status=403,
                reason="Forbidden",
                headers={"x-amz-request-id": "header-id"},
            )
            response.read.return_value = b"<Error><Code>AccessDenied</Code><RequestId>body-id</RequestId><HostId>secret</HostId></Error>"
            connection = Mock()
            connection.getresponse.return_value = response

            with patch("central_storage_compute.client.http.client.HTTPSConnection", return_value=connection):
                with self.assertRaisesRegex(RuntimeError, "Compute upload failed \\(403\\): AccessDenied.*body-id") as raised:
                    client._put_file("https://bucket.s3.example/presigned-secret-url", source, "", None)

            self.assertNotIn("presigned-secret-url", str(raised.exception))

    def test_single_upload_streams_fixed_length_body_without_chunked_transfer(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "output.txt"
            source.write_text("hello")
            client = ComputeClient("https://api.example.invalid", "cpt_token.secret")
            response = Mock(status=200, reason="OK", headers={})
            response.read.return_value = b""
            connection = Mock()
            connection.getresponse.return_value = response

            with patch("central_storage_compute.client.http.client.HTTPSConnection", return_value=connection):
                client._put_file("https://bucket.s3.example/presigned-secret-url", source, "text/plain", None)

            connection.putheader.assert_any_call("Content-Length", "5")
            connection.putheader.assert_any_call("Content-Type", "text/plain")
            connection.send.assert_called_once_with(b"hello")
            connection.close.assert_called_once()

    def test_single_upload_uses_compute_namespace_and_completion_key(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "output.txt"
            source.write_text("hello")
            client = ComputeClient("https://api.example.invalid", "cpt_token.secret")
            client._request = Mock(side_effect=[{"uploadId": "upload-1", "mode": "single", "uploadUrl": "https://s3.example/put", "headers": {}}, {"fileId": "file-1"}])
            client._put_file = Mock()

            result = client.upload(source, mode="single")

            self.assertEqual(result.file_id, "file-1")
            self.assertEqual(result.bytes_read, 5)
            self.assertEqual(result.mode, "single")
            self.assertNotIn("checksum", client._request.call_args_list[0].args[2])
            self.assertEqual(client._request.call_args_list[1].args[1], "/api/v1/compute/uploads/upload-1/complete")
            self.assertIn("idempotency_key", client._request.call_args_list[1].kwargs)

    def test_multipart_upload_retries_parts_and_completes_in_order(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "output.bin"
            source.write_bytes(b"abcde")
            client = ComputeClient("https://api.example.invalid", "cpt_token.secret")
            client._request = Mock(side_effect=[{"parts": [{"partNumber": 1, "uploadUrl": "https://s3.example/1"}, {"partNumber": 2, "uploadUrl": "https://s3.example/2"}]}, {"fileId": "file-1"}])
            client._put_bytes = Mock(side_effect=lambda url, _body: "etag-1" if url.endswith("/1") else "etag-2")

            result = client._upload_multipart(source, {"uploadId": "upload-1", "partSizeBytes": 3, "totalParts": 2, "maxConcurrency": 2}, None, "digest", 2)

            self.assertEqual(result.mode, "multipart")
            completion = client._request.call_args_list[1].args[2]
            self.assertEqual([part["partNumber"] for part in completion["parts"]], [1, 2])
            self.assertEqual([part["etag"] for part in completion["parts"]], ["etag-1", "etag-2"])

    def test_completion_replays_the_same_key_after_a_read_timeout(self) -> None:
        """A slow server-side finalization must not look like a failed upload."""
        client = ComputeClient("https://api.example.invalid", "cpt_token.secret")
        client._request = Mock(side_effect=[TimeoutError("read timed out"), {"fileId": "file-1"}])

        with patch("central_storage_compute.client.time.sleep") as sleep:
            result = client._complete("/api/v1/compute/uploads/upload-1/multipart/complete", "upload-1", {}, abort=True)

        self.assertEqual(result, {"fileId": "file-1"})
        self.assertEqual(sleep.call_count, 1)
        keys = {call.kwargs["idempotency_key"] for call in client._request.call_args_list}
        self.assertEqual(len(keys), 1, "the replay must reuse the original Idempotency-Key")
        self.assertNotIn("/multipart/abort", [call.args[1] for call in client._request.call_args_list])

    def test_completion_waits_while_the_api_reports_work_in_progress(self) -> None:
        client = ComputeClient("https://api.example.invalid", "cpt_token.secret")
        in_progress = ComputeApiError(409, "IDEMPOTENCY_IN_PROGRESS", "still working")
        client._request = Mock(side_effect=[in_progress, {"fileId": "file-1"}])

        with patch("central_storage_compute.client.time.sleep"):
            result = client._complete("/api/v1/compute/uploads/upload-1/multipart/complete", "upload-1", {}, abort=True)

        self.assertEqual(result, {"fileId": "file-1"})
        self.assertNotIn("/multipart/abort", [call.args[1] for call in client._request.call_args_list])

    def test_completion_aborts_only_when_the_api_rejects_the_upload(self) -> None:
        client = ComputeClient("https://api.example.invalid", "cpt_token.secret")
        rejection = ComputeApiError(409, "MULTIPART_PARTS_INCOMPLETE", "All multipart parts are required")
        client._request = Mock(side_effect=[rejection, {"uploadId": "upload-1", "status": "CANCELLED"}])

        with self.assertRaises(ComputeApiError):
            client._complete("/api/v1/compute/uploads/upload-1/multipart/complete", "upload-1", {}, abort=True)

        self.assertEqual(client._request.call_args_list[1].args[1], "/api/v1/compute/uploads/upload-1/multipart/abort")

    def test_completion_gives_up_without_discarding_the_uploaded_parts(self) -> None:
        client = ComputeClient("https://api.example.invalid", "cpt_token.secret", timeout=0.01)
        client._request = Mock(side_effect=TimeoutError("read timed out"))

        with patch("central_storage_compute.client.time.sleep"), self.assertRaisesRegex(TimeoutError, "still finalizing"):
            client._complete("/api/v1/compute/uploads/upload-1/multipart/complete", "upload-1", {}, abort=True)

        self.assertNotIn("/multipart/abort", [call.args[1] for call in client._request.call_args_list])

    def test_multipart_aborts_when_a_part_upload_fails(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "output.bin"
            source.write_bytes(b"abcde")
            client = ComputeClient("https://api.example.invalid", "cpt_token.secret")
            client._request = Mock(side_effect=[{"parts": [{"partNumber": 1, "uploadUrl": "https://s3.example/1"}]}, {"uploadId": "upload-1", "status": "CANCELLED"}])
            client._put_bytes = Mock(side_effect=OSError("connection reset"))

            with patch("central_storage_compute.client.time.sleep"), self.assertRaises(RuntimeError):
                client._upload_multipart(source, {"uploadId": "upload-1", "partSizeBytes": 8, "totalParts": 1, "maxConcurrency": 1}, None, "digest", 1)

            self.assertEqual(client._request.call_args_list[-1].args[1], "/api/v1/compute/uploads/upload-1/multipart/abort")

    def test_benchmark_respects_connection_limit_and_prefers_fewer_on_tie(self) -> None:
        client = ComputeClient("https://api.example.invalid", "cpt_token.secret")
        client.download = Mock(side_effect=lambda _file_id, destination, **kwargs: type("Result", (), {"file_id": "file-1", "destination": Path(destination), "bytes_written": 100, "sha256": "digest", "mode": "normal" if kwargs["max_connections"] == 1 else "parallel"})())

        with patch("central_storage_compute.client.time.perf_counter", side_effect=[0.0, 1.0, 10.0, 11.0]):
            result = client.benchmark("file-1", max_connections=4)

        self.assertEqual([run["connections"] for run in result["runs"]], [1, 4])
        self.assertEqual(result["recommendedConcurrency"], 1)


if __name__ == "__main__":
    unittest.main()
