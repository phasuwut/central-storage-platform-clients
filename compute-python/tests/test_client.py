import json
import tempfile
import unittest
from io import BytesIO
from pathlib import Path
from unittest.mock import Mock, patch
from urllib.error import HTTPError

from central_storage_compute.client import ComputeClient


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

    def test_benchmark_respects_connection_limit_and_prefers_fewer_on_tie(self) -> None:
        client = ComputeClient("https://api.example.invalid", "cpt_token.secret")
        client.download = Mock(side_effect=lambda _file_id, destination, **kwargs: type("Result", (), {"file_id": "file-1", "destination": Path(destination), "bytes_written": 100, "sha256": "digest", "mode": "normal" if kwargs["max_connections"] == 1 else "parallel"})())

        with patch("central_storage_compute.client.time.perf_counter", side_effect=[0.0, 1.0, 10.0, 11.0]):
            result = client.benchmark("file-1", max_connections=4)

        self.assertEqual([run["connections"] for run in result["runs"]], [1, 4])
        self.assertEqual(result["recommendedConcurrency"], 1)


if __name__ == "__main__":
    unittest.main()
