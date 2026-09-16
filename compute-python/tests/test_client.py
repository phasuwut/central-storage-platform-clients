import json
import tempfile
import unittest
from io import BytesIO
from pathlib import Path
from unittest.mock import Mock, patch
from urllib.error import HTTPError

from central_storage_compute.client import (
    ComputeApiError,
    ComputeClient,
    _KeepAliveConnections,
    _PartSigner,
    _StorageRequestError,
)


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
        pool = _KeepAliveConnections(client.timeout)

        with patch("central_storage_compute.client.http.client.HTTPSConnection") as factory:
            pool.connection("https", "s3.example", None)

        self.assertEqual(factory.call_args.kwargs["timeout"], 600)

    def test_a_part_is_streamed_rather_than_buffered(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "output.bin"
            source.write_bytes(b"abcdefghij")
            client = ComputeClient("https://api.example.invalid", "cpt_token.secret")
            client.STREAM_BLOCK_BYTES = 2
            connection = Mock()
            connection.getresponse.return_value = Mock(status=200, reason="OK", headers={"ETag": '"etag-1"'}, will_close=False, read=Mock(return_value=b""))
            pool = Mock()
            pool.connection.return_value = connection

            etag = client._put_range("https://s3.example/1?sig=x", source, 4, 5, pool)

            self.assertEqual(etag, "etag-1")
            self.assertEqual(b"".join(call.args[0] for call in connection.send.call_args_list), b"efghi")
            self.assertEqual(connection.putheader.call_args.args, ("Content-Length", "5"))
            # Nothing larger than a block is ever resident, whatever the part size.
            self.assertTrue(all(len(call.args[0]) <= 2 for call in connection.send.call_args_list))

    def test_a_reusable_connection_is_kept_for_the_next_part(self) -> None:
        pool = _KeepAliveConnections(60.0)
        with patch("central_storage_compute.client.http.client.HTTPSConnection") as factory:
            first = pool.connection("https", "s3.example", None)
            second = pool.connection("https", "s3.example", None)
            self.assertIs(first, second)
            self.assertEqual(factory.call_count, 1)
            pool.discard()
            pool.connection("https", "s3.example", None)
            self.assertEqual(factory.call_count, 2)

    def test_a_closing_response_retires_the_connection(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "output.bin"
            source.write_bytes(b"abcde")
            client = ComputeClient("https://api.example.invalid", "cpt_token.secret")
            connection = Mock()
            connection.getresponse.return_value = Mock(status=200, reason="OK", headers={"ETag": '"etag-1"'}, will_close=True, read=Mock(return_value=b""))
            pool = Mock()
            pool.connection.return_value = connection

            client._put_range("https://s3.example/1", source, 0, 5, pool)

            pool.discard.assert_called_once()

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
            client._put_range = Mock(side_effect=lambda url, *_args: "etag-1" if url.endswith("/1") else "etag-2")

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


            def respond(_method: str, path: str, *_args: object, **_kwargs: object) -> dict[str, object]:
                if path.endswith("/parts/sign"):
                    return {"parts": [{"partNumber": 1, "uploadUrl": "https://s3.example/1"}], "expiresIn": 900}
                return {"uploadId": "upload-1", "status": "CANCELLED"}

            client._request = Mock(side_effect=respond)
            client._put_range = Mock(side_effect=OSError("connection reset"))

            with patch("central_storage_compute.client.time.sleep"), self.assertRaises(RuntimeError):
                client._upload_multipart(source, {"uploadId": "upload-1", "partSizeBytes": 8, "totalParts": 1, "maxConcurrency": 1}, None, "digest", 1)

            paths = [call.args[1] for call in client._request.call_args_list]
            self.assertEqual(paths[-1], "/api/v1/compute/uploads/upload-1/multipart/abort")
            # Every retry starts from a freshly signed URL, so a signature that
            # expired mid-transfer is never replayed.
            self.assertEqual(sum(1 for path in paths if path.endswith("/parts/sign")), client.retry_count)

    def test_benchmark_respects_connection_limit_and_prefers_fewer_on_tie(self) -> None:
        client = ComputeClient("https://api.example.invalid", "cpt_token.secret")
        client.download = Mock(side_effect=lambda _file_id, destination, **kwargs: type("Result", (), {"file_id": "file-1", "destination": Path(destination), "bytes_written": 100, "sha256": "digest", "mode": "normal" if kwargs["max_connections"] == 1 else "parallel"})())

        with patch("central_storage_compute.client.time.perf_counter", side_effect=[0.0, 1.0, 10.0, 11.0]):
            result = client.benchmark("file-1", max_connections=4)

        self.assertEqual([run["connections"] for run in result["runs"]], [1, 4])
        self.assertEqual(result["recommendedConcurrency"], 1)


class PartSignerTests(unittest.TestCase):
    def _signer(self, client: ComputeClient, total_parts: int = 3, expires_in: float = 900.0) -> _PartSigner:
        return _PartSigner(client, "upload-1", total_parts, expires_in)

    def test_parts_are_signed_in_one_batch_and_reused(self) -> None:
        client = ComputeClient("https://api.example.invalid", "cpt_token.secret")
        signer = self._signer(client)
        with patch.object(
            client,
            "_request",
            return_value={"parts": [{"partNumber": n, "uploadUrl": f"https://s3.example/{n}"} for n in (1, 2, 3)], "expiresIn": 900},
        ) as request:
            self.assertEqual(signer.url(1), "https://s3.example/1")
            self.assertEqual(signer.url(2), "https://s3.example/2")
            self.assertEqual(signer.url(3), "https://s3.example/3")
        self.assertEqual(request.call_count, 1)

    def test_a_rejected_url_is_replaced_rather_than_retried(self) -> None:
        client = ComputeClient("https://api.example.invalid", "cpt_token.secret")
        signer = self._signer(client, total_parts=1)
        responses = [
            {"parts": [{"partNumber": 1, "uploadUrl": "https://s3.example/stale"}], "expiresIn": 900},
            {"parts": [{"partNumber": 1, "uploadUrl": "https://s3.example/fresh"}], "expiresIn": 900},
        ]
        with patch.object(client, "_request", side_effect=responses):
            stale = signer.url(1)
            signer.invalidate(1, stale)
            self.assertEqual(signer.url(1), "https://s3.example/fresh")

    def test_invalidate_keeps_a_url_another_worker_already_refreshed(self) -> None:
        client = ComputeClient("https://api.example.invalid", "cpt_token.secret")
        signer = self._signer(client, total_parts=1)
        with patch.object(
            client,
            "_request",
            return_value={"parts": [{"partNumber": 1, "uploadUrl": "https://s3.example/fresh"}], "expiresIn": 900},
        ):
            signer.url(1)
            signer.invalidate(1, "https://s3.example/stale")
            with patch.object(client, "_request", side_effect=AssertionError("must not re-sign")):
                self.assertEqual(signer.url(1), "https://s3.example/fresh")

    def test_a_url_with_too_little_life_left_is_re_signed(self) -> None:
        client = ComputeClient("https://api.example.invalid", "cpt_token.secret")
        # A 150s lifetime is under the 120s floor once a part takes 60s, so the
        # cached URL can never cover another part and must be replaced.
        signer = self._signer(client, total_parts=1, expires_in=150.0)
        signer.observe(60.0)
        with patch.object(
            client,
            "_request",
            return_value={"parts": [{"partNumber": 1, "uploadUrl": "https://s3.example/1"}], "expiresIn": 150},
        ) as request:
            signer.url(1)
            signer.url(1)
        self.assertEqual(request.call_count, 2)

    def test_storage_rejection_is_a_retryable_error(self) -> None:
        client = ComputeClient("https://api.example.invalid", "cpt_token.secret")
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "output.bin"
            source.write_bytes(b"abcde")
            connection = Mock()
            connection.getresponse.return_value = Mock(
                status=403, reason="Forbidden", headers={}, will_close=False,
                read=Mock(return_value=b"<Error><Code>AccessDenied</Code></Error>"),
            )
            pool = Mock()
            pool.connection.return_value = connection

            with self.assertRaisesRegex(_StorageRequestError, "AccessDenied"):
                client._put_range("https://s3.example/1", source, 0, 5, pool)


class UploadResumeTests(unittest.TestCase):
    def _client(self) -> ComputeClient:
        return ComputeClient("https://api.example.invalid", "cpt_token.secret")

    def test_only_the_missing_parts_are_re_sent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "output.bin"
            source.write_bytes(b"abcdef")
            client = self._client()
            manifest = client._upload_manifest_path(source)

            def respond(_method: str, path: str, *_args: object, **_kwargs: object) -> dict[str, object]:
                if path.endswith("/parts/sign"):
                    return {"parts": [{"partNumber": 2, "uploadUrl": "https://s3.example/2"}], "expiresIn": 900}
                return {"fileId": "file-1"}

            client._request = Mock(side_effect=respond)
            client._put_range = Mock(return_value="etag-2")

            result = client._upload_multipart(
                source,
                {"uploadId": "upload-1", "partSizeBytes": 3, "totalParts": 2, "maxConcurrency": 1},
                None,
                "digest",
                1,
                {1: "etag-1"},
                manifest,
            )

            self.assertEqual(result.file_id, "file-1")
            # Part 1 was already in S3, so only part 2 moved.
            self.assertEqual(client._put_range.call_count, 1)
            completion = [call for call in client._request.call_args_list if call.args[1].endswith("/multipart/complete")][0]
            self.assertEqual(completion.args[2]["parts"], [{"partNumber": 1, "etag": "etag-1"}, {"partNumber": 2, "etag": "etag-2"}])

    def test_a_resumable_failure_keeps_the_upload_instead_of_aborting(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "output.bin"
            source.write_bytes(b"abcde")
            client = self._client()
            manifest = client._upload_manifest_path(source)
            client._request = Mock(return_value={"parts": [{"partNumber": 1, "uploadUrl": "https://s3.example/1"}], "expiresIn": 900})
            client._put_range = Mock(side_effect=OSError("connection reset"))

            with patch("central_storage_compute.client.time.sleep"), self.assertRaisesRegex(RuntimeError, "resume"):
                client._upload_multipart(source, {"uploadId": "upload-1", "partSizeBytes": 8, "totalParts": 1, "maxConcurrency": 1}, None, "digest", 1, {}, manifest)

            paths = [call.args[1] for call in client._request.call_args_list]
            self.assertNotIn("/api/v1/compute/uploads/upload-1/multipart/abort", paths)

    def test_a_completed_upload_clears_its_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "output.bin"
            source.write_bytes(b"abc")
            client = self._client()
            manifest = client._upload_manifest_path(source)
            client._save_upload_manifest(manifest, "upload-1", 3, "digest")
            client._request = Mock(side_effect=lambda _m, path, *_a, **_k: {"parts": [{"partNumber": 1, "uploadUrl": "https://s3.example/1"}], "expiresIn": 900} if path.endswith("/parts/sign") else {"fileId": "file-1"})
            client._put_range = Mock(return_value="etag-1")

            client._upload_multipart(source, {"uploadId": "upload-1", "partSizeBytes": 8, "totalParts": 1, "maxConcurrency": 1}, None, "digest", 1, {}, manifest)

            self.assertFalse(manifest.exists())

    def test_a_manifest_for_a_different_file_is_discarded(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "output.bin"
            source.write_bytes(b"abc")
            client = self._client()
            manifest = client._upload_manifest_path(source)
            client._save_upload_manifest(manifest, "upload-1", 3, "old-digest")
            client._request = Mock(side_effect=AssertionError("must not reach the API"))

            self.assertIsNone(client._resume_multipart(manifest, 3, "new-digest"))
            self.assertFalse(manifest.exists())

    def test_resume_asks_the_api_which_parts_survived_and_renews(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "output.bin"
            source.write_bytes(b"abcdef")
            client = self._client()
            manifest = client._upload_manifest_path(source)
            client._save_upload_manifest(manifest, "upload-1", 6, "digest")
            client._request = Mock(side_effect=[
                {"totalParts": 2, "partSizeBytes": 3, "parts": [{"partNumber": 1, "etag": "etag-1"}, {"partNumber": 2, "etag": None}]},
                {"uploadId": "upload-1", "status": "UPLOADING"},
            ])

            resumed = client._resume_multipart(manifest, 6, "digest")

            self.assertIsNotNone(resumed)
            payload, completed = resumed
            self.assertEqual(payload["uploadId"], "upload-1")
            self.assertEqual(completed, {1: "etag-1"})
            self.assertEqual([call.args[1] for call in client._request.call_args_list][-1], "/api/v1/compute/uploads/upload-1/renew")
            self.assertEqual(client._request.call_args_list[-1].args[0], "POST")

    def test_an_upload_the_api_no_longer_knows_starts_over(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "output.bin"
            source.write_bytes(b"abc")
            client = self._client()
            manifest = client._upload_manifest_path(source)
            client._save_upload_manifest(manifest, "upload-1", 3, "digest")
            client._request = Mock(side_effect=ComputeApiError(404, "MULTIPART_NOT_FOUND", "gone"))

            self.assertIsNone(client._resume_multipart(manifest, 3, "digest"))
            self.assertFalse(manifest.exists())

    def test_a_manifest_holds_no_secrets(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manifest = Path(directory) / ".output.bin.csp-upload.json"
            ComputeClient._save_upload_manifest(manifest, "upload-1", 10, "digest")
            recorded = manifest.read_text()
            self.assertEqual(json.loads(recorded), {"uploadId": "upload-1", "sizeBytes": 10, "sha256": "digest"})
            self.assertNotIn("cpt_", recorded)
            self.assertNotIn("X-Amz-Signature", recorded)


if __name__ == "__main__":
    unittest.main()
