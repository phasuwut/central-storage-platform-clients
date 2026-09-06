import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

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


if __name__ == "__main__":
    unittest.main()
