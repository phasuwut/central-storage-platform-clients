import json
import tempfile
import unittest
from pathlib import Path

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


if __name__ == "__main__":
    unittest.main()
