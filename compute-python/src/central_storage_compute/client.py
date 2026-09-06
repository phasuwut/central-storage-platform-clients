from __future__ import annotations

import hashlib
import json
import os
import tempfile
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO


@dataclass(frozen=True)
class DownloadResult:
    file_id: str
    destination: Path
    bytes_written: int
    sha256: str


class ComputeClient:
    """Small standard-library client; tokens and presigned URLs stay out of manifests/logs."""

    def __init__(self, api_url: str, token: str, timeout: float = 30.0) -> None:
        self.api_url = api_url.rstrip("/")
        self._token = token
        self.timeout = timeout

    def download(self, file_id: str, destination: str | os.PathLike[str], *, verify_sha256: str | None = None) -> DownloadResult:
        payload = self._request("POST", f"/api/v1/compute/files/{file_id}/download")
        url = payload["url"]
        target = Path(destination)
        target.parent.mkdir(parents=True, exist_ok=True)
        digest = hashlib.sha256()
        count = 0
        with urllib.request.urlopen(urllib.request.Request(url), timeout=self.timeout) as response, target.open("wb") as output:
            count = self._stream(response, output, digest)
        sha256 = digest.hexdigest()
        if verify_sha256 and sha256.lower() != verify_sha256.lower():
            raise ValueError("Downloaded file checksum does not match")
        return DownloadResult(file_id=file_id, destination=target, bytes_written=count, sha256=sha256)

    def benchmark(self, file_id: str, *, sample_path: str | os.PathLike[str] | None = None) -> dict[str, float | int | str]:
        scratch = Path(sample_path) if sample_path else Path(tempfile.gettempdir()) / f"csp-benchmark-{file_id}"
        started = time.perf_counter()
        result = self.download(file_id, scratch)
        duration = max(time.perf_counter() - started, 0.000001)
        if sample_path is None:
            scratch.unlink(missing_ok=True)
        return {"fileId": result.file_id, "bytes": result.bytes_written, "seconds": duration, "bytesPerSecond": result.bytes_written / duration, "sha256": result.sha256}

    def _request(self, method: str, path: str, body: dict[str, object] | None = None) -> dict[str, object]:
        request = urllib.request.Request(f"{self.api_url}{path}", method=method, headers={"Authorization": f"Bearer {self._token}", "Accept": "application/json"})
        if body is not None:
            request.data = json.dumps(body).encode()
            request.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                return json.load(response)
        except urllib.error.HTTPError as error:
            raise RuntimeError(f"Compute API request failed ({error.code})") from error

    @staticmethod
    def _stream(response: BinaryIO, output: BinaryIO, digest: hashlib._Hash) -> int:
        count = 0
        while chunk := response.read(1024 * 1024):
            output.write(chunk)
            digest.update(chunk)
            count += len(chunk)
        return count
