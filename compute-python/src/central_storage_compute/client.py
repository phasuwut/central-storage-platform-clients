from __future__ import annotations

import hashlib
import json
import os
import tempfile
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import BinaryIO


@dataclass(frozen=True)
class DownloadResult:
    file_id: str
    destination: Path
    bytes_written: int
    sha256: str
    mode: str = "normal"


class ComputeApiError(RuntimeError):
    """Safe API error that never includes a bearer token or presigned URL."""

    def __init__(self, status: int, code: str, message: str) -> None:
        super().__init__(f"Compute API request failed ({status}): {code}")
        self.status = status
        self.code = code
        self.message = message


class _RangeUnsupported(RuntimeError):
    pass


class ComputeClient:
    """Provider-neutral client; credentials and presigned URLs stay out of manifests/logs."""

    def __init__(self, api_url: str, token: str, timeout: float = 30.0, retry_count: int = 3) -> None:
        if not token.startswith("cpt_"):
            raise ValueError("Compute token must use the cpt_ prefix")
        self.api_url = api_url.rstrip("/")
        self._token = token
        self.timeout = timeout
        self.retry_count = max(1, retry_count)

    def download(
        self,
        file_id: str,
        destination: str | os.PathLike[str],
        *,
        verify_sha256: str | None = None,
        mode: str = "auto",
        max_connections: int | None = None,
        resume: bool = True,
    ) -> DownloadResult:
        if mode not in {"auto", "normal", "parallel"}:
            raise ValueError("mode must be auto, normal, or parallel")
        payload = self._request("POST", f"/api/v1/compute/files/{file_id}/download")
        target = Path(destination)
        target.parent.mkdir(parents=True, exist_ok=True)
        expected_size = _as_int(payload.get("sizeBytes"))
        configured_mode = str(payload.get("transferMode") or "NORMAL").lower()
        connections = max(1, min(int(max_connections or payload.get("maxConcurrency") or 1), 32))
        selected_mode = "parallel" if mode == "parallel" or (mode == "auto" and configured_mode == "parallel") else "normal"
        if expected_size is None or expected_size < 1 or connections < 2:
            selected_mode = "normal"
        if selected_mode == "parallel":
            try:
                return self._download_parallel(file_id, target, str(payload["url"]), expected_size, verify_sha256, connections, resume)
            except _RangeUnsupported:
                selected_mode = "normal"
        result = self._download_normal(file_id, target, str(payload["url"]), expected_size, verify_sha256)
        return DownloadResult(result.file_id, result.destination, result.bytes_written, result.sha256, selected_mode)

    def benchmark(self, file_id: str, *, sample_path: str | os.PathLike[str] | None = None) -> dict[str, float | int | str]:
        scratch = Path(sample_path) if sample_path else Path(tempfile.gettempdir()) / f"csp-benchmark-{file_id}"
        started = time.perf_counter()
        result = self.download(file_id, scratch, mode="normal", resume=False)
        duration = max(time.perf_counter() - started, 0.000001)
        if sample_path is None:
            scratch.unlink(missing_ok=True)
        return {"fileId": result.file_id, "bytes": result.bytes_written, "seconds": duration, "bytesPerSecond": result.bytes_written / duration, "sha256": result.sha256, "mode": result.mode}

    def _download_normal(self, file_id: str, target: Path, url: str, expected_size: int | None, verify_sha256: str | None) -> DownloadResult:
        last_error: Exception | None = None
        for attempt in range(self.retry_count):
            try:
                if attempt:
                    payload = self._request("POST", f"/api/v1/compute/files/{file_id}/download")
                    url = str(payload["url"])
                digest = hashlib.sha256()
                with urllib.request.urlopen(urllib.request.Request(url), timeout=self.timeout) as response, target.open("wb") as output:
                    count = self._stream(response, output, digest)
                self._verify_size_and_checksum(count, digest.hexdigest(), expected_size, verify_sha256)
                return DownloadResult(file_id=file_id, destination=target, bytes_written=count, sha256=digest.hexdigest())
            except (urllib.error.URLError, TimeoutError, OSError, ComputeApiError) as error:
                last_error = error
                if attempt + 1 < self.retry_count:
                    time.sleep(min(2**attempt, 4))
        raise RuntimeError("Compute download failed after retries") from last_error

    def _download_parallel(self, file_id: str, target: Path, url: str, expected_size: int, verify_sha256: str | None, connections: int, resume: bool) -> DownloadResult:
        manifest = target.with_name(f".{target.name}.csp-resume.json")
        chunk_size = max(1 * 1024 * 1024, min(64 * 1024 * 1024, (expected_size + connections * 8 - 1) // (connections * 8)))
        chunks = [(start, min(start + chunk_size, expected_size) - 1) for start in range(0, expected_size, chunk_size)]
        completed = self._load_manifest(manifest, file_id, expected_size, verify_sha256) if resume else set()
        if not target.exists() or target.stat().st_size != expected_size:
            with target.open("wb") as output:
                output.truncate(expected_size)
            completed = set()
        write_lock = Lock()

        def fetch(chunk: tuple[int, int]) -> tuple[int, int]:
            if chunk in completed:
                return chunk
            start, end = chunk
            last_error: Exception | None = None
            for attempt in range(self.retry_count):
                try:
                    request = urllib.request.Request(url, headers={"Range": f"bytes={start}-{end}"})
                    with urllib.request.urlopen(request, timeout=self.timeout) as response:
                        if response.status != 206:
                            raise _RangeUnsupported()
                        content_range = response.headers.get("Content-Range", "")
                        if not content_range.startswith(f"bytes {start}-{end}/"):
                            raise _RangeUnsupported()
                        body = response.read(end - start + 1)
                    if len(body) != end - start + 1:
                        raise IOError("Range response length mismatch")
                    with write_lock, target.open("r+b") as output:
                        output.seek(start)
                        output.write(body)
                    return chunk
                except _RangeUnsupported:
                    raise
                except (urllib.error.URLError, TimeoutError, OSError) as error:
                    last_error = error
                    if attempt + 1 < self.retry_count:
                        time.sleep(min(2**attempt, 4))
            raise RuntimeError("Range download failed after retries") from last_error

        with ThreadPoolExecutor(max_workers=connections) as pool:
            futures = [pool.submit(fetch, chunk) for chunk in chunks if chunk not in completed]
            for future in as_completed(futures):
                completed.add(future.result())
                self._save_manifest(manifest, file_id, expected_size, verify_sha256, completed)
        digest = hashlib.sha256()
        count = 0
        with target.open("rb") as output:
            count = self._stream(output, None, digest)
        self._verify_size_and_checksum(count, digest.hexdigest(), expected_size, verify_sha256)
        manifest.unlink(missing_ok=True)
        return DownloadResult(file_id=file_id, destination=target, bytes_written=count, sha256=digest.hexdigest(), mode="parallel")

    @staticmethod
    def _stream(response: BinaryIO, output: BinaryIO | None, digest: "hashlib._Hash") -> int:
        count = 0
        while chunk := response.read(1024 * 1024):
            if output is not None:
                output.write(chunk)
            digest.update(chunk)
            count += len(chunk)
        return count

    @staticmethod
    def _verify_size_and_checksum(count: int, digest: str, expected_size: int | None, verify_sha256: str | None) -> None:
        if expected_size is not None and count != expected_size:
            raise ValueError(f"Downloaded file size does not match expected size ({expected_size})")
        if verify_sha256 and digest.lower() != verify_sha256.lower():
            raise ValueError("Downloaded file checksum does not match")

    @staticmethod
    def _load_manifest(path: Path, file_id: str, expected_size: int, checksum: str | None) -> set[tuple[int, int]]:
        try:
            value = json.loads(path.read_text())
            if value.get("fileId") != file_id or value.get("sizeBytes") != expected_size or value.get("checksum") != checksum:
                return set()
            return {(int(item[0]), int(item[1])) for item in value.get("completed", [])}
        except (OSError, ValueError, TypeError, KeyError):
            return set()

    @staticmethod
    def _save_manifest(path: Path, file_id: str, expected_size: int, checksum: str | None, completed: set[tuple[int, int]]) -> None:
        value = {"fileId": file_id, "sizeBytes": expected_size, "checksum": checksum, "completed": sorted([list(item) for item in completed])}
        path.write_text(json.dumps(value, separators=(",", ":")))

    def _request(self, method: str, path: str, body: dict[str, object] | None = None, idempotency_key: str | None = None) -> dict[str, object]:
        headers = {"Authorization": f"Bearer {self._token}", "Accept": "application/json"}
        request = urllib.request.Request(f"{self.api_url}{path}", method=method, headers=headers)
        if body is not None:
            request.data = json.dumps(body).encode()
            request.add_header("Content-Type", "application/json")
        if idempotency_key:
            request.add_header("Idempotency-Key", idempotency_key)
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                value = json.load(response)
                if not isinstance(value, dict):
                    raise RuntimeError("Compute API returned an invalid response")
                return value
        except urllib.error.HTTPError as error:
            try:
                payload = json.load(error)
                envelope = payload.get("error", {}) if isinstance(payload, dict) else {}
                code = str(envelope.get("code", "HTTP_ERROR")) if isinstance(envelope, dict) else "HTTP_ERROR"
                message = str(envelope.get("message", "Request failed")) if isinstance(envelope, dict) else "Request failed"
            except (ValueError, OSError):
                code, message = "HTTP_ERROR", "Request failed"
            raise ComputeApiError(error.code, code, message) from error


def _as_int(value: object) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None
