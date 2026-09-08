from __future__ import annotations

import hashlib
import json
import base64
import mimetypes
import os
import tempfile
import time
import urllib.error
import urllib.request
from urllib.parse import urlsplit, urlunsplit
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import BinaryIO
from uuid import uuid4


@dataclass(frozen=True)
class DownloadResult:
    file_id: str
    destination: Path
    bytes_written: int
    sha256: str
    mode: str = "normal"


@dataclass(frozen=True)
class UploadResult:
    upload_id: str
    file_id: str
    source: Path
    bytes_read: int
    sha256: str
    mode: str


class ComputeApiError(RuntimeError):
    """Safe API error that never includes a bearer token or presigned URL."""

    def __init__(self, status: int, code: str, message: str, request_id: str | None = None) -> None:
        detail = f"Compute API request failed ({status}): {code} — {message}"
        super().__init__(f"{detail} [request ID: {request_id}]" if request_id else detail)
        self.status = status
        self.code = code
        self.message = message
        self.request_id = request_id


class _RangeUnsupported(RuntimeError):
    pass


class ComputeClient:
    """Provider-neutral client; credentials and presigned URLs stay out of manifests/logs."""

    def __init__(self, api_url: str, token: str, timeout: float = 30.0, retry_count: int = 3) -> None:
        if not token.startswith("cpt_"):
            raise ValueError("Compute token must use the cpt_ prefix")
        self.api_url = _normalise_api_url(api_url)
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

    def benchmark(self, file_id: str, *, sample_path: str | os.PathLike[str] | None = None, max_connections: int | None = None) -> dict[str, float | int | str | list[dict[str, float | int | str]]]:
        allowed = max(1, min(int(max_connections or 8), 32))
        candidates = [connections for connections in (1, 4, 8) if connections <= allowed]
        runs: list[dict[str, float | int | str]] = []
        for connections in candidates:
            scratch = Path(sample_path) if sample_path else Path(tempfile.gettempdir()) / f"csp-benchmark-{file_id}-{connections}"
            started = time.perf_counter()
            result = self.download(file_id, scratch, mode="normal" if connections == 1 else "parallel", max_connections=connections, resume=False)
            duration = max(time.perf_counter() - started, 0.000001)
            runs.append({"connections": connections, "bytes": result.bytes_written, "seconds": duration, "bytesPerSecond": result.bytes_written / duration, "mode": result.mode, "sha256": result.sha256})
            if sample_path is None:
                scratch.unlink(missing_ok=True)
        recommended = max(runs, key=lambda run: (float(run["bytesPerSecond"]), -int(run["connections"])))
        return {"fileId": file_id, "bytes": int(recommended["bytes"]), "seconds": float(recommended["seconds"]), "bytesPerSecond": float(recommended["bytesPerSecond"]), "sha256": str(recommended["sha256"]), "mode": str(recommended["mode"]), "recommendedConcurrency": int(recommended["connections"]), "runs": runs}

    def upload(
        self,
        source: str | os.PathLike[str],
        *,
        destination: str | None = None,
        mime_type: str | None = None,
        mode: str = "auto",
        max_connections: int | None = None,
        include_checksum: bool = True,
    ) -> UploadResult:
        if mode not in {"auto", "single", "multipart"}:
            raise ValueError("mode must be auto, single, or multipart")
        path = Path(source)
        size = path.stat().st_size
        digest = _sha256_file(path)
        checksum = _base64_sha256(path) if include_checksum else None
        body: dict[str, object] = {"filename": path.name, "sizeBytes": size}
        if destination:
            body["destination"] = destination
        if mime_type or mimetypes.guess_type(path.name)[0]:
            body["mimeType"] = mime_type or mimetypes.guess_type(path.name)[0]
        if checksum:
            body["checksum"] = checksum
        payload: dict[str, object] | None = None
        if mode in {"auto", "single"}:
            try:
                payload = self._request("POST", "/api/v1/compute/uploads/create", body)
            except ComputeApiError as error:
                if mode == "single" or error.code != "MULTIPART_REQUIRED":
                    raise
        if payload is not None and str(payload.get("mode")) == "single":
            self._put_file(str(payload["uploadUrl"]), path, str(payload.get("headers", {}).get("content-type", "")), checksum)
            completed = self._request("POST", f"/api/v1/compute/uploads/{payload['uploadId']}/complete", {"checksum": checksum} if checksum else {}, idempotency_key=str(uuid4()))
            return UploadResult(str(payload["uploadId"]), str(completed["fileId"]), path, size, digest, "single")
        multipart = payload if payload is not None else self._request("POST", "/api/v1/compute/uploads/multipart/create", body)
        return self._upload_multipart(path, multipart, checksum, digest, max_connections)

    def _upload_multipart(self, path: Path, payload: dict[str, object], checksum: str | None, digest: str, max_connections: int | None) -> UploadResult:
        upload_id = str(payload["uploadId"])
        part_size = _as_int(payload.get("partSizeBytes")) or 8 * 1024 * 1024
        total_parts = _as_int(payload.get("totalParts")) or ((path.stat().st_size + part_size - 1) // part_size)
        concurrency = max(1, min(int(max_connections or payload.get("maxConcurrency") or 4), 32))
        signed: dict[int, str] = {}
        for offset in range(0, total_parts, 500):
            numbers = list(range(offset + 1, min(total_parts, offset + 500) + 1))
            response = self._request("POST", f"/api/v1/compute/uploads/{upload_id}/multipart/parts/sign", {"partNumbers": numbers})
            for item in response.get("parts", []):
                signed[int(item["partNumber"])] = str(item["uploadUrl"])

        def put(part_number: int) -> tuple[int, str]:
            start = (part_number - 1) * part_size
            length = min(part_size, path.stat().st_size - start)
            last_error: Exception | None = None
            for attempt in range(self.retry_count):
                try:
                    with path.open("rb") as source:
                        source.seek(start)
                        body = source.read(length)
                    etag = self._put_bytes(signed[part_number], body)
                    return part_number, etag
                except (urllib.error.URLError, TimeoutError, OSError) as error:
                    last_error = error
                    if attempt + 1 < self.retry_count:
                        time.sleep(min(2**attempt, 4))
            raise RuntimeError(f"Multipart part {part_number} failed") from last_error

        try:
            parts: list[tuple[int, str]] = []
            with ThreadPoolExecutor(max_workers=concurrency) as pool:
                futures = [pool.submit(put, part_number) for part_number in range(1, total_parts + 1)]
                for future in as_completed(futures):
                    parts.append(future.result())
            completed = self._request("POST", f"/api/v1/compute/uploads/{upload_id}/multipart/complete", {"parts": [{"partNumber": number, "etag": etag} for number, etag in sorted(parts)], **({"checksum": checksum} if checksum else {})}, idempotency_key=str(uuid4()))
        except Exception:
            self._request("POST", f"/api/v1/compute/uploads/{upload_id}/multipart/abort", {})
            raise
        return UploadResult(upload_id, str(completed["fileId"]), path, path.stat().st_size, digest, "multipart")

    def _put_file(self, url: str, path: Path, content_type: str, checksum: str | None) -> None:
        headers = {"Content-Length": str(path.stat().st_size)}
        if content_type:
            headers["Content-Type"] = content_type
        if checksum:
            headers["x-amz-checksum-sha256"] = checksum
        request = urllib.request.Request(url, method="PUT", headers=headers)
        with path.open("rb") as source:
            request.data = source
            try:
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    if response.status < 200 or response.status >= 300:
                        raise RuntimeError(f"Compute upload failed ({response.status})")
            except urllib.error.HTTPError as error:
                raise RuntimeError(f"Compute upload failed ({error.code})") from error

    @staticmethod
    def _put_bytes(url: str, body: bytes) -> str:
        request = urllib.request.Request(url, data=body, method="PUT", headers={"Content-Length": str(len(body))})
        try:
            with urllib.request.urlopen(request) as response:
                if response.status < 200 or response.status >= 300:
                    raise RuntimeError(f"Compute multipart upload failed ({response.status})")
                return response.headers.get("ETag", "").strip('"')
        except urllib.error.HTTPError as error:
            raise RuntimeError(f"Compute multipart upload failed ({error.code})") from error

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
            raise _compute_api_error(error) from error


def _as_int(value: object) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _normalise_api_url(value: str) -> str:
    """Accept the API origin, optionally normalising common API path prefixes."""
    parsed = urlsplit(value.strip())
    if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.query or parsed.fragment:
        raise ValueError("api_url must be an http(s) API origin without a query or fragment")
    path = parsed.path.rstrip("/")
    if path not in {"", "/api/v1", "/api/v1/compute"}:
        raise ValueError("api_url must be the API origin; do not include /api/v1/compute routes or wildcards")
    return urlunsplit((parsed.scheme, parsed.netloc, "", "", ""))


def _compute_api_error(error: urllib.error.HTTPError) -> ComputeApiError:
    """Extract only safe, structured API error details from an HTTP failure."""
    code = "HTTP_ERROR"
    message = _default_http_error_message(error.code)
    request_id = _safe_error_text(error.headers.get("X-Request-Id"), "") or None
    try:
        payload = json.loads(error.read().decode("utf-8"))
        envelope = payload.get("error") if isinstance(payload, dict) else None
        detail = envelope if isinstance(envelope, dict) else payload if isinstance(payload, dict) else {}
        code = _safe_error_code(detail.get("code"), code)
        message = _safe_error_text(detail.get("message"), message)
        request_id = _safe_error_text(detail.get("requestId"), request_id or "") or request_id
    except (UnicodeDecodeError, ValueError, OSError):
        pass
    return ComputeApiError(error.code, code, message, request_id)


def _default_http_error_message(status: int) -> str:
    if status == 401:
        return "Compute token is invalid, expired, or revoked"
    if status == 403:
        return "Request was forbidden; verify the API URL and that compute API traffic is allowed"
    return "Request failed"


def _safe_error_code(value: object, fallback: str) -> str:
    text = str(value).strip() if value is not None else ""
    return text if text and len(text) <= 64 and all(char.isupper() or char.isdigit() or char == "_" for char in text) else fallback


def _safe_error_text(value: object, fallback: str) -> str:
    text = " ".join(str(value).split()).strip() if value is not None else ""
    sensitive_markers = ("cpt_", "x-amz-", "http://", "https://")
    if not text or len(text) > 300 or any(marker in text.lower() for marker in sensitive_markers):
        return fallback
    return text


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _base64_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return base64.b64encode(digest.digest()).decode("ascii")
