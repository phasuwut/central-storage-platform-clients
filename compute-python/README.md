# Central Storage Platform Compute client

```bash
pip install central-storage-platform-compute
```

The package is published to PyPI, so compute nodes (RunPod, Colab, a bare GPU box) install it without access to the platform's private repositories. It has no third-party dependencies and needs Python 3.10+. To work on the client itself, clone this repository and run `pip install -e compute-python`.

```python
from central_storage_compute import ComputeClient

client = ComputeClient("https://api.example.invalid", token="cpt_<show-once-token>")
result = client.download("<file-id>", "./data.bin")
print(result.bytes_written, result.sha256)

upload = client.upload("./result.bin", destination="results/", mode="auto")
print(upload.file_id, upload.mode, upload.sha256)
```

The client waits up to one hour for each API or storage request by default, and a completion that outlives a single request is replayed under its original `Idempotency-Key` until the API reports a result — a read timeout while the server finalizes a multi-gigabyte object never discards the uploaded bytes. Only an outright rejection from the API aborts a multipart upload. For a slow network or an exceptionally large finalization, set a different per-request limit when creating the client:

```python
client = ComputeClient("https://api.example.invalid", token="cpt_<show-once-token>", timeout=7_200)
```

Pass the API origin only (for example, `https://central-storage-platform-api.phasuwut.com`). Do not include `/api/v1`, `/compute`, a route, or `*`; the client supplies the API path itself. Storage upload failures report only a safe S3 error code and request ID, never a presigned URL.

An interrupted multipart upload is resumed by default: the client records which upload a file belongs to and, on the next `upload()` of that same file, asks the API which parts S3 actually holds and sends only what is missing. A part failure therefore leaves the upload open rather than aborting it. To give up instead and release the staged parts, call `client.abort_upload(upload_id, source)`; resume is bounded by the compute token that created the upload, and a token stays alive while it is being used — signing parts counts — up to the ceiling it was issued with, so only a genuinely abandoned transfer starts over. Pass `resume=False` to always start clean.

Multipart part URLs are signed as the transfer reaches them and re-signed whenever their remaining lifetime no longer covers a part or S3 rejects one, so an upload that outlives a presigned URL continues instead of failing at the tail. Raise `max_connections` for a long-haul link — a single TCP stream to a distant region is limited by round-trip time, not bandwidth, so parallel parts are what make a multi-gigabyte upload finish inside the token's lifetime:

```python
upload = client.upload("./model.zip", destination="models/", mode="multipart", max_connections=32)
```

`download(mode="auto")` follows the transfer mode and concurrency returned by the API. Use `mode="parallel"` to request bounded HTTP Range workers; the client falls back to streaming mode when the storage endpoint does not support ranges. `upload(mode="auto")` tries the single path and switches to the token-scoped multipart path when the API requires it.

The token is supplied at runtime. The client never writes it or a presigned URL to a resume manifest or log. Resume manifests contain only file identity, expected size/checksum and completed byte ranges. All upload completion calls carry a fresh `Idempotency-Key`. Upload checksums are disabled by default for compatibility with S3-compatible presigned PUT endpoints; pass `include_checksum=True` only when the target bucket supports signed `x-amz-checksum-sha256` headers.

## Releasing

`central-storage-platform-compute` is published to PyPI from this repository. Bump `__version__` in `src/central_storage_compute/__init__.py` (`pyproject.toml` reads the version from there), then tag:

```bash
git tag compute-client-v0.2.0 && git push origin compute-client-v0.2.0
```

`.github/workflows/publish-compute-client.yml` checks the tag against `__version__`, runs the tests, builds the sdist and wheel, and uploads them through PyPI Trusted Publishing — no API token is stored in the repository. PyPI never lets a version number be reused, so a bad release needs a new patch version rather than a re-upload.
