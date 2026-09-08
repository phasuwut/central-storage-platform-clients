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

`download(mode="auto")` follows the transfer mode and concurrency returned by the API. Use `mode="parallel"` to request bounded HTTP Range workers; the client falls back to streaming mode when the storage endpoint does not support ranges. `upload(mode="auto")` tries the single path and switches to the token-scoped multipart path when the API requires it.

The token is supplied at runtime. The client never writes it or a presigned URL to a resume manifest or log. Resume manifests contain only file identity, expected size/checksum and completed byte ranges. All upload completion calls carry a fresh `Idempotency-Key`.

## Releasing

`central-storage-platform-compute` is published to PyPI from this repository. Bump `__version__` in `src/central_storage_compute/__init__.py` (`pyproject.toml` reads the version from there), then tag:

```bash
git tag compute-client-v0.2.0 && git push origin compute-client-v0.2.0
```

`.github/workflows/publish-compute-client.yml` checks the tag against `__version__`, runs the tests, builds the sdist and wheel, and uploads them through PyPI Trusted Publishing — no API token is stored in the repository. PyPI never lets a version number be reused, so a bad release needs a new patch version rather than a re-upload.
