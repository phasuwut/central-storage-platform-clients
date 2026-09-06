# Central Storage Platform Compute client

Install locally with `pip install -e clients/compute-python` from the API repository.

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
