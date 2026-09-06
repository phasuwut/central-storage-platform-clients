# Central Storage Platform Compute client

Install locally with `pip install -e clients/compute-python` from the API repository.

```python
from central_storage_compute import ComputeClient

client = ComputeClient("https://api.example.invalid", token="cpt_<show-once-token>")
result = client.download("<file-id>", "./data.bin")
print(result.bytes_written, result.sha256)
```

The token is supplied at runtime. The client never writes it or a presigned URL to a resume manifest or log. Upload and parallel range transfer are enabled after the corresponding compute endpoints are available.
