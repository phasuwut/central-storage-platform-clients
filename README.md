# central-storage-platform-clients

Public client libraries for [Central Storage Platform](https://central-storage-platform-api.phasuwut.com). The platform's API and web repositories stay private; this repository holds only the code that runs on the caller's side, so a compute node can install a client without any access to them.

| Client | Language | Install |
| --- | --- | --- |
| [`compute-python/`](compute-python/) | Python 3.10+ | `pip install central-storage-platform-compute` |

This repository is consumed by the API repository as a Git submodule at `clients/`, so a change made here reaches the platform by committing the new submodule pointer there.

## Releasing

Each client versions and tags on its own; see the client's own README for the procedure. Tags are prefixed by client (`compute-client-v0.1.4`) so one client's release never triggers another's workflow.

## License

MIT — see [LICENSE](LICENSE).
