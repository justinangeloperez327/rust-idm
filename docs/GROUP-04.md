# Group 4 — Streaming download engine

The download-http crate now contains `TransferEngine::download`, which streams an HTTP(S) response directly to a temporary file without loading the entire download into memory.

Implemented: HTTP error checking, cancellation during requests and writes, streamed progress events, body length validation, sync of file contents, and final rename on successful transfer. Incomplete .rust-idm-part files are deleted after errors and cancellations.

Constraints: This is a *single stream* transfer. There is no pause/resume checkpoint, persistent queue, segmentation, integrity hash validation, or UI yet. The application-layer DownloadService is not yet wired to the HTTP engine.

Known limitation: checking for an existing destination before rename does not fully prevent an overwrite race on some platforms. A platform-appropriate no-replace finalize operation and crash durability tests must be implemented before production release. The caller should only download into explicitly authorized, non-existing output paths.

## Next
Group 5 introduces range-partitioned parallel transfers and strict content-range validation.
