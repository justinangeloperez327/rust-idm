# Transfer reliability milestone

- The latest baseline workflow (commit a60fd07) passed check, Clippy and tests.
- Added an actual loopback HTTP server integration test proving a streamed response is written byte-for-byte and emits completion.
- Added an existing-destination preservation test.
- Replaced rename-based finalization with hard-link publication for single and segmented downloads. This is an atomic no-replace operation on supported local filesystems (including NTFS), with a clear error if linking is unsupported.
- This does not yet implement true crash-resume, stable storage orchestration or transfer retries.

**Risk remaining:** A fixed partial-file name in single-stream transfers and a fixed parts directory in segmented transfers prevent concurrent attempts to the same target; segmented downloads need per-job directories with ownership-safe cleanup before they are production-ready.
