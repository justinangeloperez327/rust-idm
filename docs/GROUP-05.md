# Group 5 — Concurrent byte-range transfers

- Adds `SegmentedDownloader` for 1–16 concurrent HTTP byte ranges.
- Splits a known object length into gap-free, nonoverlapping segments.
- Validates exact `206 Partial Content`, `Content-Range`, and received byte count for every segment.
- Requests identity encoding and rejects unexpected content encoding.
- Supports strong ETag pinning with `If-Range` and rejects mismatching response ETags.
- Writes segment files independently, then joins them in order into an assembled temporary file.
- Supports cancellation, aborts remaining workers after failure, and cleans temporary parts.

The caller must first verify actual range support and obtain the full object length (Group 3). This API does not silently fall back to the Group 4 single-stream engine. Concurrent progress reporting, checkpoint persistence, robust unique working directories, stronger cross-segment validator consistency, and race-free finalization remain future work. It does not claim production readiness.
