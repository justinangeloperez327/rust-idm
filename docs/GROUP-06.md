# Group 6 — Checkpoint persistence foundation

This group introduces a durable, versioned JSON checkpoint schema in `download-storage`:

- Range layout, completion flags, final URL, total size, ETag and Last-Modified.
- Validation for contiguous, non-overlapping, bounded segments.
- Strict strong-ETag or Last-Modified validation against a fresh remote probe.
- Reconciliation against on-disk segment sizes after a crash.
- Serialized checkpoint writes with flush, file sync and rename.
- Unit tests for validator mismatch, range corruption, and schema serialization.

**Integration status:** checkpoint persistence is a standalone foundation. The parallel HTTP engine does not yet load/save checkpoints or reuse saved segments automatically. Group 7 must wire storage and download orchestration together. Merely matching file sizes is insufficient to prove an untrusted segment's content integrity: future integration must use verified hashes or another trustworthy mechanism before reuse.

**Known limitations:** the same-process temporary checkpoint name can collide with concurrent writes; production must adopt unique temporary names and serialized writers. Directory-sync guarantees vary by operating system. A dedicated no-replace file publishing primitive is still needed for the download output.
