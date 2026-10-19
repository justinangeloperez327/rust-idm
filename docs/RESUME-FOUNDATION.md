# Verified range-resume foundation

Adds an opt-in single-stream resume API for a previously validated strong-ETag identity. It checks exact Content-Range, ETag, response status, content encoding, current partial-file length and final byte count, and preserves partial data on cancellation. Redirects are disabled to prevent accidentally resuming against a different resource.

**Not yet wired to the desktop:** this API needs a coordinator that captures the original strong ETag, serializes checkpoint writes, owns per-job temporary paths, verifies the identity on recovery, and provides progress events. It cannot yet resume downloads from the UI, and a fully downloaded partial file requires a separately verified hash before publication. The API does not guarantee crash-safe durability for all filesystems or concurrent writers.
