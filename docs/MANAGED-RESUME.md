# Integrated single-stream resume

The desktop now uses `ManagedDownloader` instead of `TransferEngine`. On a first request with a known nonzero Content-Length and strong ETag it records a durable JSON sidecar before writing the partial file. On a subsequent Start (including after app restart), it checks the requested URL and delegates to the strict ranged `ResumeEngine`. The sidecar is removed after successful no-replace publication. A missing sidecar or partial file is an error, not permission to reuse untrusted bytes.

Limitations: servers without strong ETags or known lengths are rejected by this path; resumed progress updates are emitted initially and at completion, not continuously. A fully downloaded partial cannot yet be published after a crash. The desktop still lacks robust lifecycle interlocks and an automated full pause/restart integration test. The sidecar is not a cryptographic integrity guarantee and the DB is not the sidecar authority.
