# Group 2 — Download domain

The application service is a deterministic synchronous command handler.

- `DownloadRequest` validates HTTP(S) URL schemes and nonempty destination paths.
- `DownloadCommand` captures enqueue, start, pause, retry, cancel, progress, complete and failure intents.
- `DownloadEvent` exposes queue, transition and progress changes to future consumers.
- `DownloadService` stores in-memory entries and allocates nonzero IDs.
- Invalid commands return errors without modifying download state.

## Integration notes

This is **not** the actual network transfer engine. Group 3 adds HTTP capabilities, and Group 4 connects workers to lifecycle commands. Requests are not yet persisted. URL validation here checks the scheme only; the transport layer must parse/validate URLs strictly, enforce acceptable schemes across redirects, and apply destination sanitization. IDs are process-local and will require durable allocation once SQLite is introduced.

Completion events are currently domain signals, not proof that an actual file has been verified and atomically finalized. The future download engine must issue them only after successful finalization.
