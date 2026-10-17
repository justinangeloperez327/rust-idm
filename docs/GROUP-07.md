# Group 7 — SQLite persistence

This group provides `DownloadDatabase` backed by bundled SQLite via rusqlite.

- Schema version 1 and transactional first-run migration.
- Download metadata, status, byte progress, HTTP validators, checkpoint JSON, and timestamps.
- Insert, update, lookup, listing, deletion and interrupted-state recovery.
- WAL mode for on-disk databases, busy timeout and database constraints.
- Isolated in-memory tests for CRUD, invalid states and restart recovery.

**Integration:** callers can now save the versioned checkpoint JSON alongside download records. The SQL storage API does not yet automatically subscribe to `DownloadService` events, persist each completed segment, or instruct the HTTP engine to skip validated completed ranges. Full end-to-end resume requires a coordinator and dedicated integration tests; the Group 8 scheduler will begin wiring these pieces.

**Safety:** never automatically trust stored progress. On startup, active records become paused. Validate remote validators and segment contents before resuming; timestamps are informational, not integrity guarantees. The synchronous DB layer must be called from a blocking thread outside async network workers.
