# Architecture

The application is an independent, clean-room IDM-like download manager.

## Boundaries

- `download-core`: dependency-free domain model and legal lifecycle transitions.
- `download-http`: transport interface and range verification, introduced in Group 3.
- `download-storage`: persistent records and restart recovery, introduced in Group 7.
- `download-scheduler`: worker orchestration, introduced in Group 8.
- `download-platform`: native operating-system services, introduced in Group 12.
- `download-ipc`: authenticated local communication with external integrations.
- `apps/desktop`: Slint UI, introduced in Group 10.
- `apps/cli`: developer and automation entry point.

## Transfer safety principles

- Only trust HTTP ranges after validating response status and Content-Range.
- Validate validators (ETag/Last-Modified) before resuming.
- Write into temporary files and finalize atomically when possible.
- Avoid storing plaintext authentication secrets in SQLite.
- Treat filenames and redirects as untrusted input.
- Never claim a download completed before file integrity and I/O finalization succeed.

## Performance principles

Bound concurrent requests and buffers; separate network and disk backpressure; avoid UI work on worker threads; benchmark rather than promise improvements.
