# Desktop persistence integration

The Slint desktop now opens a local SQLite database under `%LOCALAPPDATA%\\RustIDM\\downloads.sqlite`, loads previously saved downloads on startup, and marks interrupted active records paused.

- New downloads are inserted before they are shown in the UI.
- IDs are allocated from the maximum persisted record ID to avoid reusing IDs after a restart.
- Start, pause, cancel, transfer progress, failure, and successful completion update database status.
- Transfers remain single-stream, and starting a paused transfer begins again at byte zero.
- Filename is still generated from a persisted job ID rather than a server-supplied filename.
- Database operations run synchronously on the UI thread; progress-event batching and dedicated async persistence are needed before high-frequency workloads.

This update does **not** connect the priority scheduler, bandwidth manager, or segmented checkpoint recovery to the desktop.
