# Transfer integration milestone

The desktop Start command now launches a real single-stream HTTP transfer using `TransferEngine` on a background thread, with a Tokio runtime and Slint event-loop progress updates. Pause and Cancel signal the cancellation token. Downloads are written to the user's Downloads directory using a generated file name. A cancelled transfer cannot be resumed from its last byte yet; clicking Start again restarts from zero.

This milestone also relaxes workspace unsafe-code linting from `forbid` to `warn` to support Slint's macro-generated code. The app still lacks database integration, global queue scheduling, safe unique destination reservation, consistent error display, structured cancellation teardown, and end-to-end integration tests. Do not treat it as production ready.

To run on Windows: `cargo run -p rust-idm-desktop`.
