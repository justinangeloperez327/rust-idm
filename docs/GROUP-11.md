# Group 11 — Download management UI

- Fixes Group 10's conflicting root Window dimensions.
- Enables required Slint `compat-1-18` feature.
- Binds UI download rows to a Rust-owned VecModel.
- Adds URL input, status filters, selectable records and row details.
- Includes start/pause/cancel buttons that update **only in-memory UI state**.

**Not yet functional:** The interface does not launch HTTP download workers, persist downloads to SQLite, or implement real transfer progress. These controls are demonstrative UI state, not real download operations. Next priority is an integration coordinator wiring HTTP transfers, scheduler and SQLite.
