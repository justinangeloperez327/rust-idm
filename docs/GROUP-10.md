# Group 10 — Native desktop foundation

- Slint window using the software renderer and Winit backend.
- Compact navigation: All Downloads, Active, Completed, Paused, Failed, Settings.
- Dark mode by default with interactive light-mode toggle.
- URL input and Add URL button (nonfunctional placeholder, clearly labeled).
- Empty-state transfer table, compact sidebar and window sizing constraints.

This version is only a desktop shell. It **does not** enqueue or download files, and it does not load data from SQLite. These integrations remain to be implemented. Group 9's crate documentation ordering is also corrected.

Build and run on Windows with `cargo run -p rust-idm-desktop`.
