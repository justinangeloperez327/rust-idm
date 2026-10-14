# Rust IDM

A lightweight, native download manager for Windows, developed in Rust.

> Status: Group 1 — foundational Cargo workspace. Download transfers and the desktop UI are scheduled for later groups.

## Getting started

Install a stable Rust toolchain with `rustup`, then run:

```powershell
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo run -p rust-idm-cli -- --help
```

## Architecture

- `apps/cli`: command-line entry point and early integration harness.
- `apps/desktop`: native desktop entry point (Slint UI follows in Group 10).
- `crates/download-core`: download lifecycle and validated domain types.
- `crates/download-http`: HTTP transfer adapters (Group 3).
- `crates/download-storage`: durable download metadata (Group 7).
- `crates/download-scheduler`: queue orchestration (Group 8).
- `crates/download-platform`: Windows integrations (Group 12).
- `crates/download-ipc`: integration messaging (Group 13+).

See [the roadmap](docs/ROADMAP.md) and [architecture](docs/ARCHITECTURE.md).

## License

MIT OR Apache-2.0. This is an independent project; it is not affiliated with Internet Download Manager.
