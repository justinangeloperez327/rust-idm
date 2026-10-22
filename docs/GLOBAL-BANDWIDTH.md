# Shared bandwidth control

Managed and resumed HTTP downloads now share a process-wide token bucket when the environment variable `RUST_IDM_GLOBAL_BYTES_PER_SECOND` is set to a positive integer. All worker threads share the same bucket; the existing `RUST_IDM_BYTES_PER_SECOND` setting continues to apply separately per transfer. Both limits can be combined.

Example (PowerShell):
```powershell
$env:RUST_IDM_GLOBAL_BYTES_PER_SECOND = "2097152"
$env:RUST_IDM_BYTES_PER_SECOND = "1048576"
cargo run -p rust-idm-desktop
```

The shared bucket has an initial burst allowance equal to one second of configured bandwidth. Limits are sampled at first access, so restarting the application is required to change them. The limiter currently meters bytes written to disk, not actual network ingress, and does not provide strict FIFO fairness or refund unused reservations on I/O error. Interactive Settings controls are not yet implemented.
