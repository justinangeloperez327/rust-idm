# Group 9 — Bandwidth admission and fair allocation

Adds deterministic token buckets with configurable global and per-job byte-per-second limits, bounded burst capacity, elapsed-time replenishment, and round-robin admission grants. Unit tests cover fairness, throttling, invalid rates, dynamic membership and combined limits.

This component is **not yet wired into HTTP transport**, so it does not currently throttle real downloads. Integration must reserve allowances before reads/writes and refund unused reservations, or otherwise ensure byte accounting; it must also wake asynchronously rather than busy-poll. Rust `Instant` is used for monotonic timing.

The existing download scheduler still selects jobs; this separate bandwidth manager selects the next job for a transfer budget. Future work should unify scheduling and network I/O.
