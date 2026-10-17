# Group 8 — Queue scheduler

Implements a deterministic in-memory priority queue, configurable worker concurrency (1–64), stable FIFO ordering within each priority, and dispatch/completion accounting.

The scheduler is not yet a background task runner. `dispatch_ready` returns job IDs for an external coordinator to launch. It does not yet persist queued state, invoke HTTP transfers, dispatch timers, or connect automatically to SQLite records. Running workers are not preempted when limits decrease.

Next: integrate the scheduler with Tokio workers and database state transitions and add bandwidth throttling in Group 9.
