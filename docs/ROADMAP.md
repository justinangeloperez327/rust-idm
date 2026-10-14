# Implementation roadmap

1. Repository foundation — workspace, CI, domain lifecycle.
2. Download domain — commands, events, errors.
3. HTTP client — metadata and server capability probing.
4. Streaming download engine — safe file writing and cancellation.
5. Range downloads — concurrent segments and response validation.
6. Resume and recovery — checkpoints, ETags, crash recovery.
7. Persistence — SQLite and migrations.
8. Scheduler — priorities, queues and concurrency.
9. Bandwidth manager — token bucket and fairness.
10. Desktop foundation — Slint windows and application state.
11. Download management interface — listing, details and progress.
12. Windows integration — system tray, notifications and clipboard.
13. Browser extension — Chromium, Firefox and native messaging.
14. Advanced networking — proxies, credentials and mirrors.
15. Automation — CLI, batch jobs and local API.
16. Security and reliability — integrity tests and fuzzing.
17. Profiling — throughput, allocations and resource benchmarks.
18. Release engineering — signed installers and documentation.

## Non-goals for Group 1
No simulated or pretend downloads. A functioning HTTP transfer path is introduced in Groups 3–4. No automatic browser interception or executable injection.
