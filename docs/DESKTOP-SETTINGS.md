# Desktop transfer settings

The Settings view provides a configurable concurrency limit (1–16) and a global aggregate disk-write cap in KiB/s (0 = unlimited). Apply Settings updates the existing scheduler in real time, dispatching any newly eligible jobs, and updates the shared global token bucket used by active managed and resumed transfers.

Settings are session-only and revert to defaults on restart. Input fields need stronger numeric validation and inline error feedback before release. The global cap meters disk writes and permits an initial one-second burst, not exact network ingress.