# Resume hardening update

- Continuous progress events from resumed range-transfer chunks now reach the existing desktop progress observer.
- A paused/cancelled worker's cancellation token remains registered until the worker actually completes, preventing the desktop from launching a second worker for the same job while the first is still shutting down.
- A Slint UI-thread callback clears registered worker tokens on completion.
- Existing localhost resume tests remain in CI.

**Known limitation:** abruptly failing runtime initialization or worker database setup can leave a registered token until application restart; future work should provide a unified guaranteed worker-completion signal and end-to-end restart test. Resumed partial files must remain trusted only after strict remote validation.
