# Desktop queue dispatch integration

Desktop Start enqueues downloads in `download-scheduler` and dispatches at most three concurrent workers. Queued downloads remain visibly queued and may be cancelled. Worker completion releases its scheduler slot and launches the next pending job. The desktop keeps cancellation tokens until a running worker actually stops, so a paused transfer continues occupying a slot during shutdown. All existing jobs default to normal priority; prioritization controls and runtime concurrency settings are not yet exposed in the UI.

The scheduler is in-memory and rebuilt after application restart; SQLite download records persist, but queued jobs are not automatically scheduled after restart. There is no bandwidth integration yet. Worker startup failures release slots, but robust error display and full desktop E2E testing are still required.
