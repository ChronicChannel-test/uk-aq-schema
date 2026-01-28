# uk_aq_dispatcher_settings

Controls dispatcher behavior for edge poll dispatching.

## Fields
- id: Singleton row id (always 1).
- dispatcher_parallel_ingest: If true, allow dispatching multiple connectors in a single dispatcher call.
- max_runs_per_dispatch_call: Max number of connectors to dispatch per call (minimum 1).
- updated_at: Last update timestamp.

## Notes
- Readable via `uk_aq_public.dispatcher_settings` for dashboard toggles.
- Writes should be performed by service role only.
