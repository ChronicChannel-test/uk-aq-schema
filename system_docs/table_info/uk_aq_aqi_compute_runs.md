# aqi_compute_runs

AQI compute run telemetry table in `uk_aq_ops`.

## Fields
- id: Run identifier (UUID).
- started_at: Run timestamp.
- run_mode: `sync_hourly` or `backfill` for current workers (legacy values `fast`, `reconcile_short`, `reconcile_deep` may exist historically).
- trigger_mode: Trigger source label (`scheduler`, `manual`, etc.).
- window_start_utc, window_end_utc: Hour window processed.
- source_rows: Source rows returned from ingest RPC.
- candidate_station_hours: Candidate station-hour rows evaluated.
- rows_upserted: Rows written to hourly fact table.
- rows_changed: Rows detected as changed.
- station_hours_changed: Changed station-hour keys.
- station_hours_changed_gt_36h: Changed keys older than 36 hours before the reference hour.
- max_changed_lag_hours: Maximum lag (hours) for changed rows.
- deep_reconcile_effective: Legacy reconcile signal (null for current sync-only runs).
- daily_rows_upserted, monthly_rows_upserted: Rollup rows refreshed.
- run_status: `ok` or `error`.
- error_message: Optional failure message.
- duration_ms: End-to-end run duration.
- created_at: Row creation timestamp.

## Notes
- Written by AQI worker via RPC.
- Cleanup RPC enforces configurable retention (default 7 days).
