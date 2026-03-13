# uk_aq_ops.obs_aqidb_day_counts_current

Exact current per-day row counts for the live `obs_aqidb` retention window.

## Purpose
- Store the latest exact UTC-day row counts for:
  - `observs` from `uk_aq_observs.observations`
  - `aqilevels` from `uk_aq_aqilevels.station_aqi_hourly`
- Give the dashboard a small source of truth for:
  - ObsAQIDB calendar presence
  - exact day counts
- Avoid per-day RPC loops over the live tables during dashboard refresh.

## Key columns
- `dataset` (text): `observs` or `aqilevels`.
- `day_utc` (date): UTC day bucket.
- `row_count` (bigint): exact row count for that UTC day.
- `bucket_hour` (timestamptz): hour bucket of the latest refresh.
- `source` (text): writer identifier.
- `recorded_at` (timestamptz): refresh timestamp used for the current value.

## Primary key
- `(dataset, day_utc)`

## Reader view
- `uk_aq_public.uk_aq_obs_aqidb_day_counts_current`

## Writers
- Primary hourly sampler:
  - `uk_aq_ops.uk_aq_obs_aqidb_day_counts_refresh_current`
  - `pg_cron` job `uk_aq_obs_aqidb_day_counts_current_hourly` at `55 * * * *`
- Daily reconcile:
  - same function
  - `pg_cron` job `uk_aq_obs_aqidb_day_counts_current_reconcile_daily` at `10 6 * * *`

## Immediate cleanup hook
- `uk_aq_public.uk_aq_rpc_obs_aqidb_day_count_delete(text, date)`
- Called by the live ObsAQIDB pruning services after successful deletes so pruned days disappear from the current table immediately.

## Notes
- The refresh function stores exact counts for the full live table window, not a historical hourly series.
- Gaps between the earliest and latest live day are stored explicitly with `row_count = 0` so the dashboard can distinguish zero-row days from fetch failures.
