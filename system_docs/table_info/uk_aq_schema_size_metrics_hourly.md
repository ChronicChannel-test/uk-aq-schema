# uk_aq_ops.schema_size_metrics_hourly

Hourly schema-size telemetry for `obs_aqidb` dashboard views.

## Purpose
- Store hourly size snapshots (bytes) for:
  - `uk_aq_observs`
  - `uk_aq_aqilevels`
- Provide oldest observed timestamp per schema for day-only legend rendering.

## Key columns
- `bucket_hour` (timestamptz): hour bucket (`date_trunc('hour', recorded_at)`).
- `database_label` (text): currently `obs_aqidb`.
- `schema_name` (text): `uk_aq_observs` or `uk_aq_aqilevels`.
- `size_bytes` (bigint): schema size in bytes.
- `oldest_observed_at` (timestamptz): oldest timestamp used for legend day formatting.
- `source` (text): writer identifier. Expected values are `uk_aq_schema_size_metrics_pg_cron` for the primary local sampler and `uk_aq_db_size_logger_cloud_run` for the optional Cloud Run fallback.
- `recorded_at` (timestamptz): source sample timestamp.

## Primary key
- `(bucket_hour, database_label, schema_name)`

## Reader view
- `uk_aq_public.uk_aq_schema_size_metrics_hourly`

## Writers
- Primary local sampler: `uk_aq_ops.uk_aq_schema_size_metric_sample_local`
- Primary schedule: `pg_cron` job `uk_aq_obs_aqidb_schema_size_metrics_hourly` at `2 * * * *` after DB size at `1 * * * *`
- Timeout guard: the local sampler sets `statement_timeout = '15min'` to avoid short inherited session limits during the relation-size scan.

## Fallback writer RPCs
- `uk_aq_public.uk_aq_rpc_schema_size_metric_upsert`
- `uk_aq_public.uk_aq_rpc_schema_size_metric_cleanup`
