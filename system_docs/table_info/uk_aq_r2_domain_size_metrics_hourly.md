# uk_aq_ops.r2_domain_size_metrics_hourly

Hourly R2 History domain-size telemetry for dashboard stacked area charting.

## Purpose
- Store hourly size snapshots (bytes) for R2 History domains:
  - `observations`
  - `aqilevels`

## Key columns
- `bucket_hour` (timestamptz): hour bucket (`date_trunc('hour', recorded_at)`).
- `domain_name` (text): `observations` or `aqilevels`.
- `size_bytes` (bigint): domain size in bytes.
- `source` (text): writer identifier.
- `recorded_at` (timestamptz): source sample timestamp.

## Primary key
- `(bucket_hour, domain_name)`

## Reader view
- `uk_aq_public.uk_aq_r2_domain_size_metrics_hourly`

## Writer RPCs
- `uk_aq_public.uk_aq_rpc_r2_domain_size_metric_upsert`
- `uk_aq_public.uk_aq_rpc_r2_domain_size_metric_cleanup`
