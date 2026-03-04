# uk_aq_history.observations

History-only observations table for long-term retention outside the main `uk_aq_core.observations` table.

## Fields
- connector_id: Internal connector integer id (key component).
- timeseries_id: Internal timeseries integer id (key component).
- observed_at: Observation timestamp in UTC (natural key component and partition key).
- value: Observation value (double precision).

## Partitioning and indexes
- Partitioned by UTC day (`PARTITION BY RANGE (observed_at)`).
- Default partition: `uk_aq_history.observations_default` catches out-of-range inserts and is treated as an alert signal.
- Hot partitions (today + previous 2 UTC days) keep a unique btree key on `(connector_id, timeseries_id, observed_at)` plus BRIN on `observed_at`.
- Cold partitions keep BRIN on `observed_at` only (no btree key index).

## Notes
- RLS is service_role only (intended for Edge Functions / server-side use).
- Designed for append-only raw-history retention with no aggregation/downsampling.
- `uk_aq_public.uk_aq_rpc_history_observations_upsert` is partition-aware:
  - Hot partitions with `<partition>_hot_key_uidx` use direct partition `INSERT ... ON CONFLICT ... DO UPDATE`.
  - Non-hot or missing partitions fall back to update-then-insert against `uk_aq_history.observations`.
