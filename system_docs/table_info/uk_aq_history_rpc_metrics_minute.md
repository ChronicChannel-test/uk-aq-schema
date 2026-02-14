# uk_aq_raw.history_rpc_metrics_minute

Per-minute telemetry for history DB RPC write pressure.

## Fields
- bucket_minute: UTC minute bucket.
- endpoint: RPC endpoint label (currently `rpc/uk_aq_rpc_history_observations_upsert`).
- calls: Number of RPC calls recorded in the minute.
- rows_input: Total input rows submitted across calls in the minute.
- payload_bytes: Total JSON payload bytes received across calls in the minute.
- rows_upserted: Total rows inserted/updated by the RPC in the minute.
- duration_ms_sum: Sum of RPC execution duration in milliseconds.
- duration_ms_max: Max single-call RPC duration in milliseconds.

## Notes
- Primary key is `(bucket_minute, endpoint)`.
- Populated by `uk_aq_public.uk_aq_rpc_history_observations_upsert` in
  `schemas/history_db/history_db_dualwrite_bootstrap.sql`.
- Exposed via `uk_aq_public.uk_aq_history_rpc_metrics_minute` for service-role reads.
