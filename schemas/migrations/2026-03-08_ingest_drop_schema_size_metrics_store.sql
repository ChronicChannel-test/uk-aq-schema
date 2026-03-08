-- Phase 3 hard-cut cleanup: remove schema-size metric storage from ingestdb.
-- Apply target: ingestdb

-- Schema-size metrics now live only in obs_aqidb.

drop view if exists uk_aq_public.uk_aq_schema_size_metrics_hourly;

drop function if exists uk_aq_public.uk_aq_rpc_schema_size_metric_upsert(
  text,
  text,
  bigint,
  timestamptz,
  timestamptz,
  text
);

drop function if exists uk_aq_public.uk_aq_rpc_schema_size_metric_cleanup(integer);

drop table if exists uk_aq_ops.schema_size_metrics_hourly;
