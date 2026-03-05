-- Schedule local hourly ingest DB-size sampling and retention via pg_cron.

create extension if not exists pg_cron with schema extensions;

create or replace function uk_aq_ops.uk_aq_db_size_metric_sample_local(
  p_retention_days integer default 120,
  p_recorded_at timestamptz default now(),
  p_source text default 'uk_aq_db_size_logger_pg_cron'
)
returns table (
  rows_upserted int,
  rows_deleted bigint
)
language plpgsql
security definer
set search_path = uk_aq_ops, uk_aq_core, public, pg_catalog
as $$
declare
  v_days integer;
  v_bucket_hour timestamptz;
  v_rows_upserted int := 0;
  v_rows_deleted bigint := 0;
  v_source text;
begin
  v_days := greatest(1, least(coalesce(p_retention_days, 120), 3650));
  v_bucket_hour := date_trunc('hour', coalesce(p_recorded_at, now()));
  v_source := coalesce(nullif(btrim(p_source), ''), 'uk_aq_db_size_logger_pg_cron');

  insert into uk_aq_ops.db_size_metrics_hourly (
    bucket_hour,
    database_label,
    database_name,
    size_bytes,
    oldest_observed_at,
    source,
    recorded_at,
    updated_at
  )
  values (
    v_bucket_hour,
    'ingestdb',
    current_database()::text,
    pg_database_size(current_database())::bigint,
    (select min(o.observed_at) from uk_aq_core.observations o),
    v_source,
    coalesce(p_recorded_at, now()),
    now()
  )
  on conflict (bucket_hour, database_label) do update set
    database_name = excluded.database_name,
    size_bytes = excluded.size_bytes,
    oldest_observed_at = excluded.oldest_observed_at,
    source = excluded.source,
    recorded_at = excluded.recorded_at,
    updated_at = now();

  get diagnostics v_rows_upserted = row_count;

  delete from uk_aq_ops.db_size_metrics_hourly
  where bucket_hour < now() - make_interval(days => v_days);

  get diagnostics v_rows_deleted = row_count;

  return query select v_rows_upserted, v_rows_deleted;
end;
$$;

revoke all on function uk_aq_ops.uk_aq_db_size_metric_sample_local(integer, timestamptz, text) from public;
grant execute on function uk_aq_ops.uk_aq_db_size_metric_sample_local(integer, timestamptz, text) to service_role;

select cron.unschedule(jobid)
from cron.job
where jobname = 'uk_aq_ingest_db_size_metrics_hourly';

select cron.schedule(
  'uk_aq_ingest_db_size_metrics_hourly',
  '2 * * * *',
  $$select * from uk_aq_ops.uk_aq_db_size_metric_sample_local();$$
);
