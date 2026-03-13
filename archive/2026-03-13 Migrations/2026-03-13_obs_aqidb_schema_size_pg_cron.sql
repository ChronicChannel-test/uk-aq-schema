-- Apply target: obs_aqidb
-- Purpose: move ObsAQIDB schema-size telemetry to a separate local pg_cron
-- schedule instead of relying on Cloud Run as the primary writer.

begin;

select cron.unschedule(jobid)
from cron.job
where jobname in (
  'uk_aq_obs_aqidb_db_size_metrics_hourly'
);

select cron.schedule(
  'uk_aq_obs_aqidb_db_size_metrics_hourly',
  '1 * * * *',
  $$select * from uk_aq_ops.uk_aq_db_size_metric_sample_local();$$
);

create or replace function uk_aq_ops.uk_aq_schema_size_metric_sample_local(
  p_retention_days integer default 120,
  p_recorded_at timestamptz default now(),
  p_source text default 'uk_aq_schema_size_metrics_pg_cron'
)
returns table (
  rows_upserted int,
  rows_deleted bigint
)
language plpgsql
security definer
set search_path = uk_aq_ops, public, pg_catalog
as $$
declare
  v_days integer;
  v_bucket_hour timestamptz;
  v_rows_upserted int := 0;
  v_rows_deleted bigint := 0;
  v_rows int := 0;
  v_source text;
  v_schema text;
  v_size_bytes bigint := 0;
  v_oldest_observed_at timestamptz := null;
begin
  set local timezone = 'UTC';
  -- Guard against short inherited role/session statement timeouts during
  -- relation-size scans across the obs_aqidb schemas.
  set local statement_timeout = '15min';

  v_days := greatest(1, least(coalesce(p_retention_days, 120), 3650));
  v_bucket_hour := date_trunc('hour', coalesce(p_recorded_at, now()));
  v_source := coalesce(nullif(btrim(p_source), ''), 'uk_aq_schema_size_metrics_pg_cron');

  foreach v_schema in array array['uk_aq_observs', 'uk_aq_aqilevels'] loop
    v_size_bytes := 0;
    v_oldest_observed_at := null;

    if to_regnamespace(v_schema) is not null then
      execute format(
        $sql$
          select coalesce(sum(pg_total_relation_size(c.oid)), 0)::bigint
          from pg_class c
          join pg_namespace n on n.oid = c.relnamespace
          where n.nspname = %L
            and c.relkind in ('r', 'p', 'm', 't')
        $sql$,
        v_schema
      ) into v_size_bytes;

      if v_schema = 'uk_aq_observs'
         and to_regclass('uk_aq_observs.observations') is not null then
        execute 'select min(o.observed_at) from uk_aq_observs.observations o'
          into v_oldest_observed_at;
      elsif v_schema = 'uk_aq_aqilevels'
            and to_regclass('uk_aq_aqilevels.station_aqi_hourly') is not null then
        execute 'select min(a.timestamp_hour_utc) from uk_aq_aqilevels.station_aqi_hourly a'
          into v_oldest_observed_at;
      end if;
    end if;

    insert into uk_aq_ops.schema_size_metrics_hourly (
      bucket_hour,
      database_label,
      schema_name,
      size_bytes,
      oldest_observed_at,
      source,
      recorded_at,
      updated_at
    )
    values (
      v_bucket_hour,
      'obs_aqidb',
      v_schema,
      coalesce(v_size_bytes, 0),
      v_oldest_observed_at,
      v_source,
      coalesce(p_recorded_at, now()),
      now()
    )
    on conflict (bucket_hour, database_label, schema_name) do update set
      size_bytes = excluded.size_bytes,
      oldest_observed_at = excluded.oldest_observed_at,
      source = excluded.source,
      recorded_at = excluded.recorded_at,
      updated_at = now();

    get diagnostics v_rows = row_count;
    v_rows_upserted := v_rows_upserted + coalesce(v_rows, 0);
  end loop;

  delete from uk_aq_ops.schema_size_metrics_hourly
  where bucket_hour < now() - make_interval(days => v_days);

  get diagnostics v_rows_deleted = row_count;

  return query select v_rows_upserted, v_rows_deleted;
end;
$$;

revoke all on function uk_aq_ops.uk_aq_schema_size_metric_sample_local(integer, timestamptz, text) from public;
grant execute on function uk_aq_ops.uk_aq_schema_size_metric_sample_local(integer, timestamptz, text) to service_role;

select cron.unschedule(jobid)
from cron.job
where jobname in (
  'uk_aq_obs_aqidb_schema_size_metrics_hourly'
);

select cron.schedule(
  'uk_aq_obs_aqidb_schema_size_metrics_hourly',
  '2 * * * *',
  $$select * from uk_aq_ops.uk_aq_schema_size_metric_sample_local();$$
);

commit;
