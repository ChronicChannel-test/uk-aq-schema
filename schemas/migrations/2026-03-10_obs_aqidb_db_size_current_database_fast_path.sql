-- Apply target: obs_aqidb
-- Purpose: avoid statement timeout in db-size sampling by using current_database()
-- instead of summing pg_database_size across every database in the cluster.

begin;

create or replace function uk_aq_public.uk_aq_rpc_database_size_bytes()
returns table (
  database_name text,
  size_bytes bigint,
  oldest_observed_at timestamptz,
  sampled_at timestamptz
)
language plpgsql
security definer
set search_path = pg_catalog, public
as $$
declare
  v_oldest_observs timestamptz := null;
  v_oldest_aqilevels timestamptz := null;
  v_oldest_observed_at timestamptz := null;
begin
  if auth.role() <> 'service_role' then
    raise exception 'service_role required';
  end if;

  if to_regclass('uk_aq_observs.observations') is not null then
    execute 'select min(o.observed_at) from uk_aq_observs.observations o'
      into v_oldest_observs;
  end if;

  if to_regclass('uk_aq_aqilevels.station_aqi_hourly') is not null then
    execute 'select min(a.timestamp_hour_utc) from uk_aq_aqilevels.station_aqi_hourly a'
      into v_oldest_aqilevels;
  end if;

  select min(v)
    into v_oldest_observed_at
  from (values (v_oldest_observs), (v_oldest_aqilevels)) as oldest(v);

  return query
  select
    current_database()::text as database_name,
    pg_database_size(current_database())::bigint as size_bytes,
    v_oldest_observed_at as oldest_observed_at,
    now() as sampled_at;
end;
$$;

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
set search_path = uk_aq_ops, public, pg_catalog
as $$
declare
  v_days integer;
  v_bucket_hour timestamptz;
  v_rows_upserted int := 0;
  v_rows_deleted bigint := 0;
  v_source text;
  v_oldest_observs timestamptz := null;
  v_oldest_aqilevels timestamptz := null;
  v_oldest_observed_at timestamptz := null;
begin
  v_days := greatest(1, least(coalesce(p_retention_days, 120), 3650));
  v_bucket_hour := date_trunc('hour', coalesce(p_recorded_at, now()));
  v_source := coalesce(nullif(btrim(p_source), ''), 'uk_aq_db_size_logger_pg_cron');

  if to_regclass('uk_aq_observs.observations') is not null then
    execute 'select min(o.observed_at) from uk_aq_observs.observations o'
      into v_oldest_observs;
  end if;

  if to_regclass('uk_aq_aqilevels.station_aqi_hourly') is not null then
    execute 'select min(a.timestamp_hour_utc) from uk_aq_aqilevels.station_aqi_hourly a'
      into v_oldest_aqilevels;
  end if;

  select min(v)
    into v_oldest_observed_at
  from (values (v_oldest_observs), (v_oldest_aqilevels)) as oldest(v);

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
    'obs_aqidb',
    current_database()::text,
    pg_database_size(current_database())::bigint,
    v_oldest_observed_at,
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

commit;
