-- Apply target: obs_aqidb
-- Purpose: align Cloud Run DB-size sampling with the local pg_cron path by
-- returning cluster-wide size_bytes from uk_aq_rpc_database_size_bytes().

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
    (
      select coalesce(sum(pg_database_size(pg_database.datname)), 0)::bigint
      from pg_database
    ) as size_bytes,
    v_oldest_observed_at as oldest_observed_at,
    now() as sampled_at;
end;
$$;

commit;
