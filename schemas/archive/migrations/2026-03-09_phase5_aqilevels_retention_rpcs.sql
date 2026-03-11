-- Phase 5: AQI levels retention RPCs (manifest-gated day cleanup support)

create schema if not exists uk_aq_public;

-- Return day-level cleanup candidates older than cutoff day.
drop function if exists uk_aq_public.uk_aq_rpc_aqilevels_drop_candidates(date);
create or replace function uk_aq_public.uk_aq_rpc_aqilevels_drop_candidates(
  p_cutoff_day_utc date
)
returns table (
  day_utc date,
  hourly_rows bigint,
  daily_rows bigint
)
language plpgsql
security definer
set search_path = uk_aq_aqilevels, public, pg_catalog
as $$
begin
  if auth.role() <> 'service_role' then
    raise exception 'service_role required';
  end if;

  if p_cutoff_day_utc is null then
    raise exception 'p_cutoff_day_utc is required';
  end if;

  return query
  with hourly as (
    select
      (h.timestamp_hour_utc at time zone 'UTC')::date as day_utc,
      count(*)::bigint as hourly_rows
    from uk_aq_aqilevels.station_aqi_hourly h
    where h.timestamp_hour_utc < ((p_cutoff_day_utc::text || ' 00:00:00+00')::timestamptz)
    group by 1
  ),
  daily as (
    select
      d.observed_day as day_utc,
      count(*)::bigint as daily_rows
    from uk_aq_aqilevels.station_aqi_daily d
    where d.observed_day < p_cutoff_day_utc
    group by 1
  ),
  merged as (
    select h.day_utc, h.hourly_rows, 0::bigint as daily_rows
    from hourly h
    union all
    select d.day_utc, 0::bigint as hourly_rows, d.daily_rows
    from daily d
  )
  select
    m.day_utc,
    sum(m.hourly_rows)::bigint as hourly_rows,
    sum(m.daily_rows)::bigint as daily_rows
  from merged m
  group by m.day_utc
  order by m.day_utc;
end;
$$;

-- Delete one AQI UTC day from hourly and daily tables.
drop function if exists uk_aq_public.uk_aq_rpc_aqilevels_drop_day(date);
create or replace function uk_aq_public.uk_aq_rpc_aqilevels_drop_day(
  p_day_utc date
)
returns table (
  hourly_rows_deleted bigint,
  daily_rows_deleted bigint
)
language plpgsql
security definer
set search_path = uk_aq_aqilevels, public, pg_catalog
as $$
declare
  v_day_start_utc timestamptz;
  v_day_end_utc timestamptz;
  v_hourly_deleted bigint := 0;
  v_daily_deleted bigint := 0;
begin
  set local timezone = 'UTC';

  if auth.role() <> 'service_role' then
    raise exception 'service_role required';
  end if;

  if p_day_utc is null then
    raise exception 'p_day_utc is required';
  end if;

  v_day_start_utc := (p_day_utc::text || ' 00:00:00+00')::timestamptz;
  v_day_end_utc := ((p_day_utc + 1)::text || ' 00:00:00+00')::timestamptz;

  delete from uk_aq_aqilevels.station_aqi_hourly h
  where h.timestamp_hour_utc >= v_day_start_utc
    and h.timestamp_hour_utc < v_day_end_utc;

  get diagnostics v_hourly_deleted = row_count;

  delete from uk_aq_aqilevels.station_aqi_daily d
  where d.observed_day = p_day_utc;

  get diagnostics v_daily_deleted = row_count;

  return query
  select
    coalesce(v_hourly_deleted, 0),
    coalesce(v_daily_deleted, 0);
end;
$$;

revoke all on function uk_aq_public.uk_aq_rpc_aqilevels_drop_candidates(date) from public;
grant execute on function uk_aq_public.uk_aq_rpc_aqilevels_drop_candidates(date) to service_role;

revoke all on function uk_aq_public.uk_aq_rpc_aqilevels_drop_day(date) from public;
grant execute on function uk_aq_public.uk_aq_rpc_aqilevels_drop_day(date) to service_role;
