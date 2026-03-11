-- Add AQI history day RPCs used by backfill obs_aqi_to_r2 AQI-domain export.

drop function if exists uk_aq_public.uk_aq_rpc_aqilevels_history_day_connector_counts(
  date,
  integer[]
);

create or replace function uk_aq_public.uk_aq_rpc_aqilevels_history_day_connector_counts(
  p_day_utc date,
  p_connector_ids integer[] default null
)
returns table (
  connector_id integer,
  row_count bigint
)
language plpgsql
security definer
set search_path = uk_aq_aqilevels, uk_aq_core, public, pg_catalog
as $$
declare
  v_start timestamptz;
  v_end timestamptz;
begin
  if auth.role() <> 'service_role' then
    raise exception 'service_role required';
  end if;

  if p_day_utc is null then
    raise exception 'p_day_utc is required';
  end if;

  v_start := (p_day_utc::text || ' 00:00:00+00')::timestamptz;
  v_end := ((p_day_utc + 1)::text || ' 00:00:00+00')::timestamptz;

  return query
  select
    s.connector_id::integer,
    count(*)::bigint as row_count
  from uk_aq_aqilevels.station_aqi_hourly h
  join uk_aq_core.stations s
    on s.id = h.station_id
  where h.timestamp_hour_utc >= v_start
    and h.timestamp_hour_utc < v_end
    and s.connector_id is not null
    and s.connector_id > 0
    and (
      p_connector_ids is null
      or s.connector_id = any(p_connector_ids)
    )
  group by s.connector_id
  order by s.connector_id;
end;
$$;

drop function if exists uk_aq_public.uk_aq_rpc_aqilevels_history_day_rows(
  date,
  integer,
  bigint,
  timestamptz,
  integer
);

create or replace function uk_aq_public.uk_aq_rpc_aqilevels_history_day_rows(
  p_day_utc date,
  p_connector_id integer,
  p_after_station_id bigint default null,
  p_after_timestamp_hour_utc timestamptz default null,
  p_limit integer default 20000
)
returns table (
  station_id bigint,
  timestamp_hour_utc timestamptz,
  no2_hourly_mean_ugm3 double precision,
  pm25_hourly_mean_ugm3 double precision,
  pm10_hourly_mean_ugm3 double precision,
  pm25_rolling24h_mean_ugm3 double precision,
  pm10_rolling24h_mean_ugm3 double precision,
  no2_hourly_sample_count smallint,
  pm25_hourly_sample_count smallint,
  pm10_hourly_sample_count smallint,
  daqi_no2_index_level smallint,
  daqi_pm25_rolling24h_index_level smallint,
  daqi_pm10_rolling24h_index_level smallint,
  eaqi_no2_index_level smallint,
  eaqi_pm25_index_level smallint,
  eaqi_pm10_index_level smallint
)
language plpgsql
security definer
set search_path = uk_aq_aqilevels, uk_aq_core, public, pg_catalog
as $$
declare
  v_start timestamptz;
  v_end timestamptz;
  v_limit integer;
begin
  if auth.role() <> 'service_role' then
    raise exception 'service_role required';
  end if;

  if p_day_utc is null then
    raise exception 'p_day_utc is required';
  end if;

  if p_connector_id is null or p_connector_id <= 0 then
    raise exception 'p_connector_id must be > 0';
  end if;

  if (p_after_station_id is null) <> (p_after_timestamp_hour_utc is null) then
    raise exception 'p_after_station_id and p_after_timestamp_hour_utc must both be null or both provided';
  end if;

  v_limit := greatest(1, least(coalesce(p_limit, 20000), 100000));
  v_start := (p_day_utc::text || ' 00:00:00+00')::timestamptz;
  v_end := ((p_day_utc + 1)::text || ' 00:00:00+00')::timestamptz;

  return query
  select
    h.station_id,
    h.timestamp_hour_utc,
    h.no2_hourly_mean_ugm3,
    h.pm25_hourly_mean_ugm3,
    h.pm10_hourly_mean_ugm3,
    h.pm25_rolling24h_mean_ugm3,
    h.pm10_rolling24h_mean_ugm3,
    h.no2_hourly_sample_count,
    h.pm25_hourly_sample_count,
    h.pm10_hourly_sample_count,
    h.daqi_no2_index_level,
    h.daqi_pm25_rolling24h_index_level,
    h.daqi_pm10_rolling24h_index_level,
    h.eaqi_no2_index_level,
    h.eaqi_pm25_index_level,
    h.eaqi_pm10_index_level
  from uk_aq_aqilevels.station_aqi_hourly h
  join uk_aq_core.stations s
    on s.id = h.station_id
  where s.connector_id = p_connector_id
    and h.timestamp_hour_utc >= v_start
    and h.timestamp_hour_utc < v_end
    and (
      p_after_station_id is null
      or h.station_id > p_after_station_id
      or (h.station_id = p_after_station_id and h.timestamp_hour_utc > p_after_timestamp_hour_utc)
    )
  order by h.station_id asc, h.timestamp_hour_utc asc
  limit v_limit;
end;
$$;

revoke all on function uk_aq_public.uk_aq_rpc_aqilevels_history_day_connector_counts(
  date,
  integer[]
) from public;
grant execute on function uk_aq_public.uk_aq_rpc_aqilevels_history_day_connector_counts(
  date,
  integer[]
) to service_role;

revoke all on function uk_aq_public.uk_aq_rpc_aqilevels_history_day_rows(
  date,
  integer,
  bigint,
  timestamptz,
  integer
) from public;
grant execute on function uk_aq_public.uk_aq_rpc_aqilevels_history_day_rows(
  date,
  integer,
  bigint,
  timestamptz,
  integer
) to service_role;
