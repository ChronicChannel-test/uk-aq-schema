-- Station AQI source RPC (ingest DB).
-- v1 simplified source output: hourly means + sample_count only.

drop function if exists uk_aq_public.uk_aq_rpc_station_aqi_hourly_source(
  timestamptz,
  timestamptz,
  bigint[]
);

create or replace function uk_aq_public.uk_aq_rpc_station_aqi_hourly_source(
  p_window_start timestamptz,
  p_window_end timestamptz,
  p_station_ids bigint[] default null
)
returns table (
  station_id bigint,
  timestamp_hour_utc timestamptz,
  pollutant_code text,
  hourly_mean_ugm3 double precision,
  sample_count integer
)
language plpgsql
security definer
set search_path = uk_aq_core, public, pg_catalog
as $$
declare
  v_window_start timestamptz;
  v_window_end timestamptz;
begin
  if auth.role() <> 'service_role' then
    raise exception 'service_role required';
  end if;

  if p_window_start is null or p_window_end is null then
    raise exception 'p_window_start and p_window_end are required';
  end if;

  v_window_start := date_trunc('hour', p_window_start);
  v_window_end := date_trunc('hour', p_window_end);

  if v_window_end <= v_window_start then
    raise exception 'p_window_end must be greater than p_window_start';
  end if;

  return query
  with raw as (
    select
      ts.id as timeseries_id,
      ts.station_id,
      op.code as pollutant_code,
      o.observed_at,
      o.value
    from uk_aq_core.observations o
    join uk_aq_core.timeseries ts
      on ts.id = o.timeseries_id
     and ts.connector_id = o.connector_id
    join uk_aq_core.phenomena p
      on p.id = ts.phenomenon_id
    join uk_aq_core.observed_properties op
      on op.id = p.observed_property_id
    where o.observed_at >= v_window_start
      and o.observed_at < v_window_end
      and ts.station_id is not null
      and op.code in ('pm25', 'pm10', 'no2')
      and o.value is not null
      and o.value >= 0
      and (
        p_station_ids is null
        or ts.station_id = any(p_station_ids)
      )
  ),
  hourly_by_timeseries as (
    select
      r.station_id,
      r.timeseries_id,
      r.pollutant_code,
      date_trunc('hour', r.observed_at at time zone 'UTC') at time zone 'UTC' as timestamp_hour_utc,
      avg(r.value)::double precision as hourly_mean_ugm3,
      count(*)::int as sample_count
    from raw r
    group by
      r.station_id,
      r.timeseries_id,
      r.pollutant_code,
      date_trunc('hour', r.observed_at at time zone 'UTC') at time zone 'UTC'
  ),
  ranked as (
    select
      h.station_id,
      h.timestamp_hour_utc,
      h.pollutant_code,
      h.hourly_mean_ugm3,
      h.sample_count,
      row_number() over (
        partition by h.station_id, h.timestamp_hour_utc, h.pollutant_code
        order by
          h.sample_count desc,
          h.timeseries_id asc
      ) as rn
    from hourly_by_timeseries h
  )
  select
    r.station_id,
    r.timestamp_hour_utc,
    r.pollutant_code,
    r.hourly_mean_ugm3,
    r.sample_count
  from ranked r
  where r.rn = 1
  order by
    r.timestamp_hour_utc,
    r.station_id,
    r.pollutant_code;
end;
$$;

revoke all on function uk_aq_public.uk_aq_rpc_station_aqi_hourly_source(
  timestamptz,
  timestamptz,
  bigint[]
) from public;

revoke all on function uk_aq_public.uk_aq_rpc_station_aqi_hourly_source(
  timestamptz,
  timestamptz,
  bigint[]
) from anon, authenticated;

grant execute on function uk_aq_public.uk_aq_rpc_station_aqi_hourly_source(
  timestamptz,
  timestamptz,
  bigint[]
) to service_role;
