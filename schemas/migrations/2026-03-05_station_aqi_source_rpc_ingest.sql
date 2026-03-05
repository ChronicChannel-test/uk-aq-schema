-- Station AQI source RPC (ingest DB).
-- station AQI source RPC (service_role): station-hour pollutant means with
-- inferred cadence + completeness filtering.

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
  sample_count integer,
  expected_count integer,
  required_count integer,
  capture_ratio real,
  cadence_minutes integer,
  timeseries_id integer
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
    where o.observed_at >= v_window_start - interval '48 hours'
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
  diffs as (
    select
      r.timeseries_id,
      extract(epoch from (r.observed_at - lag(r.observed_at) over (
        partition by r.timeseries_id
        order by r.observed_at
      ))) as diff_seconds
    from raw r
  ),
  cadence as (
    select
      d.timeseries_id,
      case
        when percentile_cont(0.5) within group (order by d.diff_seconds) <= 90 then 1
        when percentile_cont(0.5) within group (order by d.diff_seconds) <= 420 then 5
        when percentile_cont(0.5) within group (order by d.diff_seconds) <= 780 then 10
        when percentile_cont(0.5) within group (order by d.diff_seconds) <= 1260 then 15
        when percentile_cont(0.5) within group (order by d.diff_seconds) <= 2700 then 30
        else 60
      end::int as cadence_minutes
    from diffs d
    where d.diff_seconds is not null
      and d.diff_seconds >= 30
      and d.diff_seconds <= 7200
    group by d.timeseries_id
  ),
  hourly_by_timeseries as (
    select
      r.station_id,
      r.timeseries_id,
      r.pollutant_code,
      date_trunc('hour', r.observed_at at time zone 'UTC') at time zone 'UTC' as timestamp_hour_utc,
      avg(r.value)::double precision as hourly_mean_ugm3,
      count(*)::int as sample_count,
      coalesce(c.cadence_minutes, 60)::int as cadence_minutes
    from raw r
    left join cadence c
      on c.timeseries_id = r.timeseries_id
    where r.observed_at >= v_window_start
      and r.observed_at < v_window_end
    group by
      r.station_id,
      r.timeseries_id,
      r.pollutant_code,
      date_trunc('hour', r.observed_at at time zone 'UTC') at time zone 'UTC',
      coalesce(c.cadence_minutes, 60)
  ),
  hourly_scored as (
    select
      h.station_id,
      h.timestamp_hour_utc,
      h.pollutant_code,
      h.hourly_mean_ugm3,
      h.sample_count,
      greatest(
        1,
        case
          when h.cadence_minutes <= 0 then 1
          else floor(60.0 / h.cadence_minutes)::int
        end
      )::int as expected_count,
      h.cadence_minutes,
      h.timeseries_id
    from hourly_by_timeseries h
  ),
  ranked as (
    select
      hs.station_id,
      hs.timestamp_hour_utc,
      hs.pollutant_code,
      hs.hourly_mean_ugm3,
      hs.sample_count,
      hs.expected_count,
      ceil(0.75 * hs.expected_count::numeric)::int as required_count,
      (
        hs.sample_count::double precision
        / nullif(hs.expected_count::double precision, 0)
      )::real as capture_ratio,
      hs.cadence_minutes,
      hs.timeseries_id,
      row_number() over (
        partition by hs.station_id, hs.timestamp_hour_utc, hs.pollutant_code
        order by
          (hs.sample_count >= ceil(0.75 * hs.expected_count::numeric)::int) desc,
          hs.sample_count desc,
          hs.timeseries_id asc
      ) as rn
    from hourly_scored hs
  )
  select
    r.station_id,
    r.timestamp_hour_utc,
    r.pollutant_code,
    r.hourly_mean_ugm3,
    r.sample_count,
    r.expected_count,
    r.required_count,
    r.capture_ratio,
    r.cadence_minutes,
    r.timeseries_id
  from ranked r
  where r.rn = 1
    and r.sample_count >= r.required_count
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
