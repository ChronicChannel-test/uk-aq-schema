begin;

-- Hard-cut refresh of station AQI helper RPCs so they only target uk_aq_aqilevels.
do $$
begin
  if to_regnamespace('uk_aq_aqilevels') is null then
    raise exception 'Required schema missing: uk_aq_aqilevels';
  end if;
end $$;

drop function if exists uk_aq_public.uk_aq_rpc_station_aqi_hourly_helper_upsert(
  timestamptz,
  timestamptz,
  bigint[],
  timestamptz
);

create or replace function uk_aq_public.uk_aq_rpc_station_aqi_hourly_helper_upsert(
  p_hour_end_start_exclusive timestamptz,
  p_hour_end_end_inclusive timestamptz,
  p_station_ids bigint[] default null,
  p_reference_hour_end_utc timestamptz default null
)
returns table (
  source_rows integer,
  rows_upserted integer,
  station_hours_changed integer,
  max_changed_lag_hours numeric
)
language plpgsql
security definer
set search_path = uk_aq_aqilevels, uk_aq_core, uk_aq_public, public, pg_catalog
as $$
declare
  v_start_exclusive timestamptz;
  v_end_inclusive timestamptz;
  v_source_start timestamptz;
  v_source_end timestamptz;
  v_reference_end timestamptz;
  v_source_rows integer := 0;
  v_rows_upserted integer := 0;
  v_station_hours_changed integer := 0;
  v_max_changed_lag_hours numeric := null;
begin
  if auth.role() <> 'service_role' then
    raise exception 'service_role required';
  end if;

  if p_hour_end_start_exclusive is null or p_hour_end_end_inclusive is null then
    raise exception 'p_hour_end_start_exclusive and p_hour_end_end_inclusive are required';
  end if;

  v_start_exclusive := date_trunc('hour', p_hour_end_start_exclusive);
  v_end_inclusive := date_trunc('hour', p_hour_end_end_inclusive);
  if v_end_inclusive <= v_start_exclusive then
    raise exception 'p_hour_end_end_inclusive must be greater than p_hour_end_start_exclusive';
  end if;

  v_source_start := v_start_exclusive - interval '23 hours';
  v_source_end := v_end_inclusive;
  v_reference_end := date_trunc('hour', coalesce(p_reference_hour_end_utc, v_end_inclusive));

  with source_rows as (
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
      where o.observed_at >= v_source_start
        and o.observed_at < v_source_end
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
  ),
  source_count as (
    select count(*)::integer as source_rows
    from source_rows
  ),
  hourly_pivot as (
    select
      s.station_id,
      s.timestamp_hour_utc,
      max(s.hourly_mean_ugm3) filter (where s.pollutant_code = 'no2') as no2_hourly_mean_ugm3,
      max(s.hourly_mean_ugm3) filter (where s.pollutant_code = 'pm25') as pm25_hourly_mean_ugm3,
      max(s.hourly_mean_ugm3) filter (where s.pollutant_code = 'pm10') as pm10_hourly_mean_ugm3,
      max(s.sample_count) filter (where s.pollutant_code = 'no2') as no2_hourly_sample_count_raw,
      max(s.sample_count) filter (where s.pollutant_code = 'pm25') as pm25_hourly_sample_count_raw,
      max(s.sample_count) filter (where s.pollutant_code = 'pm10') as pm10_hourly_sample_count_raw
    from source_rows s
    group by
      s.station_id,
      s.timestamp_hour_utc
  ),
  stations as (
    select distinct
      h.station_id
    from hourly_pivot h
  ),
  hour_grid as (
    select
      s.station_id,
      gs.timestamp_hour_utc
    from stations s
    cross join lateral generate_series(
      v_source_start,
      v_end_inclusive - interval '1 hour',
      interval '1 hour'
    ) as gs(timestamp_hour_utc)
  ),
  hourly as (
    select
      g.station_id,
      g.timestamp_hour_utc,
      h.no2_hourly_mean_ugm3,
      h.pm25_hourly_mean_ugm3,
      h.pm10_hourly_mean_ugm3,
      h.no2_hourly_sample_count_raw,
      h.pm25_hourly_sample_count_raw,
      h.pm10_hourly_sample_count_raw
    from hour_grid g
    left join hourly_pivot h
      on h.station_id = g.station_id
     and h.timestamp_hour_utc = g.timestamp_hour_utc
  ),
  with_rolling as (
    select
      h.station_id,
      h.timestamp_hour_utc,
      h.no2_hourly_mean_ugm3,
      h.pm25_hourly_mean_ugm3,
      h.pm10_hourly_mean_ugm3,
      h.no2_hourly_sample_count_raw,
      h.pm25_hourly_sample_count_raw,
      h.pm10_hourly_sample_count_raw,
      avg(h.pm25_hourly_mean_ugm3) over w as pm25_rolling24h_mean_raw,
      count(h.pm25_hourly_mean_ugm3) over w as pm25_rolling24h_valid_hours_raw,
      avg(h.pm10_hourly_mean_ugm3) over w as pm10_rolling24h_mean_raw,
      count(h.pm10_hourly_mean_ugm3) over w as pm10_rolling24h_valid_hours_raw
    from hourly h
    window w as (
      partition by h.station_id
      order by h.timestamp_hour_utc
      rows between 23 preceding and current row
    )
  ),
  target_hours as (
    select
      wr.station_id,
      wr.timestamp_hour_utc,
      wr.no2_hourly_mean_ugm3,
      wr.pm25_hourly_mean_ugm3,
      wr.pm10_hourly_mean_ugm3,
      wr.no2_hourly_sample_count_raw,
      wr.pm25_hourly_sample_count_raw,
      wr.pm10_hourly_sample_count_raw,
      case
        when wr.pm25_rolling24h_valid_hours_raw >= 18 then wr.pm25_rolling24h_mean_raw
        else null
      end as pm25_rolling24h_mean_ugm3,
      case
        when wr.pm10_rolling24h_valid_hours_raw >= 18 then wr.pm10_rolling24h_mean_raw
        else null
      end as pm10_rolling24h_mean_ugm3
    from with_rolling wr
    where wr.timestamp_hour_utc > (v_start_exclusive - interval '1 hour')
      and wr.timestamp_hour_utc <= (v_end_inclusive - interval '1 hour')
  ),
  computed as (
    select
      t.station_id,
      t.timestamp_hour_utc,
      t.no2_hourly_mean_ugm3,
      t.pm25_hourly_mean_ugm3,
      t.pm10_hourly_mean_ugm3,
      t.pm25_rolling24h_mean_ugm3,
      t.pm10_rolling24h_mean_ugm3,
      case
        when t.no2_hourly_sample_count_raw is null then null
        else least(32767, greatest(0, t.no2_hourly_sample_count_raw))::smallint
      end as no2_hourly_sample_count,
      case
        when t.pm25_hourly_sample_count_raw is null then null
        else least(32767, greatest(0, t.pm25_hourly_sample_count_raw))::smallint
      end as pm25_hourly_sample_count,
      case
        when t.pm10_hourly_sample_count_raw is null then null
        else least(32767, greatest(0, t.pm10_hourly_sample_count_raw))::smallint
      end as pm10_hourly_sample_count
    from target_hours t
    where
      t.no2_hourly_mean_ugm3 is not null
      or t.pm25_hourly_mean_ugm3 is not null
      or t.pm10_hourly_mean_ugm3 is not null
      or t.pm25_rolling24h_mean_ugm3 is not null
      or t.pm10_rolling24h_mean_ugm3 is not null
  ),
  changed as (
    select
      c.*
    from computed c
    left join uk_aq_aqilevels.station_aqi_hourly_helper e
      on e.station_id = c.station_id
     and e.timestamp_hour_utc = c.timestamp_hour_utc
    where
      e.station_id is null
      or (
        (
          e.no2_hourly_mean_ugm3,
          e.pm25_hourly_mean_ugm3,
          e.pm10_hourly_mean_ugm3,
          e.pm25_rolling24h_mean_ugm3,
          e.pm10_rolling24h_mean_ugm3,
          e.no2_hourly_sample_count,
          e.pm25_hourly_sample_count,
          e.pm10_hourly_sample_count
        )
        is distinct from
        (
          c.no2_hourly_mean_ugm3,
          c.pm25_hourly_mean_ugm3,
          c.pm10_hourly_mean_ugm3,
          c.pm25_rolling24h_mean_ugm3,
          c.pm10_rolling24h_mean_ugm3,
          c.no2_hourly_sample_count,
          c.pm25_hourly_sample_count,
          c.pm10_hourly_sample_count
        )
      )
  ),
  upserted as (
    insert into uk_aq_aqilevels.station_aqi_hourly_helper (
      station_id,
      timestamp_hour_utc,
      no2_hourly_mean_ugm3,
      pm25_hourly_mean_ugm3,
      pm10_hourly_mean_ugm3,
      pm25_rolling24h_mean_ugm3,
      pm10_rolling24h_mean_ugm3,
      no2_hourly_sample_count,
      pm25_hourly_sample_count,
      pm10_hourly_sample_count,
      updated_at
    )
    select
      c.station_id,
      c.timestamp_hour_utc,
      c.no2_hourly_mean_ugm3,
      c.pm25_hourly_mean_ugm3,
      c.pm10_hourly_mean_ugm3,
      c.pm25_rolling24h_mean_ugm3,
      c.pm10_rolling24h_mean_ugm3,
      c.no2_hourly_sample_count,
      c.pm25_hourly_sample_count,
      c.pm10_hourly_sample_count,
      now()
    from changed c
    on conflict (station_id, timestamp_hour_utc) do update
    set
      no2_hourly_mean_ugm3 = excluded.no2_hourly_mean_ugm3,
      pm25_hourly_mean_ugm3 = excluded.pm25_hourly_mean_ugm3,
      pm10_hourly_mean_ugm3 = excluded.pm10_hourly_mean_ugm3,
      pm25_rolling24h_mean_ugm3 = excluded.pm25_rolling24h_mean_ugm3,
      pm10_rolling24h_mean_ugm3 = excluded.pm10_rolling24h_mean_ugm3,
      no2_hourly_sample_count = excluded.no2_hourly_sample_count,
      pm25_hourly_sample_count = excluded.pm25_hourly_sample_count,
      pm10_hourly_sample_count = excluded.pm10_hourly_sample_count,
      updated_at = now()
    where
      (
        uk_aq_aqilevels.station_aqi_hourly_helper.no2_hourly_mean_ugm3,
        uk_aq_aqilevels.station_aqi_hourly_helper.pm25_hourly_mean_ugm3,
        uk_aq_aqilevels.station_aqi_hourly_helper.pm10_hourly_mean_ugm3,
        uk_aq_aqilevels.station_aqi_hourly_helper.pm25_rolling24h_mean_ugm3,
        uk_aq_aqilevels.station_aqi_hourly_helper.pm10_rolling24h_mean_ugm3,
        uk_aq_aqilevels.station_aqi_hourly_helper.no2_hourly_sample_count,
        uk_aq_aqilevels.station_aqi_hourly_helper.pm25_hourly_sample_count,
        uk_aq_aqilevels.station_aqi_hourly_helper.pm10_hourly_sample_count
      )
      is distinct from
      (
        excluded.no2_hourly_mean_ugm3,
        excluded.pm25_hourly_mean_ugm3,
        excluded.pm10_hourly_mean_ugm3,
        excluded.pm25_rolling24h_mean_ugm3,
        excluded.pm10_rolling24h_mean_ugm3,
        excluded.no2_hourly_sample_count,
        excluded.pm25_hourly_sample_count,
        excluded.pm10_hourly_sample_count
      )
    returning
      station_id,
      timestamp_hour_utc
  )
  select
    coalesce((select sc.source_rows from source_count sc), 0),
    coalesce((select count(*)::integer from upserted), 0),
    coalesce((select count(*)::integer from upserted), 0),
    (
      select max(
        greatest(
          0,
          extract(epoch from (v_reference_end - (u.timestamp_hour_utc + interval '1 hour'))) / 3600.0
        )
      )::numeric
      from upserted u
    )
  into
    v_source_rows,
    v_rows_upserted,
    v_station_hours_changed,
    v_max_changed_lag_hours;

  return query
  select
    coalesce(v_source_rows, 0),
    coalesce(v_rows_upserted, 0),
    coalesce(v_station_hours_changed, 0),
    v_max_changed_lag_hours;
end;
$$;

drop function if exists uk_aq_public.uk_aq_rpc_station_aqi_hourly_helper_window(
  timestamptz,
  timestamptz,
  bigint[]
);

create or replace function uk_aq_public.uk_aq_rpc_station_aqi_hourly_helper_window(
  p_hour_end_start_exclusive timestamptz,
  p_hour_end_end_inclusive timestamptz,
  p_station_ids bigint[] default null
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
  pm10_hourly_sample_count smallint
)
language plpgsql
security definer
set search_path = uk_aq_aqilevels, public, pg_catalog
as $$
declare
  v_start_exclusive timestamptz;
  v_end_inclusive timestamptz;
begin
  if auth.role() <> 'service_role' then
    raise exception 'service_role required';
  end if;

  if p_hour_end_start_exclusive is null or p_hour_end_end_inclusive is null then
    raise exception 'p_hour_end_start_exclusive and p_hour_end_end_inclusive are required';
  end if;

  v_start_exclusive := date_trunc('hour', p_hour_end_start_exclusive);
  v_end_inclusive := date_trunc('hour', p_hour_end_end_inclusive);
  if v_end_inclusive <= v_start_exclusive then
    raise exception 'p_hour_end_end_inclusive must be greater than p_hour_end_start_exclusive';
  end if;

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
    h.pm10_hourly_sample_count
  from uk_aq_aqilevels.station_aqi_hourly_helper h
  where h.timestamp_hour_utc > (v_start_exclusive - interval '1 hour')
    and h.timestamp_hour_utc <= (v_end_inclusive - interval '1 hour')
    and (p_station_ids is null or h.station_id = any(p_station_ids))
  order by
    h.timestamp_hour_utc,
    h.station_id;
end;
$$;

drop function if exists uk_aq_public.uk_aq_rpc_station_aqi_hourly_helper_cleanup(integer);

create or replace function uk_aq_public.uk_aq_rpc_station_aqi_hourly_helper_cleanup(
  p_retention_days integer default 45
)
returns table (
  rows_deleted bigint
)
language plpgsql
security definer
set search_path = uk_aq_aqilevels, public, pg_catalog
as $$
declare
  v_days integer;
  v_rows bigint := 0;
begin
  if auth.role() is not null and auth.role() <> 'service_role' then
    raise exception 'service_role required';
  end if;

  v_days := greatest(1, least(coalesce(p_retention_days, 45), 3650));

  delete from uk_aq_aqilevels.station_aqi_hourly_helper
  where timestamp_hour_utc < date_trunc('hour', now()) - make_interval(days => v_days);

  get diagnostics v_rows = row_count;

  return query select coalesce(v_rows, 0);
end;
$$;

revoke all on function uk_aq_public.uk_aq_rpc_station_aqi_hourly_helper_upsert(
  timestamptz,
  timestamptz,
  bigint[],
  timestamptz
) from public;

revoke all on function uk_aq_public.uk_aq_rpc_station_aqi_hourly_helper_upsert(
  timestamptz,
  timestamptz,
  bigint[],
  timestamptz
) from anon, authenticated;

grant execute on function uk_aq_public.uk_aq_rpc_station_aqi_hourly_helper_upsert(
  timestamptz,
  timestamptz,
  bigint[],
  timestamptz
) to service_role;

revoke all on function uk_aq_public.uk_aq_rpc_station_aqi_hourly_helper_window(
  timestamptz,
  timestamptz,
  bigint[]
) from public;

revoke all on function uk_aq_public.uk_aq_rpc_station_aqi_hourly_helper_window(
  timestamptz,
  timestamptz,
  bigint[]
) from anon, authenticated;

grant execute on function uk_aq_public.uk_aq_rpc_station_aqi_hourly_helper_window(
  timestamptz,
  timestamptz,
  bigint[]
) to service_role;

revoke all on function uk_aq_public.uk_aq_rpc_station_aqi_hourly_helper_cleanup(integer) from public;

revoke all on function uk_aq_public.uk_aq_rpc_station_aqi_hourly_helper_cleanup(integer) from anon, authenticated;

grant execute on function uk_aq_public.uk_aq_rpc_station_aqi_hourly_helper_cleanup(integer) to service_role;

commit;
