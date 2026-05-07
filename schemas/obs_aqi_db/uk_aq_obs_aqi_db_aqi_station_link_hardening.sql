-- Focused apply: AQI station-link hardening and rollup join fix
-- Generated from uk_aq_obs_aqi_db_schema.sql

drop function if exists uk_aq_public.uk_aq_rpc_timeseries_aqi_hourly_upsert(
  jsonb,
  timestamptz,
  timestamptz
);

create or replace function uk_aq_public.uk_aq_rpc_timeseries_aqi_hourly_upsert(
  p_rows jsonb,
  p_late_cutoff_hour timestamptz default null,
  p_reference_hour timestamptz default null
)
returns table (
  rows_attempted integer,
  rows_changed integer,
  rows_inserted integer,
  rows_updated integer,
  timeseries_hours_changed integer,
  timeseries_hours_changed_gt_cutoff integer,
  max_changed_lag_hours numeric
)
language plpgsql
security definer
set search_path = uk_aq_aqilevels, public, pg_catalog
as $$
declare
  v_rows_attempted integer := 0;
  v_rows_changed integer := 0;
  v_rows_inserted integer := 0;
  v_rows_updated integer := 0;
  v_timeseries_hours_changed integer := 0;
  v_timeseries_hours_changed_gt_cutoff integer := 0;
  v_max_changed_lag_hours numeric := null;
  v_reference_effective_date date := null;
begin
  if auth.role() <> 'service_role' then
    raise exception 'service_role required';
  end if;

  if p_rows is null
     or jsonb_typeof(p_rows) <> 'array'
     or jsonb_array_length(p_rows) = 0 then
    return query
    select 0, 0, 0, 0, 0, 0, null::numeric;
    return;
  end if;

  if p_reference_hour is not null then
    v_reference_effective_date := (p_reference_hour at time zone 'UTC')::date;
  end if;

  with incoming_raw as (
    select
      r.timeseries_id,
      r.station_id,
      r.connector_id,
      r.pollutant_code,
      date_trunc('hour', r.timestamp_hour_utc) as timestamp_hour_utc,
      r.hourly_mean_ugm3,
      r.rolling24h_mean_ugm3,
      r.no2_hourly_mean_ugm3,
      r.pm25_hourly_mean_ugm3,
      r.pm10_hourly_mean_ugm3,
      r.pm25_rolling24h_mean_ugm3,
      r.pm10_rolling24h_mean_ugm3,
      r.hourly_sample_count
    from jsonb_to_recordset(p_rows) as r(
      timeseries_id integer,
      station_id bigint,
      connector_id integer,
      pollutant_code text,
      timestamp_hour_utc timestamptz,
      hourly_mean_ugm3 double precision,
      rolling24h_mean_ugm3 double precision,
      no2_hourly_mean_ugm3 double precision,
      pm25_hourly_mean_ugm3 double precision,
      pm10_hourly_mean_ugm3 double precision,
      pm25_rolling24h_mean_ugm3 double precision,
      pm10_rolling24h_mean_ugm3 double precision,
      hourly_sample_count smallint
    )
    where r.timeseries_id is not null
      and r.connector_id is not null
      and r.timestamp_hour_utc is not null
      and r.pollutant_code in ('pm25', 'pm10', 'no2')
  ),
  incoming_base as (
    select
      r.timeseries_id,
      coalesce(r.station_id, ts.station_id) as station_id,
      r.connector_id,
      r.pollutant_code,
      r.timestamp_hour_utc,
      coalesce(
        r.hourly_mean_ugm3,
        case r.pollutant_code
          when 'no2' then r.no2_hourly_mean_ugm3
          when 'pm25' then r.pm25_hourly_mean_ugm3
          when 'pm10' then r.pm10_hourly_mean_ugm3
          else null
        end
      ) as hourly_mean_ugm3,
      case r.pollutant_code
        when 'pm25' then coalesce(r.rolling24h_mean_ugm3, r.pm25_rolling24h_mean_ugm3)
        when 'pm10' then coalesce(r.rolling24h_mean_ugm3, r.pm10_rolling24h_mean_ugm3)
        else null
      end as rolling24h_mean_ugm3,
      r.hourly_sample_count
    from incoming_raw r
    left join uk_aq_core.timeseries ts
      on ts.id = r.timeseries_id
     and ts.connector_id = r.connector_id
  ),
  incoming as (
    select
      b.timeseries_id,
      b.station_id,
      b.connector_id,
      b.pollutant_code,
      b.timestamp_hour_utc,
      b.hourly_mean_ugm3,
      b.rolling24h_mean_ugm3,
      b.hourly_sample_count,
      daqi.index_level as daqi_index_level,
      eaqi.index_level as eaqi_index_level
    from incoming_base b
    left join lateral uk_aq_aqilevels.uk_aq_aqi_index_lookup(
      'daqi',
      b.pollutant_code,
      case
        when b.pollutant_code = 'no2' then 'hourly_mean'
        else 'rolling_24h_mean'
      end,
      case
        when b.pollutant_code = 'no2' then b.hourly_mean_ugm3
        else b.rolling24h_mean_ugm3
      end,
      coalesce(v_reference_effective_date, (b.timestamp_hour_utc at time zone 'UTC')::date)
    ) daqi on true
    left join lateral uk_aq_aqilevels.uk_aq_aqi_index_lookup(
      'eaqi',
      b.pollutant_code,
      'hourly_mean',
      b.hourly_mean_ugm3,
      coalesce(v_reference_effective_date, (b.timestamp_hour_utc at time zone 'UTC')::date)
    ) eaqi on true
  ),
  dedup as (
    select distinct on (timeseries_id, timestamp_hour_utc)
      i.*
    from incoming i
    order by i.timeseries_id, i.timestamp_hour_utc
  ),
  compared as (
    select
      d.*,
      (e.timeseries_id is null) as is_insert,
      (
        e.timeseries_id is null
        or (
          (
            e.station_id,
            e.connector_id,
            e.pollutant_code,
            e.hourly_mean_ugm3,
            e.rolling24h_mean_ugm3,
            e.hourly_sample_count,
            e.daqi_index_level,
            e.eaqi_index_level
          )
          is distinct from
          (
            d.station_id,
            d.connector_id,
            d.pollutant_code,
            d.hourly_mean_ugm3,
            d.rolling24h_mean_ugm3,
            d.hourly_sample_count,
            d.daqi_index_level,
            d.eaqi_index_level
          )
        )
      ) as is_changed
    from dedup d
    left join uk_aq_aqilevels.timeseries_aqi_hourly e
      on e.timeseries_id = d.timeseries_id
     and e.timestamp_hour_utc = d.timestamp_hour_utc
  )
  select
    count(*)::integer,
    count(*) filter (where is_changed)::integer,
    count(*) filter (where is_changed and is_insert)::integer,
    count(*) filter (where is_changed and not is_insert)::integer,
    count(*) filter (where is_changed)::integer,
    count(*) filter (
      where is_changed
        and p_late_cutoff_hour is not null
        and timestamp_hour_utc < p_late_cutoff_hour
    )::integer,
    max(
      case
        when is_changed and p_reference_hour is not null then
          greatest(
            0,
            extract(epoch from (p_reference_hour - timestamp_hour_utc)) / 3600.0
          )
        else null
      end
    )::numeric
  into
    v_rows_attempted,
    v_rows_changed,
    v_rows_inserted,
    v_rows_updated,
    v_timeseries_hours_changed,
    v_timeseries_hours_changed_gt_cutoff,
    v_max_changed_lag_hours
  from compared;

  with incoming_raw as (
    select
      r.timeseries_id,
      r.station_id,
      r.connector_id,
      r.pollutant_code,
      date_trunc('hour', r.timestamp_hour_utc) as timestamp_hour_utc,
      r.hourly_mean_ugm3,
      r.rolling24h_mean_ugm3,
      r.no2_hourly_mean_ugm3,
      r.pm25_hourly_mean_ugm3,
      r.pm10_hourly_mean_ugm3,
      r.pm25_rolling24h_mean_ugm3,
      r.pm10_rolling24h_mean_ugm3,
      r.hourly_sample_count
    from jsonb_to_recordset(p_rows) as r(
      timeseries_id integer,
      station_id bigint,
      connector_id integer,
      pollutant_code text,
      timestamp_hour_utc timestamptz,
      hourly_mean_ugm3 double precision,
      rolling24h_mean_ugm3 double precision,
      no2_hourly_mean_ugm3 double precision,
      pm25_hourly_mean_ugm3 double precision,
      pm10_hourly_mean_ugm3 double precision,
      pm25_rolling24h_mean_ugm3 double precision,
      pm10_rolling24h_mean_ugm3 double precision,
      hourly_sample_count smallint
    )
    where r.timeseries_id is not null
      and r.connector_id is not null
      and r.timestamp_hour_utc is not null
      and r.pollutant_code in ('pm25', 'pm10', 'no2')
  ),
  incoming_base as (
    select
      r.timeseries_id,
      coalesce(r.station_id, ts.station_id) as station_id,
      r.connector_id,
      r.pollutant_code,
      r.timestamp_hour_utc,
      coalesce(
        r.hourly_mean_ugm3,
        case r.pollutant_code
          when 'no2' then r.no2_hourly_mean_ugm3
          when 'pm25' then r.pm25_hourly_mean_ugm3
          when 'pm10' then r.pm10_hourly_mean_ugm3
          else null
        end
      ) as hourly_mean_ugm3,
      case r.pollutant_code
        when 'pm25' then coalesce(r.rolling24h_mean_ugm3, r.pm25_rolling24h_mean_ugm3)
        when 'pm10' then coalesce(r.rolling24h_mean_ugm3, r.pm10_rolling24h_mean_ugm3)
        else null
      end as rolling24h_mean_ugm3,
      r.hourly_sample_count
    from incoming_raw r
    left join uk_aq_core.timeseries ts
      on ts.id = r.timeseries_id
     and ts.connector_id = r.connector_id
  ),
  incoming as (
    select
      b.timeseries_id,
      b.station_id,
      b.connector_id,
      b.pollutant_code,
      b.timestamp_hour_utc,
      b.hourly_mean_ugm3,
      b.rolling24h_mean_ugm3,
      b.hourly_sample_count,
      daqi.index_level as daqi_index_level,
      eaqi.index_level as eaqi_index_level
    from incoming_base b
    left join lateral uk_aq_aqilevels.uk_aq_aqi_index_lookup(
      'daqi',
      b.pollutant_code,
      case
        when b.pollutant_code = 'no2' then 'hourly_mean'
        else 'rolling_24h_mean'
      end,
      case
        when b.pollutant_code = 'no2' then b.hourly_mean_ugm3
        else b.rolling24h_mean_ugm3
      end,
      coalesce(v_reference_effective_date, (b.timestamp_hour_utc at time zone 'UTC')::date)
    ) daqi on true
    left join lateral uk_aq_aqilevels.uk_aq_aqi_index_lookup(
      'eaqi',
      b.pollutant_code,
      'hourly_mean',
      b.hourly_mean_ugm3,
      coalesce(v_reference_effective_date, (b.timestamp_hour_utc at time zone 'UTC')::date)
    ) eaqi on true
  ),
  dedup as (
    select distinct on (timeseries_id, timestamp_hour_utc)
      i.*
    from incoming i
    order by i.timeseries_id, i.timestamp_hour_utc
  ),
  changed as (
    select
      d.*
    from dedup d
    left join uk_aq_aqilevels.timeseries_aqi_hourly e
      on e.timeseries_id = d.timeseries_id
     and e.timestamp_hour_utc = d.timestamp_hour_utc
    where
      e.timeseries_id is null
      or (
        (
          e.station_id,
          e.connector_id,
          e.pollutant_code,
          e.hourly_mean_ugm3,
          e.rolling24h_mean_ugm3,
          e.hourly_sample_count,
          e.daqi_index_level,
          e.eaqi_index_level
        )
        is distinct from
        (
          d.station_id,
          d.connector_id,
          d.pollutant_code,
          d.hourly_mean_ugm3,
          d.rolling24h_mean_ugm3,
          d.hourly_sample_count,
          d.daqi_index_level,
          d.eaqi_index_level
        )
      )
  )
  insert into uk_aq_aqilevels.timeseries_aqi_hourly (
    timeseries_id,
    station_id,
    connector_id,
    pollutant_code,
    timestamp_hour_utc,
    hourly_mean_ugm3,
    rolling24h_mean_ugm3,
    hourly_sample_count,
    daqi_index_level,
    eaqi_index_level,
    updated_at
  )
  select
    c.timeseries_id,
    c.station_id,
    c.connector_id,
    c.pollutant_code,
    c.timestamp_hour_utc,
    c.hourly_mean_ugm3,
    c.rolling24h_mean_ugm3,
    c.hourly_sample_count,
    c.daqi_index_level,
    c.eaqi_index_level,
    now()
  from changed c
  on conflict (timeseries_id, timestamp_hour_utc) do update
  set
    station_id = excluded.station_id,
    connector_id = excluded.connector_id,
    pollutant_code = excluded.pollutant_code,
    hourly_mean_ugm3 = excluded.hourly_mean_ugm3,
    rolling24h_mean_ugm3 = excluded.rolling24h_mean_ugm3,
    hourly_sample_count = excluded.hourly_sample_count,
    daqi_index_level = excluded.daqi_index_level,
    eaqi_index_level = excluded.eaqi_index_level,
    updated_at = now()
  where
    (
      uk_aq_aqilevels.timeseries_aqi_hourly.station_id,
      uk_aq_aqilevels.timeseries_aqi_hourly.connector_id,
      uk_aq_aqilevels.timeseries_aqi_hourly.pollutant_code,
      uk_aq_aqilevels.timeseries_aqi_hourly.hourly_mean_ugm3,
      uk_aq_aqilevels.timeseries_aqi_hourly.rolling24h_mean_ugm3,
      uk_aq_aqilevels.timeseries_aqi_hourly.hourly_sample_count,
      uk_aq_aqilevels.timeseries_aqi_hourly.daqi_index_level,
      uk_aq_aqilevels.timeseries_aqi_hourly.eaqi_index_level
    )
    is distinct from
    (
      excluded.station_id,
      excluded.connector_id,
      excluded.pollutant_code,
      excluded.hourly_mean_ugm3,
      excluded.rolling24h_mean_ugm3,
      excluded.hourly_sample_count,
      excluded.daqi_index_level,
      excluded.eaqi_index_level
    );

  return query
  select
    coalesce(v_rows_attempted, 0),
    coalesce(v_rows_changed, 0),
    coalesce(v_rows_inserted, 0),
    coalesce(v_rows_updated, 0),
    coalesce(v_timeseries_hours_changed, 0),
    coalesce(v_timeseries_hours_changed_gt_cutoff, 0),
    v_max_changed_lag_hours;
end;
$$;

drop function if exists uk_aq_public.uk_aq_rpc_timeseries_aqi_rollups_refresh(
  timestamptz,
  timestamptz,
  integer[]
);

create or replace function uk_aq_public.uk_aq_rpc_timeseries_aqi_rollups_refresh(
  p_start_hour_utc timestamptz,
  p_end_hour_utc timestamptz,
  p_timeseries_ids integer[] default null
)
returns table (
  daily_rows_upserted bigint,
  monthly_rows_upserted bigint
)
language plpgsql
security definer
set search_path = uk_aq_aqilevels, public, pg_catalog
as $$
declare
  v_start_hour timestamptz;
  v_end_hour timestamptz;
  v_start_day date;
  v_end_day date;
  v_start_month date;
  v_end_month date;
  v_daily_rows bigint := 0;
  v_monthly_rows bigint := 0;
begin
  if auth.role() <> 'service_role' then
    raise exception 'service_role required';
  end if;

  if p_start_hour_utc is null or p_end_hour_utc is null then
    raise exception 'p_start_hour_utc and p_end_hour_utc are required';
  end if;

  v_start_hour := date_trunc('hour', p_start_hour_utc);
  v_end_hour := date_trunc('hour', p_end_hour_utc);
  if v_end_hour <= v_start_hour then
    raise exception 'p_end_hour_utc must be greater than p_start_hour_utc';
  end if;

  set local timezone = 'UTC';

  v_start_day := v_start_hour::date;
  v_end_day := (v_end_hour - interval '1 hour')::date;
  v_start_month := date_trunc('month', v_start_day::timestamp)::date;
  v_end_month := date_trunc('month', v_end_day::timestamp)::date;

  delete from uk_aq_aqilevels.timeseries_aqi_daily d
  where d.observed_day between v_start_day and v_end_day
    and (p_timeseries_ids is null or d.timeseries_id = any(p_timeseries_ids));

  with flat as (
    select
      h.timeseries_id,
      h.station_id,
      h.connector_id,
      h.pollutant_code,
      (h.timestamp_hour_utc at time zone 'UTC')::date as observed_day,
      'daqi'::text as standard_code,
      h.daqi_index_level as index_level
    from uk_aq_aqilevels.timeseries_aqi_hourly h
    where h.timestamp_hour_utc >= v_start_day::timestamptz
      and h.timestamp_hour_utc < (v_end_day + 1)::timestamptz
      and (p_timeseries_ids is null or h.timeseries_id = any(p_timeseries_ids))
    union all
    select
      h.timeseries_id,
      h.station_id,
      h.connector_id,
      h.pollutant_code,
      (h.timestamp_hour_utc at time zone 'UTC')::date,
      'eaqi'::text,
      h.eaqi_index_level
    from uk_aq_aqilevels.timeseries_aqi_hourly h
    where h.timestamp_hour_utc >= v_start_day::timestamptz
      and h.timestamp_hour_utc < (v_end_day + 1)::timestamptz
      and (p_timeseries_ids is null or h.timeseries_id = any(p_timeseries_ids))
  ),
  counts as (
    select
      f.timeseries_id,
      f.station_id,
      f.connector_id,
      f.pollutant_code,
      f.observed_day,
      f.standard_code,
      f.index_level,
      count(*)::integer as cnt
    from flat f
    where f.index_level is not null
    group by
      f.timeseries_id,
      f.station_id,
      f.connector_id,
      f.pollutant_code,
      f.observed_day,
      f.standard_code,
      f.index_level
  ),
  keys as (
    select distinct
      c.timeseries_id,
      c.station_id,
      c.connector_id,
      c.pollutant_code,
      c.observed_day,
      c.standard_code
    from counts c
  ),
  assembled as (
    select
      k.timeseries_id,
      k.station_id,
      k.connector_id,
      k.pollutant_code,
      k.observed_day,
      k.standard_code,
      array_agg(coalesce(c.cnt, 0)::integer order by lvl.level) as index_level_hour_counts,
      coalesce(sum(c.cnt), 0)::smallint as valid_hour_count,
      max(c.index_level)::smallint as max_index_level
    from keys k
    cross join lateral generate_series(
      1,
      case when k.standard_code = 'daqi' then 10 else 6 end
    ) as lvl(level)
    left join counts c
      on c.timeseries_id = k.timeseries_id
     and c.observed_day = k.observed_day
     and c.standard_code = k.standard_code
     and c.pollutant_code = k.pollutant_code
     and c.station_id is not distinct from k.station_id
     and c.connector_id is not distinct from k.connector_id
     and c.index_level = lvl.level
    group by
      k.timeseries_id,
      k.station_id,
      k.connector_id,
      k.pollutant_code,
      k.observed_day,
      k.standard_code
  )
  insert into uk_aq_aqilevels.timeseries_aqi_daily (
    timeseries_id,
    station_id,
    connector_id,
    pollutant_code,
    observed_day,
    standard_code,
    index_level_hour_counts,
    valid_hour_count,
    max_index_level,
    updated_at
  )
  select
    a.timeseries_id,
    a.station_id,
    a.connector_id,
    a.pollutant_code,
    a.observed_day,
    a.standard_code,
    a.index_level_hour_counts,
    a.valid_hour_count,
    a.max_index_level,
    now()
  from assembled a
  on conflict (timeseries_id, observed_day, standard_code, pollutant_code) do update
  set
    station_id = excluded.station_id,
    connector_id = excluded.connector_id,
    index_level_hour_counts = excluded.index_level_hour_counts,
    valid_hour_count = excluded.valid_hour_count,
    max_index_level = excluded.max_index_level,
    updated_at = now();

  get diagnostics v_daily_rows = row_count;

  delete from uk_aq_aqilevels.timeseries_aqi_monthly m
  where m.observed_month between v_start_month and v_end_month
    and (p_timeseries_ids is null or m.timeseries_id = any(p_timeseries_ids));

  with flat as (
    select
      h.timeseries_id,
      h.station_id,
      h.connector_id,
      h.pollutant_code,
      date_trunc('month', h.timestamp_hour_utc at time zone 'UTC')::date as observed_month,
      'daqi'::text as standard_code,
      h.daqi_index_level as index_level
    from uk_aq_aqilevels.timeseries_aqi_hourly h
    where h.timestamp_hour_utc >= v_start_month::timestamptz
      and h.timestamp_hour_utc < (v_end_month + interval '1 month')::timestamptz
      and (p_timeseries_ids is null or h.timeseries_id = any(p_timeseries_ids))
    union all
    select
      h.timeseries_id,
      h.station_id,
      h.connector_id,
      h.pollutant_code,
      date_trunc('month', h.timestamp_hour_utc at time zone 'UTC')::date,
      'eaqi'::text,
      h.eaqi_index_level
    from uk_aq_aqilevels.timeseries_aqi_hourly h
    where h.timestamp_hour_utc >= v_start_month::timestamptz
      and h.timestamp_hour_utc < (v_end_month + interval '1 month')::timestamptz
      and (p_timeseries_ids is null or h.timeseries_id = any(p_timeseries_ids))
  ),
  counts as (
    select
      f.timeseries_id,
      f.station_id,
      f.connector_id,
      f.pollutant_code,
      f.observed_month,
      f.standard_code,
      f.index_level,
      count(*)::integer as cnt
    from flat f
    where f.index_level is not null
    group by
      f.timeseries_id,
      f.station_id,
      f.connector_id,
      f.pollutant_code,
      f.observed_month,
      f.standard_code,
      f.index_level
  ),
  keys as (
    select distinct
      c.timeseries_id,
      c.station_id,
      c.connector_id,
      c.pollutant_code,
      c.observed_month,
      c.standard_code
    from counts c
  ),
  assembled as (
    select
      k.timeseries_id,
      k.station_id,
      k.connector_id,
      k.pollutant_code,
      k.observed_month,
      k.standard_code,
      array_agg(coalesce(c.cnt, 0)::integer order by lvl.level) as index_level_hour_counts,
      coalesce(sum(c.cnt), 0)::integer as valid_hour_count,
      max(c.index_level)::smallint as max_index_level
    from keys k
    cross join lateral generate_series(
      1,
      case when k.standard_code = 'daqi' then 10 else 6 end
    ) as lvl(level)
    left join counts c
      on c.timeseries_id = k.timeseries_id
     and c.observed_month = k.observed_month
     and c.standard_code = k.standard_code
     and c.pollutant_code = k.pollutant_code
     and c.station_id is not distinct from k.station_id
     and c.connector_id is not distinct from k.connector_id
     and c.index_level = lvl.level
    group by
      k.timeseries_id,
      k.station_id,
      k.connector_id,
      k.pollutant_code,
      k.observed_month,
      k.standard_code
  )
  insert into uk_aq_aqilevels.timeseries_aqi_monthly (
    timeseries_id,
    station_id,
    connector_id,
    pollutant_code,
    observed_month,
    standard_code,
    index_level_hour_counts,
    valid_hour_count,
    max_index_level,
    updated_at
  )
  select
    a.timeseries_id,
    a.station_id,
    a.connector_id,
    a.pollutant_code,
    a.observed_month,
    a.standard_code,
    a.index_level_hour_counts,
    a.valid_hour_count,
    a.max_index_level,
    now()
  from assembled a
  on conflict (timeseries_id, observed_month, standard_code, pollutant_code) do update
  set
    station_id = excluded.station_id,
    connector_id = excluded.connector_id,
    index_level_hour_counts = excluded.index_level_hour_counts,
    valid_hour_count = excluded.valid_hour_count,
    max_index_level = excluded.max_index_level,
    updated_at = now();

  get diagnostics v_monthly_rows = row_count;

  return query
  select
    coalesce(v_daily_rows, 0),
    coalesce(v_monthly_rows, 0);
end;
$$;

revoke all on function uk_aq_public.uk_aq_rpc_timeseries_aqi_hourly_upsert(
  jsonb,
  timestamptz,
  timestamptz
) from public;
grant execute on function uk_aq_public.uk_aq_rpc_timeseries_aqi_hourly_upsert(
  jsonb,
  timestamptz,
  timestamptz
) to service_role;

revoke all on function uk_aq_public.uk_aq_rpc_timeseries_aqi_rollups_refresh(
  timestamptz,
  timestamptz,
  integer[]
) from public;
grant execute on function uk_aq_public.uk_aq_rpc_timeseries_aqi_rollups_refresh(
  timestamptz,
  timestamptz,
  integer[]
) to service_role;

drop function if exists uk_aq_public.uk_aq_rpc_timeseries_aqi_station_link_health(
  timestamptz,
  timestamptz,
  integer[]
);

create or replace function uk_aq_public.uk_aq_rpc_timeseries_aqi_station_link_health(
  p_start_hour_utc timestamptz default null,
  p_end_hour_utc timestamptz default null,
  p_timeseries_ids integer[] default null
)
returns table (
  null_station_rows bigint,
  mismatched_station_rows bigint,
  null_station_timeseries integer,
  mismatched_station_timeseries integer,
  sample_null_timeseries_ids integer[],
  sample_mismatched_timeseries_ids integer[]
)
language plpgsql
security definer
set search_path = uk_aq_aqilevels, uk_aq_core, public, pg_catalog
as $$
declare
  v_start_hour timestamptz := null;
  v_end_hour timestamptz := null;
begin
  if auth.role() <> 'service_role' then
    raise exception 'service_role required';
  end if;

  if (p_start_hour_utc is null) <> (p_end_hour_utc is null) then
    raise exception 'p_start_hour_utc and p_end_hour_utc must both be null or both provided';
  end if;

  if p_start_hour_utc is not null then
    v_start_hour := date_trunc('hour', p_start_hour_utc);
    v_end_hour := date_trunc('hour', p_end_hour_utc);
    if v_end_hour <= v_start_hour then
      raise exception 'p_end_hour_utc must be greater than p_start_hour_utc';
    end if;
  end if;

  return query
  with hourly as (
    select
      h.timeseries_id,
      h.station_id,
      h.timestamp_hour_utc
    from uk_aq_aqilevels.timeseries_aqi_hourly h
    where
      (p_timeseries_ids is null or h.timeseries_id = any(p_timeseries_ids))
      and (v_start_hour is null or h.timestamp_hour_utc >= v_start_hour)
      and (v_end_hour is null or h.timestamp_hour_utc < v_end_hour)
  ),
  joined as (
    select
      h.timeseries_id,
      h.station_id as hourly_station_id,
      ts.station_id as core_station_id
    from hourly h
    join uk_aq_core.timeseries ts
      on ts.id = h.timeseries_id
  ),
  null_rows as (
    select j.timeseries_id
    from joined j
    where j.hourly_station_id is null
  ),
  mismatched_rows as (
    select j.timeseries_id
    from joined j
    where j.hourly_station_id is distinct from j.core_station_id
  ),
  null_sample as (
    select coalesce(array_agg(x.timeseries_id order by x.timeseries_id), '{}'::integer[]) as ids
    from (
      select distinct n.timeseries_id
      from null_rows n
      order by n.timeseries_id
      limit 20
    ) x
  ),
  mismatch_sample as (
    select coalesce(array_agg(x.timeseries_id order by x.timeseries_id), '{}'::integer[]) as ids
    from (
      select distinct m.timeseries_id
      from mismatched_rows m
      order by m.timeseries_id
      limit 20
    ) x
  )
  select
    (select count(*) from null_rows)::bigint as null_station_rows,
    (select count(*) from mismatched_rows)::bigint as mismatched_station_rows,
    (select count(distinct timeseries_id) from null_rows)::integer as null_station_timeseries,
    (select count(distinct timeseries_id) from mismatched_rows)::integer as mismatched_station_timeseries,
    (select ids from null_sample) as sample_null_timeseries_ids,
    (select ids from mismatch_sample) as sample_mismatched_timeseries_ids;
end;
$$;

revoke all on function uk_aq_public.uk_aq_rpc_timeseries_aqi_station_link_health(
  timestamptz,
  timestamptz,
  integer[]
) from public;
grant execute on function uk_aq_public.uk_aq_rpc_timeseries_aqi_station_link_health(
  timestamptz,
  timestamptz,
  integer[]
) to service_role;
