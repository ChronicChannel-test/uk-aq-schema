-- AggDaily station AQI hourly: remove redundant columns, rename DAQI PM rolling-24h index fields,
-- and compute AQI index levels inside AggDaily hourly upsert from mean inputs.

drop view if exists uk_aq_public.uk_aq_station_aqi_hourly;

alter table if exists uk_aq_aggdaily.station_aqi_hourly
  drop column if exists no2_hourly_capture_ratio,
  drop column if exists pm25_hourly_capture_ratio,
  drop column if exists pm10_hourly_capture_ratio,
  drop column if exists no2_hourly_expected_count,
  drop column if exists pm25_hourly_expected_count,
  drop column if exists pm10_hourly_expected_count,
  drop column if exists pm25_rolling24h_valid_hours,
  drop column if exists pm10_rolling24h_valid_hours,
  drop column if exists daqi_no2_index_band,
  drop column if exists daqi_pm25_index_band,
  drop column if exists daqi_pm10_index_band,
  drop column if exists eaqi_no2_index_band,
  drop column if exists eaqi_pm25_index_band,
  drop column if exists eaqi_pm10_index_band;

do $$
begin
  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'uk_aq_aggdaily'
      and table_name = 'station_aqi_hourly'
      and column_name = 'daqi_pm25_index_level'
  ) then
    if exists (
      select 1
      from information_schema.columns
      where table_schema = 'uk_aq_aggdaily'
        and table_name = 'station_aqi_hourly'
        and column_name = 'daqi_pm25_rolling24h_index_level'
    ) then
      execute $sql$
        update uk_aq_aggdaily.station_aqi_hourly
        set daqi_pm25_rolling24h_index_level = coalesce(daqi_pm25_rolling24h_index_level, daqi_pm25_index_level)
        where daqi_pm25_index_level is not null
      $sql$;
      execute 'alter table uk_aq_aggdaily.station_aqi_hourly drop column daqi_pm25_index_level';
    else
      execute 'alter table uk_aq_aggdaily.station_aqi_hourly rename column daqi_pm25_index_level to daqi_pm25_rolling24h_index_level';
    end if;
  end if;

  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'uk_aq_aggdaily'
      and table_name = 'station_aqi_hourly'
      and column_name = 'daqi_pm10_index_level'
  ) then
    if exists (
      select 1
      from information_schema.columns
      where table_schema = 'uk_aq_aggdaily'
        and table_name = 'station_aqi_hourly'
        and column_name = 'daqi_pm10_rolling24h_index_level'
    ) then
      execute $sql$
        update uk_aq_aggdaily.station_aqi_hourly
        set daqi_pm10_rolling24h_index_level = coalesce(daqi_pm10_rolling24h_index_level, daqi_pm10_index_level)
        where daqi_pm10_index_level is not null
      $sql$;
      execute 'alter table uk_aq_aggdaily.station_aqi_hourly drop column daqi_pm10_index_level';
    else
      execute 'alter table uk_aq_aggdaily.station_aqi_hourly rename column daqi_pm10_index_level to daqi_pm10_rolling24h_index_level';
    end if;
  end if;
end
$$;

create or replace view uk_aq_public.uk_aq_station_aqi_hourly as
select
  station_id,
  timestamp_hour_utc,
  no2_hourly_mean_ugm3,
  pm25_hourly_mean_ugm3,
  pm10_hourly_mean_ugm3,
  pm25_rolling24h_mean_ugm3,
  pm10_rolling24h_mean_ugm3,
  daqi_no2_index_level,
  daqi_pm25_rolling24h_index_level,
  daqi_pm10_rolling24h_index_level,
  eaqi_no2_index_level,
  eaqi_pm25_index_level,
  eaqi_pm10_index_level,
  updated_at
from uk_aq_aggdaily.station_aqi_hourly;
alter view if exists uk_aq_public.uk_aq_station_aqi_hourly set (security_invoker = true);

revoke all on uk_aq_public.uk_aq_station_aqi_hourly from public;
grant select on uk_aq_public.uk_aq_station_aqi_hourly to authenticated;
grant select on uk_aq_public.uk_aq_station_aqi_hourly to service_role;

drop function if exists uk_aq_aggdaily.uk_aq_aqi_index_lookup(
  text,
  text,
  text,
  double precision,
  date
);

create or replace function uk_aq_aggdaily.uk_aq_aqi_index_lookup(
  p_standard_code text,
  p_pollutant_code text,
  p_averaging_code text,
  p_value double precision,
  p_effective_date date default ((now() at time zone 'UTC')::date)
)
returns table (
  index_level smallint,
  index_band text
)
language sql
stable
set search_path = uk_aq_aggdaily, public, pg_catalog
as $$
  select
    b.index_level,
    b.index_band
  from uk_aq_aggdaily.aqi_breakpoints b
  join uk_aq_aggdaily.aqi_standard_versions v
    on v.standard_code = b.standard_code
   and v.version_code = b.version_code
  where p_value is not null
    and b.standard_code = p_standard_code
    and b.pollutant_code = p_pollutant_code
    and b.averaging_code = p_averaging_code
    and (v.valid_from is null or v.valid_from <= p_effective_date)
    and (v.valid_to is null or v.valid_to >= p_effective_date)
    and (b.valid_from is null or b.valid_from <= p_effective_date)
    and (b.valid_to is null or b.valid_to >= p_effective_date)
    and p_value >= b.range_low
    and (b.range_high is null or p_value <= b.range_high)
  order by b.index_level
  limit 1;
$$;

revoke all on function uk_aq_aggdaily.uk_aq_aqi_index_lookup(
  text,
  text,
  text,
  double precision,
  date
) from public;

create or replace function uk_aq_public.uk_aq_rpc_station_aqi_hourly_upsert(
  p_rows jsonb,
  p_late_cutoff_hour timestamptz default null,
  p_reference_hour timestamptz default null
)
returns table (
  rows_attempted integer,
  rows_changed integer,
  rows_inserted integer,
  rows_updated integer,
  station_hours_changed integer,
  station_hours_changed_gt_cutoff integer,
  max_changed_lag_hours numeric
)
language plpgsql
security definer
set search_path = uk_aq_aggdaily, public, pg_catalog
as $$
declare
  v_rows_attempted integer := 0;
  v_rows_changed integer := 0;
  v_rows_inserted integer := 0;
  v_rows_updated integer := 0;
  v_station_hours_changed integer := 0;
  v_station_hours_changed_gt_cutoff integer := 0;
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

  with incoming_base as (
    select
      r.station_id,
      date_trunc('hour', r.timestamp_hour_utc) as timestamp_hour_utc,
      r.no2_hourly_mean_ugm3,
      r.pm25_hourly_mean_ugm3,
      r.pm10_hourly_mean_ugm3,
      r.pm25_rolling24h_mean_ugm3,
      r.pm10_rolling24h_mean_ugm3,
      r.no2_hourly_sample_count,
      r.pm25_hourly_sample_count,
      r.pm10_hourly_sample_count
    from jsonb_to_recordset(p_rows) as r(
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
    where r.station_id is not null
      and r.timestamp_hour_utc is not null
  ),
  incoming as (
    select
      b.station_id,
      b.timestamp_hour_utc,
      b.no2_hourly_mean_ugm3,
      b.pm25_hourly_mean_ugm3,
      b.pm10_hourly_mean_ugm3,
      b.pm25_rolling24h_mean_ugm3,
      b.pm10_rolling24h_mean_ugm3,
      b.no2_hourly_sample_count,
      b.pm25_hourly_sample_count,
      b.pm10_hourly_sample_count,
      daqi_no2.index_level as daqi_no2_index_level,
      daqi_pm25.index_level as daqi_pm25_rolling24h_index_level,
      daqi_pm10.index_level as daqi_pm10_rolling24h_index_level,
      eaqi_no2.index_level as eaqi_no2_index_level,
      eaqi_pm25.index_level as eaqi_pm25_index_level,
      eaqi_pm10.index_level as eaqi_pm10_index_level
    from incoming_base b
    left join lateral uk_aq_aggdaily.uk_aq_aqi_index_lookup(
      'daqi',
      'no2',
      'hourly_mean',
      b.no2_hourly_mean_ugm3,
      coalesce(v_reference_effective_date, (b.timestamp_hour_utc at time zone 'UTC')::date)
    ) daqi_no2 on true
    left join lateral uk_aq_aggdaily.uk_aq_aqi_index_lookup(
      'daqi',
      'pm25',
      'rolling_24h_mean',
      b.pm25_rolling24h_mean_ugm3,
      coalesce(v_reference_effective_date, (b.timestamp_hour_utc at time zone 'UTC')::date)
    ) daqi_pm25 on true
    left join lateral uk_aq_aggdaily.uk_aq_aqi_index_lookup(
      'daqi',
      'pm10',
      'rolling_24h_mean',
      b.pm10_rolling24h_mean_ugm3,
      coalesce(v_reference_effective_date, (b.timestamp_hour_utc at time zone 'UTC')::date)
    ) daqi_pm10 on true
    left join lateral uk_aq_aggdaily.uk_aq_aqi_index_lookup(
      'eaqi',
      'no2',
      'hourly_mean',
      b.no2_hourly_mean_ugm3,
      coalesce(v_reference_effective_date, (b.timestamp_hour_utc at time zone 'UTC')::date)
    ) eaqi_no2 on true
    left join lateral uk_aq_aggdaily.uk_aq_aqi_index_lookup(
      'eaqi',
      'pm25',
      'hourly_mean',
      b.pm25_hourly_mean_ugm3,
      coalesce(v_reference_effective_date, (b.timestamp_hour_utc at time zone 'UTC')::date)
    ) eaqi_pm25 on true
    left join lateral uk_aq_aggdaily.uk_aq_aqi_index_lookup(
      'eaqi',
      'pm10',
      'hourly_mean',
      b.pm10_hourly_mean_ugm3,
      coalesce(v_reference_effective_date, (b.timestamp_hour_utc at time zone 'UTC')::date)
    ) eaqi_pm10 on true
  ),
  dedup as (
    select distinct on (station_id, timestamp_hour_utc)
      i.*
    from incoming i
    order by i.station_id, i.timestamp_hour_utc
  ),
  compared as (
    select
      d.*,
      (e.station_id is null) as is_insert,
      (
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
            e.pm10_hourly_sample_count,
            e.daqi_no2_index_level,
            e.daqi_pm25_rolling24h_index_level,
            e.daqi_pm10_rolling24h_index_level,
            e.eaqi_no2_index_level,
            e.eaqi_pm25_index_level,
            e.eaqi_pm10_index_level
          )
          is distinct from
          (
            d.no2_hourly_mean_ugm3,
            d.pm25_hourly_mean_ugm3,
            d.pm10_hourly_mean_ugm3,
            d.pm25_rolling24h_mean_ugm3,
            d.pm10_rolling24h_mean_ugm3,
            d.no2_hourly_sample_count,
            d.pm25_hourly_sample_count,
            d.pm10_hourly_sample_count,
            d.daqi_no2_index_level,
            d.daqi_pm25_rolling24h_index_level,
            d.daqi_pm10_rolling24h_index_level,
            d.eaqi_no2_index_level,
            d.eaqi_pm25_index_level,
            d.eaqi_pm10_index_level
          )
        )
      ) as is_changed
    from dedup d
    left join uk_aq_aggdaily.station_aqi_hourly e
      on e.station_id = d.station_id
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
    v_station_hours_changed,
    v_station_hours_changed_gt_cutoff,
    v_max_changed_lag_hours
  from compared;

  with incoming_base as (
    select
      r.station_id,
      date_trunc('hour', r.timestamp_hour_utc) as timestamp_hour_utc,
      r.no2_hourly_mean_ugm3,
      r.pm25_hourly_mean_ugm3,
      r.pm10_hourly_mean_ugm3,
      r.pm25_rolling24h_mean_ugm3,
      r.pm10_rolling24h_mean_ugm3,
      r.no2_hourly_sample_count,
      r.pm25_hourly_sample_count,
      r.pm10_hourly_sample_count
    from jsonb_to_recordset(p_rows) as r(
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
    where r.station_id is not null
      and r.timestamp_hour_utc is not null
  ),
  incoming as (
    select
      b.station_id,
      b.timestamp_hour_utc,
      b.no2_hourly_mean_ugm3,
      b.pm25_hourly_mean_ugm3,
      b.pm10_hourly_mean_ugm3,
      b.pm25_rolling24h_mean_ugm3,
      b.pm10_rolling24h_mean_ugm3,
      b.no2_hourly_sample_count,
      b.pm25_hourly_sample_count,
      b.pm10_hourly_sample_count,
      daqi_no2.index_level as daqi_no2_index_level,
      daqi_pm25.index_level as daqi_pm25_rolling24h_index_level,
      daqi_pm10.index_level as daqi_pm10_rolling24h_index_level,
      eaqi_no2.index_level as eaqi_no2_index_level,
      eaqi_pm25.index_level as eaqi_pm25_index_level,
      eaqi_pm10.index_level as eaqi_pm10_index_level
    from incoming_base b
    left join lateral uk_aq_aggdaily.uk_aq_aqi_index_lookup(
      'daqi',
      'no2',
      'hourly_mean',
      b.no2_hourly_mean_ugm3,
      coalesce(v_reference_effective_date, (b.timestamp_hour_utc at time zone 'UTC')::date)
    ) daqi_no2 on true
    left join lateral uk_aq_aggdaily.uk_aq_aqi_index_lookup(
      'daqi',
      'pm25',
      'rolling_24h_mean',
      b.pm25_rolling24h_mean_ugm3,
      coalesce(v_reference_effective_date, (b.timestamp_hour_utc at time zone 'UTC')::date)
    ) daqi_pm25 on true
    left join lateral uk_aq_aggdaily.uk_aq_aqi_index_lookup(
      'daqi',
      'pm10',
      'rolling_24h_mean',
      b.pm10_rolling24h_mean_ugm3,
      coalesce(v_reference_effective_date, (b.timestamp_hour_utc at time zone 'UTC')::date)
    ) daqi_pm10 on true
    left join lateral uk_aq_aggdaily.uk_aq_aqi_index_lookup(
      'eaqi',
      'no2',
      'hourly_mean',
      b.no2_hourly_mean_ugm3,
      coalesce(v_reference_effective_date, (b.timestamp_hour_utc at time zone 'UTC')::date)
    ) eaqi_no2 on true
    left join lateral uk_aq_aggdaily.uk_aq_aqi_index_lookup(
      'eaqi',
      'pm25',
      'hourly_mean',
      b.pm25_hourly_mean_ugm3,
      coalesce(v_reference_effective_date, (b.timestamp_hour_utc at time zone 'UTC')::date)
    ) eaqi_pm25 on true
    left join lateral uk_aq_aggdaily.uk_aq_aqi_index_lookup(
      'eaqi',
      'pm10',
      'hourly_mean',
      b.pm10_hourly_mean_ugm3,
      coalesce(v_reference_effective_date, (b.timestamp_hour_utc at time zone 'UTC')::date)
    ) eaqi_pm10 on true
  ),
  dedup as (
    select distinct on (station_id, timestamp_hour_utc)
      i.*
    from incoming i
    order by i.station_id, i.timestamp_hour_utc
  ),
  changed as (
    select
      d.*
    from dedup d
    left join uk_aq_aggdaily.station_aqi_hourly e
      on e.station_id = d.station_id
     and e.timestamp_hour_utc = d.timestamp_hour_utc
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
          e.pm10_hourly_sample_count,
          e.daqi_no2_index_level,
          e.daqi_pm25_rolling24h_index_level,
          e.daqi_pm10_rolling24h_index_level,
          e.eaqi_no2_index_level,
          e.eaqi_pm25_index_level,
          e.eaqi_pm10_index_level
        )
        is distinct from
        (
          d.no2_hourly_mean_ugm3,
          d.pm25_hourly_mean_ugm3,
          d.pm10_hourly_mean_ugm3,
          d.pm25_rolling24h_mean_ugm3,
          d.pm10_rolling24h_mean_ugm3,
          d.no2_hourly_sample_count,
          d.pm25_hourly_sample_count,
          d.pm10_hourly_sample_count,
          d.daqi_no2_index_level,
          d.daqi_pm25_rolling24h_index_level,
          d.daqi_pm10_rolling24h_index_level,
          d.eaqi_no2_index_level,
          d.eaqi_pm25_index_level,
          d.eaqi_pm10_index_level
        )
      )
  )
  insert into uk_aq_aggdaily.station_aqi_hourly (
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
    daqi_no2_index_level,
    daqi_pm25_rolling24h_index_level,
    daqi_pm10_rolling24h_index_level,
    eaqi_no2_index_level,
    eaqi_pm25_index_level,
    eaqi_pm10_index_level,
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
    c.daqi_no2_index_level,
    c.daqi_pm25_rolling24h_index_level,
    c.daqi_pm10_rolling24h_index_level,
    c.eaqi_no2_index_level,
    c.eaqi_pm25_index_level,
    c.eaqi_pm10_index_level,
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
    daqi_no2_index_level = excluded.daqi_no2_index_level,
    daqi_pm25_rolling24h_index_level = excluded.daqi_pm25_rolling24h_index_level,
    daqi_pm10_rolling24h_index_level = excluded.daqi_pm10_rolling24h_index_level,
    eaqi_no2_index_level = excluded.eaqi_no2_index_level,
    eaqi_pm25_index_level = excluded.eaqi_pm25_index_level,
    eaqi_pm10_index_level = excluded.eaqi_pm10_index_level,
    updated_at = now()
  where
    (
      uk_aq_aggdaily.station_aqi_hourly.no2_hourly_mean_ugm3,
      uk_aq_aggdaily.station_aqi_hourly.pm25_hourly_mean_ugm3,
      uk_aq_aggdaily.station_aqi_hourly.pm10_hourly_mean_ugm3,
      uk_aq_aggdaily.station_aqi_hourly.pm25_rolling24h_mean_ugm3,
      uk_aq_aggdaily.station_aqi_hourly.pm10_rolling24h_mean_ugm3,
      uk_aq_aggdaily.station_aqi_hourly.no2_hourly_sample_count,
      uk_aq_aggdaily.station_aqi_hourly.pm25_hourly_sample_count,
      uk_aq_aggdaily.station_aqi_hourly.pm10_hourly_sample_count,
      uk_aq_aggdaily.station_aqi_hourly.daqi_no2_index_level,
      uk_aq_aggdaily.station_aqi_hourly.daqi_pm25_rolling24h_index_level,
      uk_aq_aggdaily.station_aqi_hourly.daqi_pm10_rolling24h_index_level,
      uk_aq_aggdaily.station_aqi_hourly.eaqi_no2_index_level,
      uk_aq_aggdaily.station_aqi_hourly.eaqi_pm25_index_level,
      uk_aq_aggdaily.station_aqi_hourly.eaqi_pm10_index_level
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
      excluded.pm10_hourly_sample_count,
      excluded.daqi_no2_index_level,
      excluded.daqi_pm25_rolling24h_index_level,
      excluded.daqi_pm10_rolling24h_index_level,
      excluded.eaqi_no2_index_level,
      excluded.eaqi_pm25_index_level,
      excluded.eaqi_pm10_index_level
    );

  return query
  select
    coalesce(v_rows_attempted, 0),
    coalesce(v_rows_changed, 0),
    coalesce(v_rows_inserted, 0),
    coalesce(v_rows_updated, 0),
    coalesce(v_station_hours_changed, 0),
    coalesce(v_station_hours_changed_gt_cutoff, 0),
    v_max_changed_lag_hours;
end;
$$;

alter table if exists uk_aq_ops.aqi_compute_runs
  drop constraint if exists aqi_compute_runs_run_mode_check;

alter table if exists uk_aq_ops.aqi_compute_runs
  add constraint aqi_compute_runs_run_mode_check
  check (run_mode in ('sync_hourly', 'backfill', 'fast', 'reconcile_short', 'reconcile_deep'));

create or replace function uk_aq_public.uk_aq_rpc_aqi_compute_run_log(
  p_run_mode text,
  p_trigger_mode text,
  p_window_start_utc timestamptz,
  p_window_end_utc timestamptz,
  p_source_rows integer,
  p_candidate_station_hours integer,
  p_rows_upserted integer,
  p_rows_changed integer,
  p_station_hours_changed integer,
  p_station_hours_changed_gt_36h integer,
  p_max_changed_lag_hours numeric,
  p_deep_reconcile_effective boolean,
  p_daily_rows_upserted integer,
  p_monthly_rows_upserted integer,
  p_run_status text,
  p_error_message text default null,
  p_duration_ms integer default null
)
returns table (
  run_id uuid
)
language plpgsql
security definer
set search_path = uk_aq_ops, public, pg_catalog
as $$
declare
  v_run_id uuid;
begin
  if auth.role() <> 'service_role' then
    raise exception 'service_role required';
  end if;

  if coalesce(nullif(trim(p_run_mode), ''), '') not in ('sync_hourly', 'backfill', 'fast', 'reconcile_short', 'reconcile_deep') then
    raise exception 'invalid run_mode: %', p_run_mode;
  end if;

  if coalesce(nullif(trim(p_run_status), ''), '') not in ('ok', 'error') then
    raise exception 'invalid run_status: %', p_run_status;
  end if;

  insert into uk_aq_ops.aqi_compute_runs (
    run_mode,
    trigger_mode,
    window_start_utc,
    window_end_utc,
    source_rows,
    candidate_station_hours,
    rows_upserted,
    rows_changed,
    station_hours_changed,
    station_hours_changed_gt_36h,
    max_changed_lag_hours,
    deep_reconcile_effective,
    daily_rows_upserted,
    monthly_rows_upserted,
    run_status,
    error_message,
    duration_ms
  )
  values (
    trim(p_run_mode),
    coalesce(nullif(trim(p_trigger_mode), ''), 'manual'),
    p_window_start_utc,
    p_window_end_utc,
    greatest(0, coalesce(p_source_rows, 0)),
    greatest(0, coalesce(p_candidate_station_hours, 0)),
    greatest(0, coalesce(p_rows_upserted, 0)),
    greatest(0, coalesce(p_rows_changed, 0)),
    greatest(0, coalesce(p_station_hours_changed, 0)),
    greatest(0, coalesce(p_station_hours_changed_gt_36h, 0)),
    p_max_changed_lag_hours,
    p_deep_reconcile_effective,
    greatest(0, coalesce(p_daily_rows_upserted, 0)),
    greatest(0, coalesce(p_monthly_rows_upserted, 0)),
    trim(p_run_status),
    nullif(trim(coalesce(p_error_message, '')), ''),
    case
      when p_duration_ms is null then null
      else greatest(0, p_duration_ms)
    end
  )
  returning id into v_run_id;

  return query select v_run_id;
end;
$$;

create or replace function uk_aq_public.uk_aq_rpc_station_aqi_rollups_refresh(
  p_start_hour_utc timestamptz,
  p_end_hour_utc timestamptz,
  p_station_ids bigint[] default null
)
returns table (
  daily_rows_upserted bigint,
  monthly_rows_upserted bigint
)
language plpgsql
security definer
set search_path = uk_aq_aggdaily, public, pg_catalog
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

  delete from uk_aq_aggdaily.station_aqi_daily d
  where d.observed_day between v_start_day and v_end_day
    and (p_station_ids is null or d.station_id = any(p_station_ids));

  with flat as (
    select
      h.station_id,
      (h.timestamp_hour_utc at time zone 'UTC')::date as observed_day,
      'daqi'::text as standard_code,
      'pm25'::text as pollutant_code,
      h.daqi_pm25_rolling24h_index_level as index_level
    from uk_aq_aggdaily.station_aqi_hourly h
    where h.timestamp_hour_utc >= v_start_day::timestamptz
      and h.timestamp_hour_utc < (v_end_day + 1)::timestamptz
      and (p_station_ids is null or h.station_id = any(p_station_ids))
    union all
    select
      h.station_id,
      (h.timestamp_hour_utc at time zone 'UTC')::date,
      'daqi'::text,
      'pm10'::text,
      h.daqi_pm10_rolling24h_index_level
    from uk_aq_aggdaily.station_aqi_hourly h
    where h.timestamp_hour_utc >= v_start_day::timestamptz
      and h.timestamp_hour_utc < (v_end_day + 1)::timestamptz
      and (p_station_ids is null or h.station_id = any(p_station_ids))
    union all
    select
      h.station_id,
      (h.timestamp_hour_utc at time zone 'UTC')::date,
      'daqi'::text,
      'no2'::text,
      h.daqi_no2_index_level
    from uk_aq_aggdaily.station_aqi_hourly h
    where h.timestamp_hour_utc >= v_start_day::timestamptz
      and h.timestamp_hour_utc < (v_end_day + 1)::timestamptz
      and (p_station_ids is null or h.station_id = any(p_station_ids))
    union all
    select
      h.station_id,
      (h.timestamp_hour_utc at time zone 'UTC')::date,
      'eaqi'::text,
      'pm25'::text,
      h.eaqi_pm25_index_level
    from uk_aq_aggdaily.station_aqi_hourly h
    where h.timestamp_hour_utc >= v_start_day::timestamptz
      and h.timestamp_hour_utc < (v_end_day + 1)::timestamptz
      and (p_station_ids is null or h.station_id = any(p_station_ids))
    union all
    select
      h.station_id,
      (h.timestamp_hour_utc at time zone 'UTC')::date,
      'eaqi'::text,
      'pm10'::text,
      h.eaqi_pm10_index_level
    from uk_aq_aggdaily.station_aqi_hourly h
    where h.timestamp_hour_utc >= v_start_day::timestamptz
      and h.timestamp_hour_utc < (v_end_day + 1)::timestamptz
      and (p_station_ids is null or h.station_id = any(p_station_ids))
    union all
    select
      h.station_id,
      (h.timestamp_hour_utc at time zone 'UTC')::date,
      'eaqi'::text,
      'no2'::text,
      h.eaqi_no2_index_level
    from uk_aq_aggdaily.station_aqi_hourly h
    where h.timestamp_hour_utc >= v_start_day::timestamptz
      and h.timestamp_hour_utc < (v_end_day + 1)::timestamptz
      and (p_station_ids is null or h.station_id = any(p_station_ids))
  ),
  counts as (
    select
      f.station_id,
      f.observed_day,
      f.standard_code,
      f.pollutant_code,
      f.index_level,
      count(*)::integer as cnt
    from flat f
    where f.index_level is not null
    group by
      f.station_id,
      f.observed_day,
      f.standard_code,
      f.pollutant_code,
      f.index_level
  ),
  keys as (
    select distinct
      c.station_id,
      c.observed_day,
      c.standard_code,
      c.pollutant_code
    from counts c
  ),
  assembled as (
    select
      k.station_id,
      k.observed_day,
      k.standard_code,
      k.pollutant_code,
      array_agg(coalesce(c.cnt, 0)::integer order by lvl.level) as index_level_hour_counts,
      coalesce(sum(c.cnt), 0)::smallint as valid_hour_count,
      max(c.index_level)::smallint as max_index_level
    from keys k
    cross join lateral generate_series(
      1,
      case when k.standard_code = 'daqi' then 10 else 6 end
    ) as lvl(level)
    left join counts c
      on c.station_id = k.station_id
     and c.observed_day = k.observed_day
     and c.standard_code = k.standard_code
     and c.pollutant_code = k.pollutant_code
     and c.index_level = lvl.level
    group by
      k.station_id,
      k.observed_day,
      k.standard_code,
      k.pollutant_code
  )
  insert into uk_aq_aggdaily.station_aqi_daily (
    station_id,
    observed_day,
    standard_code,
    pollutant_code,
    index_level_hour_counts,
    valid_hour_count,
    max_index_level,
    updated_at
  )
  select
    a.station_id,
    a.observed_day,
    a.standard_code,
    a.pollutant_code,
    a.index_level_hour_counts,
    a.valid_hour_count,
    a.max_index_level,
    now()
  from assembled a
  on conflict (station_id, observed_day, standard_code, pollutant_code) do update
  set
    index_level_hour_counts = excluded.index_level_hour_counts,
    valid_hour_count = excluded.valid_hour_count,
    max_index_level = excluded.max_index_level,
    updated_at = now();

  get diagnostics v_daily_rows = row_count;

  delete from uk_aq_aggdaily.station_aqi_monthly m
  where m.observed_month between v_start_month and v_end_month
    and (p_station_ids is null or m.station_id = any(p_station_ids));

  with flat as (
    select
      h.station_id,
      date_trunc('month', h.timestamp_hour_utc at time zone 'UTC')::date as observed_month,
      'daqi'::text as standard_code,
      'pm25'::text as pollutant_code,
      h.daqi_pm25_rolling24h_index_level as index_level
    from uk_aq_aggdaily.station_aqi_hourly h
    where h.timestamp_hour_utc >= v_start_month::timestamptz
      and h.timestamp_hour_utc < (v_end_month + interval '1 month')::timestamptz
      and (p_station_ids is null or h.station_id = any(p_station_ids))
    union all
    select
      h.station_id,
      date_trunc('month', h.timestamp_hour_utc at time zone 'UTC')::date,
      'daqi'::text,
      'pm10'::text,
      h.daqi_pm10_rolling24h_index_level
    from uk_aq_aggdaily.station_aqi_hourly h
    where h.timestamp_hour_utc >= v_start_month::timestamptz
      and h.timestamp_hour_utc < (v_end_month + interval '1 month')::timestamptz
      and (p_station_ids is null or h.station_id = any(p_station_ids))
    union all
    select
      h.station_id,
      date_trunc('month', h.timestamp_hour_utc at time zone 'UTC')::date,
      'daqi'::text,
      'no2'::text,
      h.daqi_no2_index_level
    from uk_aq_aggdaily.station_aqi_hourly h
    where h.timestamp_hour_utc >= v_start_month::timestamptz
      and h.timestamp_hour_utc < (v_end_month + interval '1 month')::timestamptz
      and (p_station_ids is null or h.station_id = any(p_station_ids))
    union all
    select
      h.station_id,
      date_trunc('month', h.timestamp_hour_utc at time zone 'UTC')::date,
      'eaqi'::text,
      'pm25'::text,
      h.eaqi_pm25_index_level
    from uk_aq_aggdaily.station_aqi_hourly h
    where h.timestamp_hour_utc >= v_start_month::timestamptz
      and h.timestamp_hour_utc < (v_end_month + interval '1 month')::timestamptz
      and (p_station_ids is null or h.station_id = any(p_station_ids))
    union all
    select
      h.station_id,
      date_trunc('month', h.timestamp_hour_utc at time zone 'UTC')::date,
      'eaqi'::text,
      'pm10'::text,
      h.eaqi_pm10_index_level
    from uk_aq_aggdaily.station_aqi_hourly h
    where h.timestamp_hour_utc >= v_start_month::timestamptz
      and h.timestamp_hour_utc < (v_end_month + interval '1 month')::timestamptz
      and (p_station_ids is null or h.station_id = any(p_station_ids))
    union all
    select
      h.station_id,
      date_trunc('month', h.timestamp_hour_utc at time zone 'UTC')::date,
      'eaqi'::text,
      'no2'::text,
      h.eaqi_no2_index_level
    from uk_aq_aggdaily.station_aqi_hourly h
    where h.timestamp_hour_utc >= v_start_month::timestamptz
      and h.timestamp_hour_utc < (v_end_month + interval '1 month')::timestamptz
      and (p_station_ids is null or h.station_id = any(p_station_ids))
  ),
  counts as (
    select
      f.station_id,
      f.observed_month,
      f.standard_code,
      f.pollutant_code,
      f.index_level,
      count(*)::integer as cnt
    from flat f
    where f.index_level is not null
    group by
      f.station_id,
      f.observed_month,
      f.standard_code,
      f.pollutant_code,
      f.index_level
  ),
  keys as (
    select distinct
      c.station_id,
      c.observed_month,
      c.standard_code,
      c.pollutant_code
    from counts c
  ),
  assembled as (
    select
      k.station_id,
      k.observed_month,
      k.standard_code,
      k.pollutant_code,
      array_agg(coalesce(c.cnt, 0)::integer order by lvl.level) as index_level_hour_counts,
      coalesce(sum(c.cnt), 0)::integer as valid_hour_count,
      max(c.index_level)::smallint as max_index_level
    from keys k
    cross join lateral generate_series(
      1,
      case when k.standard_code = 'daqi' then 10 else 6 end
    ) as lvl(level)
    left join counts c
      on c.station_id = k.station_id
     and c.observed_month = k.observed_month
     and c.standard_code = k.standard_code
     and c.pollutant_code = k.pollutant_code
     and c.index_level = lvl.level
    group by
      k.station_id,
      k.observed_month,
      k.standard_code,
      k.pollutant_code
  )
  insert into uk_aq_aggdaily.station_aqi_monthly (
    station_id,
    observed_month,
    standard_code,
    pollutant_code,
    index_level_hour_counts,
    valid_hour_count,
    max_index_level,
    updated_at
  )
  select
    a.station_id,
    a.observed_month,
    a.standard_code,
    a.pollutant_code,
    a.index_level_hour_counts,
    a.valid_hour_count,
    a.max_index_level,
    now()
  from assembled a
  on conflict (station_id, observed_month, standard_code, pollutant_code) do update
  set
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
