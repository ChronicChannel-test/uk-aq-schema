-- Ingest DB station AQI helper tables + compute RPCs + hourly pg_cron tick.

create schema if not exists uk_aq_aggdaily;

create table if not exists uk_aq_aggdaily.aqi_standard_versions (
  standard_code text not null check (standard_code in ('daqi', 'eaqi')),
  version_code text not null,
  source_name text not null,
  source_url text,
  notes text,
  valid_from date not null,
  valid_to date,
  is_active boolean not null default true,
  created_at timestamptz not null default now(),
  primary key (standard_code, version_code)
);

create table if not exists uk_aq_aggdaily.aqi_breakpoints (
  standard_code text not null check (standard_code in ('daqi', 'eaqi')),
  version_code text not null,
  pollutant_code text not null check (pollutant_code in ('pm25', 'pm10', 'no2')),
  averaging_code text not null check (averaging_code in ('hourly_mean', 'rolling_24h_mean')),
  index_level smallint not null check (index_level > 0),
  index_label text,
  index_band text not null,
  color_hex text,
  range_low numeric not null,
  range_high numeric,
  uom text not null default 'ug/m3',
  valid_from date not null,
  valid_to date,
  created_at timestamptz not null default now(),
  primary key (
    standard_code,
    version_code,
    pollutant_code,
    averaging_code,
    index_level
  ),
  foreign key (standard_code, version_code)
    references uk_aq_aggdaily.aqi_standard_versions(standard_code, version_code),
  check (range_high is null or range_high >= range_low)
);

create table if not exists uk_aq_aggdaily.station_aqi_hourly_helper (
  station_id bigint not null references uk_aq_core.stations(id) on delete cascade,
  timestamp_hour_utc timestamptz not null,

  no2_hourly_mean_ugm3 double precision,
  pm25_hourly_mean_ugm3 double precision,
  pm10_hourly_mean_ugm3 double precision,
  pm25_rolling24h_mean_ugm3 double precision,
  pm10_rolling24h_mean_ugm3 double precision,

  no2_hourly_sample_count smallint,
  pm25_hourly_sample_count smallint,
  pm10_hourly_sample_count smallint,

  pm25_rolling24h_valid_hours smallint,
  pm10_rolling24h_valid_hours smallint,

  daqi_no2_index_level smallint,
  daqi_no2_index_band text,
  daqi_pm25_index_level smallint,
  daqi_pm25_index_band text,
  daqi_pm10_index_level smallint,
  daqi_pm10_index_band text,

  eaqi_no2_index_level smallint,
  eaqi_no2_index_band text,
  eaqi_pm25_index_level smallint,
  eaqi_pm25_index_band text,
  eaqi_pm10_index_level smallint,
  eaqi_pm10_index_band text,

  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (station_id, timestamp_hour_utc)
);

create index if not exists station_aqi_hourly_helper_hour_idx
  on uk_aq_aggdaily.station_aqi_hourly_helper (timestamp_hour_utc desc);

insert into uk_aq_aggdaily.aqi_standard_versions (
  standard_code,
  version_code,
  source_name,
  source_url,
  notes,
  valid_from,
  valid_to,
  is_active
)
values
  (
    'daqi',
    'uk_daqi_2013_v1',
    'UK Daily Air Quality Index',
    'https://uk-air.defra.gov.uk',
    'NO2 hourly mean; PM2.5/PM10 rolling 24h mean',
    date '2013-01-01',
    null,
    true
  ),
  (
    'eaqi',
    'eea_eaqi_hourly_v1',
    'EEA European Air Quality Index',
    'https://airindex.eea.europa.eu/AQI/index.html',
    'Hourly thresholds for PM2.5, PM10, NO2',
    date '2020-01-01',
    null,
    true
  )
on conflict (standard_code, version_code) do update
set
  source_name = excluded.source_name,
  source_url = excluded.source_url,
  notes = excluded.notes,
  valid_from = excluded.valid_from,
  valid_to = excluded.valid_to,
  is_active = excluded.is_active;

insert into uk_aq_aggdaily.aqi_breakpoints (
  standard_code,
  version_code,
  pollutant_code,
  averaging_code,
  index_level,
  index_label,
  index_band,
  color_hex,
  range_low,
  range_high,
  uom,
  valid_from,
  valid_to
)
values
  -- DAQI NO2 (hourly mean).
  ('daqi','uk_daqi_2013_v1','no2','hourly_mean',1,'1','low','#79BC6A',0,67,'ug/m3',date '2013-01-01',null),
  ('daqi','uk_daqi_2013_v1','no2','hourly_mean',2,'2','low','#79BC6A',68,134,'ug/m3',date '2013-01-01',null),
  ('daqi','uk_daqi_2013_v1','no2','hourly_mean',3,'3','low','#79BC6A',135,200,'ug/m3',date '2013-01-01',null),
  ('daqi','uk_daqi_2013_v1','no2','hourly_mean',4,'4','moderate','#BBCF4C',201,267,'ug/m3',date '2013-01-01',null),
  ('daqi','uk_daqi_2013_v1','no2','hourly_mean',5,'5','moderate','#BBCF4C',268,334,'ug/m3',date '2013-01-01',null),
  ('daqi','uk_daqi_2013_v1','no2','hourly_mean',6,'6','moderate','#BBCF4C',335,400,'ug/m3',date '2013-01-01',null),
  ('daqi','uk_daqi_2013_v1','no2','hourly_mean',7,'7','high','#EEC20B',401,467,'ug/m3',date '2013-01-01',null),
  ('daqi','uk_daqi_2013_v1','no2','hourly_mean',8,'8','high','#EEC20B',468,534,'ug/m3',date '2013-01-01',null),
  ('daqi','uk_daqi_2013_v1','no2','hourly_mean',9,'9','high','#EEC20B',535,600,'ug/m3',date '2013-01-01',null),
  ('daqi','uk_daqi_2013_v1','no2','hourly_mean',10,'10','very_high','#F29305',601,null,'ug/m3',date '2013-01-01',null),

  -- DAQI PM2.5 (rolling 24h mean).
  ('daqi','uk_daqi_2013_v1','pm25','rolling_24h_mean',1,'1','low','#79BC6A',0,11,'ug/m3',date '2013-01-01',null),
  ('daqi','uk_daqi_2013_v1','pm25','rolling_24h_mean',2,'2','low','#79BC6A',12,23,'ug/m3',date '2013-01-01',null),
  ('daqi','uk_daqi_2013_v1','pm25','rolling_24h_mean',3,'3','low','#79BC6A',24,35,'ug/m3',date '2013-01-01',null),
  ('daqi','uk_daqi_2013_v1','pm25','rolling_24h_mean',4,'4','moderate','#BBCF4C',36,41,'ug/m3',date '2013-01-01',null),
  ('daqi','uk_daqi_2013_v1','pm25','rolling_24h_mean',5,'5','moderate','#BBCF4C',42,47,'ug/m3',date '2013-01-01',null),
  ('daqi','uk_daqi_2013_v1','pm25','rolling_24h_mean',6,'6','moderate','#BBCF4C',48,53,'ug/m3',date '2013-01-01',null),
  ('daqi','uk_daqi_2013_v1','pm25','rolling_24h_mean',7,'7','high','#EEC20B',54,58,'ug/m3',date '2013-01-01',null),
  ('daqi','uk_daqi_2013_v1','pm25','rolling_24h_mean',8,'8','high','#EEC20B',59,64,'ug/m3',date '2013-01-01',null),
  ('daqi','uk_daqi_2013_v1','pm25','rolling_24h_mean',9,'9','high','#EEC20B',65,70,'ug/m3',date '2013-01-01',null),
  ('daqi','uk_daqi_2013_v1','pm25','rolling_24h_mean',10,'10','very_high','#F29305',71,null,'ug/m3',date '2013-01-01',null),

  -- DAQI PM10 (rolling 24h mean).
  ('daqi','uk_daqi_2013_v1','pm10','rolling_24h_mean',1,'1','low','#79BC6A',0,16,'ug/m3',date '2013-01-01',null),
  ('daqi','uk_daqi_2013_v1','pm10','rolling_24h_mean',2,'2','low','#79BC6A',17,33,'ug/m3',date '2013-01-01',null),
  ('daqi','uk_daqi_2013_v1','pm10','rolling_24h_mean',3,'3','low','#79BC6A',34,50,'ug/m3',date '2013-01-01',null),
  ('daqi','uk_daqi_2013_v1','pm10','rolling_24h_mean',4,'4','moderate','#BBCF4C',51,58,'ug/m3',date '2013-01-01',null),
  ('daqi','uk_daqi_2013_v1','pm10','rolling_24h_mean',5,'5','moderate','#BBCF4C',59,66,'ug/m3',date '2013-01-01',null),
  ('daqi','uk_daqi_2013_v1','pm10','rolling_24h_mean',6,'6','moderate','#BBCF4C',67,75,'ug/m3',date '2013-01-01',null),
  ('daqi','uk_daqi_2013_v1','pm10','rolling_24h_mean',7,'7','high','#EEC20B',76,83,'ug/m3',date '2013-01-01',null),
  ('daqi','uk_daqi_2013_v1','pm10','rolling_24h_mean',8,'8','high','#EEC20B',84,91,'ug/m3',date '2013-01-01',null),
  ('daqi','uk_daqi_2013_v1','pm10','rolling_24h_mean',9,'9','high','#EEC20B',92,100,'ug/m3',date '2013-01-01',null),
  ('daqi','uk_daqi_2013_v1','pm10','rolling_24h_mean',10,'10','very_high','#F29305',101,null,'ug/m3',date '2013-01-01',null),

  -- EAQI PM2.5 (hourly mean).
  ('eaqi','eea_eaqi_hourly_v1','pm25','hourly_mean',1,'Good','good','#50F0E6',0,5,'ug/m3',date '2020-01-01',null),
  ('eaqi','eea_eaqi_hourly_v1','pm25','hourly_mean',2,'Fair','fair','#50CCAA',6,15,'ug/m3',date '2020-01-01',null),
  ('eaqi','eea_eaqi_hourly_v1','pm25','hourly_mean',3,'Moderate','moderate','#F0E641',16,50,'ug/m3',date '2020-01-01',null),
  ('eaqi','eea_eaqi_hourly_v1','pm25','hourly_mean',4,'Poor','poor','#FF5050',51,90,'ug/m3',date '2020-01-01',null),
  ('eaqi','eea_eaqi_hourly_v1','pm25','hourly_mean',5,'Very poor','very_poor','#960032',91,140,'ug/m3',date '2020-01-01',null),
  ('eaqi','eea_eaqi_hourly_v1','pm25','hourly_mean',6,'Extremely poor','extremely_poor','#7D2181',141,null,'ug/m3',date '2020-01-01',null),

  -- EAQI PM10 (hourly mean).
  ('eaqi','eea_eaqi_hourly_v1','pm10','hourly_mean',1,'Good','good','#50F0E6',0,15,'ug/m3',date '2020-01-01',null),
  ('eaqi','eea_eaqi_hourly_v1','pm10','hourly_mean',2,'Fair','fair','#50CCAA',16,45,'ug/m3',date '2020-01-01',null),
  ('eaqi','eea_eaqi_hourly_v1','pm10','hourly_mean',3,'Moderate','moderate','#F0E641',46,120,'ug/m3',date '2020-01-01',null),
  ('eaqi','eea_eaqi_hourly_v1','pm10','hourly_mean',4,'Poor','poor','#FF5050',121,195,'ug/m3',date '2020-01-01',null),
  ('eaqi','eea_eaqi_hourly_v1','pm10','hourly_mean',5,'Very poor','very_poor','#960032',196,270,'ug/m3',date '2020-01-01',null),
  ('eaqi','eea_eaqi_hourly_v1','pm10','hourly_mean',6,'Extremely poor','extremely_poor','#7D2181',271,null,'ug/m3',date '2020-01-01',null),

  -- EAQI NO2 (hourly mean).
  ('eaqi','eea_eaqi_hourly_v1','no2','hourly_mean',1,'Good','good','#50F0E6',0,10,'ug/m3',date '2020-01-01',null),
  ('eaqi','eea_eaqi_hourly_v1','no2','hourly_mean',2,'Fair','fair','#50CCAA',11,25,'ug/m3',date '2020-01-01',null),
  ('eaqi','eea_eaqi_hourly_v1','no2','hourly_mean',3,'Moderate','moderate','#F0E641',26,60,'ug/m3',date '2020-01-01',null),
  ('eaqi','eea_eaqi_hourly_v1','no2','hourly_mean',4,'Poor','poor','#FF5050',61,100,'ug/m3',date '2020-01-01',null),
  ('eaqi','eea_eaqi_hourly_v1','no2','hourly_mean',5,'Very poor','very_poor','#960032',101,150,'ug/m3',date '2020-01-01',null),
  ('eaqi','eea_eaqi_hourly_v1','no2','hourly_mean',6,'Extremely poor','extremely_poor','#7D2181',151,null,'ug/m3',date '2020-01-01',null)
on conflict (
  standard_code,
  version_code,
  pollutant_code,
  averaging_code,
  index_level
) do update
set
  index_label = excluded.index_label,
  index_band = excluded.index_band,
  color_hex = excluded.color_hex,
  range_low = excluded.range_low,
  range_high = excluded.range_high,
  uom = excluded.uom,
  valid_from = excluded.valid_from,
  valid_to = excluded.valid_to;

do $$
declare
  t text;
begin
  for t in
    select unnest(ARRAY[
      'aqi_standard_versions',
      'aqi_breakpoints',
      'station_aqi_hourly_helper'
    ]::text[])
  loop
    execute format('alter table uk_aq_aggdaily.%I enable row level security', t);

    if not exists (
      select 1
      from pg_policies p
      where p.schemaname = 'uk_aq_aggdaily'
        and p.tablename = t
        and p.policyname = t || '_select_service_role'
    ) then
      execute format(
        'create policy %I on uk_aq_aggdaily.%I for select using (auth.role() = ''service_role'');',
        t || '_select_service_role',
        t
      );
    end if;

    if not exists (
      select 1
      from pg_policies p
      where p.schemaname = 'uk_aq_aggdaily'
        and p.tablename = t
        and p.policyname = t || '_write_service_role'
    ) then
      execute format(
        'create policy %I on uk_aq_aggdaily.%I for all using (auth.role() = ''service_role'') with check (auth.role() = ''service_role'');',
        t || '_write_service_role',
        t
      );
    end if;
  end loop;
end
$$;

grant usage on schema uk_aq_aggdaily to service_role;
grant all on table uk_aq_aggdaily.aqi_standard_versions to service_role;
grant all on table uk_aq_aggdaily.aqi_breakpoints to service_role;
grant all on table uk_aq_aggdaily.station_aqi_hourly_helper to service_role;

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
set search_path = uk_aq_aggdaily, uk_aq_core, uk_aq_public, public, pg_catalog
as $$
declare
  v_start_exclusive timestamptz;
  v_end_inclusive timestamptz;
  v_source_start timestamptz;
  v_source_end timestamptz;
  v_reference_end timestamptz;
  v_effective_date date;
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
  v_effective_date := (v_reference_end at time zone 'UTC')::date;

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
      end as pm10_rolling24h_mean_ugm3,
      wr.pm25_rolling24h_valid_hours_raw,
      wr.pm10_rolling24h_valid_hours_raw
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
      end as pm10_hourly_sample_count,
      least(24, greatest(0, t.pm25_rolling24h_valid_hours_raw))::smallint as pm25_rolling24h_valid_hours,
      least(24, greatest(0, t.pm10_rolling24h_valid_hours_raw))::smallint as pm10_rolling24h_valid_hours,
      daqi_no2.index_level as daqi_no2_index_level,
      daqi_no2.index_band as daqi_no2_index_band,
      daqi_pm25.index_level as daqi_pm25_index_level,
      daqi_pm25.index_band as daqi_pm25_index_band,
      daqi_pm10.index_level as daqi_pm10_index_level,
      daqi_pm10.index_band as daqi_pm10_index_band,
      eaqi_no2.index_level as eaqi_no2_index_level,
      eaqi_no2.index_band as eaqi_no2_index_band,
      eaqi_pm25.index_level as eaqi_pm25_index_level,
      eaqi_pm25.index_band as eaqi_pm25_index_band,
      eaqi_pm10.index_level as eaqi_pm10_index_level,
      eaqi_pm10.index_band as eaqi_pm10_index_band
    from target_hours t
    left join lateral uk_aq_aggdaily.uk_aq_aqi_index_lookup(
      'daqi',
      'no2',
      'hourly_mean',
      t.no2_hourly_mean_ugm3,
      v_effective_date
    ) daqi_no2 on true
    left join lateral uk_aq_aggdaily.uk_aq_aqi_index_lookup(
      'daqi',
      'pm25',
      'rolling_24h_mean',
      t.pm25_rolling24h_mean_ugm3,
      v_effective_date
    ) daqi_pm25 on true
    left join lateral uk_aq_aggdaily.uk_aq_aqi_index_lookup(
      'daqi',
      'pm10',
      'rolling_24h_mean',
      t.pm10_rolling24h_mean_ugm3,
      v_effective_date
    ) daqi_pm10 on true
    left join lateral uk_aq_aggdaily.uk_aq_aqi_index_lookup(
      'eaqi',
      'no2',
      'hourly_mean',
      t.no2_hourly_mean_ugm3,
      v_effective_date
    ) eaqi_no2 on true
    left join lateral uk_aq_aggdaily.uk_aq_aqi_index_lookup(
      'eaqi',
      'pm25',
      'hourly_mean',
      t.pm25_hourly_mean_ugm3,
      v_effective_date
    ) eaqi_pm25 on true
    left join lateral uk_aq_aggdaily.uk_aq_aqi_index_lookup(
      'eaqi',
      'pm10',
      'hourly_mean',
      t.pm10_hourly_mean_ugm3,
      v_effective_date
    ) eaqi_pm10 on true
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
    left join uk_aq_aggdaily.station_aqi_hourly_helper e
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
          e.pm10_hourly_sample_count,
          e.pm25_rolling24h_valid_hours,
          e.pm10_rolling24h_valid_hours,
          e.daqi_no2_index_level,
          e.daqi_no2_index_band,
          e.daqi_pm25_index_level,
          e.daqi_pm25_index_band,
          e.daqi_pm10_index_level,
          e.daqi_pm10_index_band,
          e.eaqi_no2_index_level,
          e.eaqi_no2_index_band,
          e.eaqi_pm25_index_level,
          e.eaqi_pm25_index_band,
          e.eaqi_pm10_index_level,
          e.eaqi_pm10_index_band
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
          c.pm10_hourly_sample_count,
          c.pm25_rolling24h_valid_hours,
          c.pm10_rolling24h_valid_hours,
          c.daqi_no2_index_level,
          c.daqi_no2_index_band,
          c.daqi_pm25_index_level,
          c.daqi_pm25_index_band,
          c.daqi_pm10_index_level,
          c.daqi_pm10_index_band,
          c.eaqi_no2_index_level,
          c.eaqi_no2_index_band,
          c.eaqi_pm25_index_level,
          c.eaqi_pm25_index_band,
          c.eaqi_pm10_index_level,
          c.eaqi_pm10_index_band
        )
      )
  ),
  upserted as (
    insert into uk_aq_aggdaily.station_aqi_hourly_helper (
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
      pm25_rolling24h_valid_hours,
      pm10_rolling24h_valid_hours,
      daqi_no2_index_level,
      daqi_no2_index_band,
      daqi_pm25_index_level,
      daqi_pm25_index_band,
      daqi_pm10_index_level,
      daqi_pm10_index_band,
      eaqi_no2_index_level,
      eaqi_no2_index_band,
      eaqi_pm25_index_level,
      eaqi_pm25_index_band,
      eaqi_pm10_index_level,
      eaqi_pm10_index_band,
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
      c.pm25_rolling24h_valid_hours,
      c.pm10_rolling24h_valid_hours,
      c.daqi_no2_index_level,
      c.daqi_no2_index_band,
      c.daqi_pm25_index_level,
      c.daqi_pm25_index_band,
      c.daqi_pm10_index_level,
      c.daqi_pm10_index_band,
      c.eaqi_no2_index_level,
      c.eaqi_no2_index_band,
      c.eaqi_pm25_index_level,
      c.eaqi_pm25_index_band,
      c.eaqi_pm10_index_level,
      c.eaqi_pm10_index_band,
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
      pm25_rolling24h_valid_hours = excluded.pm25_rolling24h_valid_hours,
      pm10_rolling24h_valid_hours = excluded.pm10_rolling24h_valid_hours,
      daqi_no2_index_level = excluded.daqi_no2_index_level,
      daqi_no2_index_band = excluded.daqi_no2_index_band,
      daqi_pm25_index_level = excluded.daqi_pm25_index_level,
      daqi_pm25_index_band = excluded.daqi_pm25_index_band,
      daqi_pm10_index_level = excluded.daqi_pm10_index_level,
      daqi_pm10_index_band = excluded.daqi_pm10_index_band,
      eaqi_no2_index_level = excluded.eaqi_no2_index_level,
      eaqi_no2_index_band = excluded.eaqi_no2_index_band,
      eaqi_pm25_index_level = excluded.eaqi_pm25_index_level,
      eaqi_pm25_index_band = excluded.eaqi_pm25_index_band,
      eaqi_pm10_index_level = excluded.eaqi_pm10_index_level,
      eaqi_pm10_index_band = excluded.eaqi_pm10_index_band,
      updated_at = now()
    where
      (
        uk_aq_aggdaily.station_aqi_hourly_helper.no2_hourly_mean_ugm3,
        uk_aq_aggdaily.station_aqi_hourly_helper.pm25_hourly_mean_ugm3,
        uk_aq_aggdaily.station_aqi_hourly_helper.pm10_hourly_mean_ugm3,
        uk_aq_aggdaily.station_aqi_hourly_helper.pm25_rolling24h_mean_ugm3,
        uk_aq_aggdaily.station_aqi_hourly_helper.pm10_rolling24h_mean_ugm3,
        uk_aq_aggdaily.station_aqi_hourly_helper.no2_hourly_sample_count,
        uk_aq_aggdaily.station_aqi_hourly_helper.pm25_hourly_sample_count,
        uk_aq_aggdaily.station_aqi_hourly_helper.pm10_hourly_sample_count,
        uk_aq_aggdaily.station_aqi_hourly_helper.pm25_rolling24h_valid_hours,
        uk_aq_aggdaily.station_aqi_hourly_helper.pm10_rolling24h_valid_hours,
        uk_aq_aggdaily.station_aqi_hourly_helper.daqi_no2_index_level,
        uk_aq_aggdaily.station_aqi_hourly_helper.daqi_no2_index_band,
        uk_aq_aggdaily.station_aqi_hourly_helper.daqi_pm25_index_level,
        uk_aq_aggdaily.station_aqi_hourly_helper.daqi_pm25_index_band,
        uk_aq_aggdaily.station_aqi_hourly_helper.daqi_pm10_index_level,
        uk_aq_aggdaily.station_aqi_hourly_helper.daqi_pm10_index_band,
        uk_aq_aggdaily.station_aqi_hourly_helper.eaqi_no2_index_level,
        uk_aq_aggdaily.station_aqi_hourly_helper.eaqi_no2_index_band,
        uk_aq_aggdaily.station_aqi_hourly_helper.eaqi_pm25_index_level,
        uk_aq_aggdaily.station_aqi_hourly_helper.eaqi_pm25_index_band,
        uk_aq_aggdaily.station_aqi_hourly_helper.eaqi_pm10_index_level,
        uk_aq_aggdaily.station_aqi_hourly_helper.eaqi_pm10_index_band
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
        excluded.pm25_rolling24h_valid_hours,
        excluded.pm10_rolling24h_valid_hours,
        excluded.daqi_no2_index_level,
        excluded.daqi_no2_index_band,
        excluded.daqi_pm25_index_level,
        excluded.daqi_pm25_index_band,
        excluded.daqi_pm10_index_level,
        excluded.daqi_pm10_index_band,
        excluded.eaqi_no2_index_level,
        excluded.eaqi_no2_index_band,
        excluded.eaqi_pm25_index_level,
        excluded.eaqi_pm25_index_band,
        excluded.eaqi_pm10_index_level,
        excluded.eaqi_pm10_index_band
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
  pm10_hourly_sample_count smallint,
  pm25_rolling24h_valid_hours smallint,
  pm10_rolling24h_valid_hours smallint,
  daqi_no2_index_level smallint,
  daqi_no2_index_band text,
  daqi_pm25_index_level smallint,
  daqi_pm25_index_band text,
  daqi_pm10_index_level smallint,
  daqi_pm10_index_band text,
  eaqi_no2_index_level smallint,
  eaqi_no2_index_band text,
  eaqi_pm25_index_level smallint,
  eaqi_pm25_index_band text,
  eaqi_pm10_index_level smallint,
  eaqi_pm10_index_band text
)
language plpgsql
security definer
set search_path = uk_aq_aggdaily, public, pg_catalog
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
    h.pm10_hourly_sample_count,
    h.pm25_rolling24h_valid_hours,
    h.pm10_rolling24h_valid_hours,
    h.daqi_no2_index_level,
    h.daqi_no2_index_band,
    h.daqi_pm25_index_level,
    h.daqi_pm25_index_band,
    h.daqi_pm10_index_level,
    h.daqi_pm10_index_band,
    h.eaqi_no2_index_level,
    h.eaqi_no2_index_band,
    h.eaqi_pm25_index_level,
    h.eaqi_pm25_index_band,
    h.eaqi_pm10_index_level,
    h.eaqi_pm10_index_band
  from uk_aq_aggdaily.station_aqi_hourly_helper h
  where h.timestamp_hour_utc > (v_start_exclusive - interval '1 hour')
    and h.timestamp_hour_utc <= (v_end_inclusive - interval '1 hour')
    and (p_station_ids is null or h.station_id = any(p_station_ids))
  order by
    h.timestamp_hour_utc,
    h.station_id;
end;
$$;

drop function if exists uk_aq_ops.uk_aq_station_aqi_hourly_ingest_tick(
  timestamptz,
  bigint[],
  integer
);

drop function if exists uk_aq_public.uk_aq_rpc_station_aqi_hourly_helper_cleanup(integer);

create or replace function uk_aq_public.uk_aq_rpc_station_aqi_hourly_helper_cleanup(
  p_retention_days integer default 45
)
returns table (
  rows_deleted bigint
)
language plpgsql
security definer
set search_path = uk_aq_aggdaily, public, pg_catalog
as $$
declare
  v_days integer;
  v_rows bigint := 0;
begin
  if auth.role() is not null and auth.role() <> 'service_role' then
    raise exception 'service_role required';
  end if;

  v_days := greatest(1, least(coalesce(p_retention_days, 45), 3650));

  delete from uk_aq_aggdaily.station_aqi_hourly_helper
  where timestamp_hour_utc < date_trunc('hour', now()) - make_interval(days => v_days);

  get diagnostics v_rows = row_count;

  return query select coalesce(v_rows, 0);
end;
$$;

drop function if exists uk_aq_ops.uk_aq_station_aqi_hourly_ingest_tick(
  timestamptz,
  bigint[]
);

create or replace function uk_aq_ops.uk_aq_station_aqi_hourly_ingest_tick(
  p_now_utc timestamptz default now(),
  p_station_ids bigint[] default null,
  p_helper_retention_days integer default 45
)
returns table (
  target_hour_end_utc timestamptz,
  source_rows integer,
  rows_upserted integer,
  station_hours_changed integer,
  max_changed_lag_hours numeric,
  helper_rows_deleted bigint
)
language plpgsql
security definer
set search_path = uk_aq_ops, uk_aq_public, public, pg_catalog
as $$
declare
  v_target_hour_end_utc timestamptz;
  v_source_rows integer := 0;
  v_rows_upserted integer := 0;
  v_station_hours_changed integer := 0;
  v_max_changed_lag_hours numeric := null;
  v_helper_rows_deleted bigint := 0;
begin
  v_target_hour_end_utc := date_trunc(
    'hour',
    coalesce(p_now_utc, now()) - interval '3 hours 10 minutes'
  );

  select
    r.source_rows,
    r.rows_upserted,
    r.station_hours_changed,
    r.max_changed_lag_hours
  into
    v_source_rows,
    v_rows_upserted,
    v_station_hours_changed,
    v_max_changed_lag_hours
  from uk_aq_public.uk_aq_rpc_station_aqi_hourly_helper_upsert(
    v_target_hour_end_utc - interval '1 hour',
    v_target_hour_end_utc,
    p_station_ids,
    v_target_hour_end_utc
  ) r;

  select
    c.rows_deleted
  into v_helper_rows_deleted
  from uk_aq_public.uk_aq_rpc_station_aqi_hourly_helper_cleanup(
    p_helper_retention_days
  ) c;

  return query
  select
    v_target_hour_end_utc,
    coalesce(v_source_rows, 0),
    coalesce(v_rows_upserted, 0),
    coalesce(v_station_hours_changed, 0),
    v_max_changed_lag_hours,
    coalesce(v_helper_rows_deleted, 0);
end;
$$;

create extension if not exists pg_cron with schema extensions;

select cron.unschedule(jobid)
from cron.job
where jobname = 'uk_aq_ingest_station_aqi_hourly_helper_tick';

select cron.schedule(
  'uk_aq_ingest_station_aqi_hourly_helper_tick',
  '10 * * * *',
  $$select * from uk_aq_ops.uk_aq_station_aqi_hourly_ingest_tick();$$
);

revoke all on function uk_aq_aggdaily.uk_aq_aqi_index_lookup(
  text,
  text,
  text,
  double precision,
  date
) from public;

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

revoke all on function uk_aq_ops.uk_aq_station_aqi_hourly_ingest_tick(
  timestamptz,
  bigint[],
  integer
) from public;

revoke all on function uk_aq_public.uk_aq_rpc_station_aqi_hourly_helper_cleanup(integer) from public;

revoke all on function uk_aq_public.uk_aq_rpc_station_aqi_hourly_helper_cleanup(integer) from anon, authenticated;

grant execute on function uk_aq_public.uk_aq_rpc_station_aqi_hourly_helper_cleanup(integer) to service_role;

grant execute on function uk_aq_ops.uk_aq_station_aqi_hourly_ingest_tick(
  timestamptz,
  bigint[],
  integer
) to service_role;
