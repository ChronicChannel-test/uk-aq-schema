-- Ingest DB: uk_aq_aggdaily helper schema objects for station AQI precompute/sync.

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

grant usage on schema uk_aq_aggdaily to service_role;
grant all on table uk_aq_aggdaily.aqi_standard_versions to service_role;
grant all on table uk_aq_aggdaily.aqi_breakpoints to service_role;
grant all on table uk_aq_aggdaily.station_aqi_hourly_helper to service_role;

revoke all on function uk_aq_aggdaily.uk_aq_aqi_index_lookup(
  text,
  text,
  text,
  double precision,
  date
) from public;
