-- Station AQI schema + RPCs (aggdaily DB).
-- AQI reference/fact tables (stations first).

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

create table if not exists uk_aq_aggdaily.station_aqi_hourly (
  station_id bigint not null references uk_aq_core.stations(id) on delete cascade,
  timestamp_hour_utc timestamptz not null,

  no2_hourly_mean_ugm3 double precision,
  pm25_hourly_mean_ugm3 double precision,
  pm10_hourly_mean_ugm3 double precision,
  pm25_rolling24h_mean_ugm3 double precision,
  pm10_rolling24h_mean_ugm3 double precision,

  no2_hourly_capture_ratio real,
  pm25_hourly_capture_ratio real,
  pm10_hourly_capture_ratio real,

  no2_hourly_sample_count smallint,
  pm25_hourly_sample_count smallint,
  pm10_hourly_sample_count smallint,
  no2_hourly_expected_count smallint,
  pm25_hourly_expected_count smallint,
  pm10_hourly_expected_count smallint,

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

create index if not exists station_aqi_hourly_hour_idx
  on uk_aq_aggdaily.station_aqi_hourly (timestamp_hour_utc desc);

create table if not exists uk_aq_aggdaily.station_aqi_daily (
  station_id bigint not null references uk_aq_core.stations(id) on delete cascade,
  observed_day date not null,
  standard_code text not null check (standard_code in ('daqi', 'eaqi')),
  pollutant_code text not null check (pollutant_code in ('pm25', 'pm10', 'no2')),
  index_level_hour_counts integer[] not null,
  valid_hour_count smallint not null check (valid_hour_count >= 0),
  max_index_level smallint,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (station_id, observed_day, standard_code, pollutant_code),
  check (
    (
      standard_code = 'daqi'
      and array_length(index_level_hour_counts, 1) = 10
    ) or (
      standard_code = 'eaqi'
      and array_length(index_level_hour_counts, 1) = 6
    )
  )
);

create index if not exists station_aqi_daily_day_idx
  on uk_aq_aggdaily.station_aqi_daily (observed_day desc);

create table if not exists uk_aq_aggdaily.station_aqi_monthly (
  station_id bigint not null references uk_aq_core.stations(id) on delete cascade,
  observed_month date not null,
  standard_code text not null check (standard_code in ('daqi', 'eaqi')),
  pollutant_code text not null check (pollutant_code in ('pm25', 'pm10', 'no2')),
  index_level_hour_counts integer[] not null,
  valid_hour_count integer not null check (valid_hour_count >= 0),
  max_index_level smallint,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (station_id, observed_month, standard_code, pollutant_code),
  check (
    (
      standard_code = 'daqi'
      and array_length(index_level_hour_counts, 1) = 10
    ) or (
      standard_code = 'eaqi'
      and array_length(index_level_hour_counts, 1) = 6
    )
  )
);

create index if not exists station_aqi_monthly_month_idx
  on uk_aq_aggdaily.station_aqi_monthly (observed_month desc);

create table if not exists uk_aq_ops.aqi_compute_runs (
  id uuid primary key default gen_random_uuid(),
  started_at timestamptz not null default now(),
  run_mode text not null check (run_mode in ('fast', 'reconcile_short', 'reconcile_deep', 'backfill')),
  trigger_mode text not null default 'manual',
  window_start_utc timestamptz,
  window_end_utc timestamptz,
  source_rows integer not null default 0,
  candidate_station_hours integer not null default 0,
  rows_upserted integer not null default 0,
  rows_changed integer not null default 0,
  station_hours_changed integer not null default 0,
  station_hours_changed_gt_36h integer not null default 0,
  max_changed_lag_hours numeric,
  deep_reconcile_effective boolean,
  daily_rows_upserted integer not null default 0,
  monthly_rows_upserted integer not null default 0,
  run_status text not null check (run_status in ('ok', 'error')),
  error_message text,
  duration_ms integer,
  created_at timestamptz not null default now()
);

create index if not exists aqi_compute_runs_started_idx
  on uk_aq_ops.aqi_compute_runs (started_at desc);

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
from uk_aq_aggdaily.station_aqi_hourly;
alter view if exists uk_aq_public.uk_aq_station_aqi_hourly set (security_invoker = true);

create or replace view uk_aq_public.uk_aq_station_aqi_daily as
select
  station_id,
  observed_day,
  standard_code,
  pollutant_code,
  index_level_hour_counts,
  valid_hour_count,
  max_index_level,
  updated_at
from uk_aq_aggdaily.station_aqi_daily;
alter view if exists uk_aq_public.uk_aq_station_aqi_daily set (security_invoker = true);

create or replace view uk_aq_public.uk_aq_station_aqi_monthly as
select
  station_id,
  observed_month,
  standard_code,
  pollutant_code,
  index_level_hour_counts,
  valid_hour_count,
  max_index_level,
  updated_at
from uk_aq_aggdaily.station_aqi_monthly;
alter view if exists uk_aq_public.uk_aq_station_aqi_monthly set (security_invoker = true);

-- RLS / policies for AQI tables.
do $$
declare
  t text;
begin
  for t in
    select unnest(ARRAY[
      'aqi_standard_versions',
      'aqi_breakpoints',
      'station_aqi_hourly',
      'station_aqi_daily',
      'station_aqi_monthly'
    ]::text[])
  loop
    execute format('alter table uk_aq_aggdaily.%I enable row level security', t);
    if not exists (
      select 1
      from pg_policies p
      where p.schemaname = 'uk_aq_aggdaily'
        and p.tablename = t
        and p.policyname = t || '_select_authenticated'
    ) then
      execute format(
        'create policy %I on uk_aq_aggdaily.%I for select using (auth.role() in (''authenticated'',''service_role''));',
        t || '_select_authenticated',
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

alter table uk_aq_ops.aqi_compute_runs enable row level security;

do $$
begin
  if not exists (
    select 1
    from pg_policies
    where schemaname = 'uk_aq_ops'
      and tablename = 'aqi_compute_runs'
      and policyname = 'aqi_compute_runs_service_role'
  ) then
    create policy aqi_compute_runs_service_role
      on uk_aq_ops.aqi_compute_runs
      for all
      using (auth.role() = 'service_role')
      with check (auth.role() = 'service_role');
  end if;
end
$$;

grant all on table uk_aq_aggdaily.aqi_standard_versions to service_role;
grant all on table uk_aq_aggdaily.aqi_breakpoints to service_role;
grant all on table uk_aq_aggdaily.station_aqi_hourly to service_role;
grant all on table uk_aq_aggdaily.station_aqi_daily to service_role;
grant all on table uk_aq_aggdaily.station_aqi_monthly to service_role;
grant all on table uk_aq_ops.aqi_compute_runs to service_role;

revoke all on uk_aq_public.uk_aq_station_aqi_hourly from public;
grant select on uk_aq_public.uk_aq_station_aqi_hourly to authenticated;
grant select on uk_aq_public.uk_aq_station_aqi_hourly to service_role;

revoke all on uk_aq_public.uk_aq_station_aqi_daily from public;
grant select on uk_aq_public.uk_aq_station_aqi_daily to authenticated;
grant select on uk_aq_public.uk_aq_station_aqi_daily to service_role;

revoke all on uk_aq_public.uk_aq_station_aqi_monthly from public;
grant select on uk_aq_public.uk_aq_station_aqi_monthly to authenticated;
grant select on uk_aq_public.uk_aq_station_aqi_monthly to service_role;

-- AQI RPCs (service_role).

drop function if exists uk_aq_public.uk_aq_rpc_aqi_breakpoints_active(
  timestamptz,
  text,
  text
);

create or replace function uk_aq_public.uk_aq_rpc_aqi_breakpoints_active(
  p_effective_at timestamptz default now(),
  p_standard_code text default null,
  p_version_code text default null
)
returns table (
  standard_code text,
  version_code text,
  pollutant_code text,
  averaging_code text,
  index_level smallint,
  index_label text,
  index_band text,
  color_hex text,
  range_low numeric,
  range_high numeric,
  uom text
)
language plpgsql
security definer
set search_path = uk_aq_aggdaily, public, pg_catalog
as $$
declare
  v_effective_date date;
begin
  if auth.role() <> 'service_role' then
    raise exception 'service_role required';
  end if;

  v_effective_date := (coalesce(p_effective_at, now()) at time zone 'UTC')::date;

  return query
  select
    b.standard_code,
    b.version_code,
    b.pollutant_code,
    b.averaging_code,
    b.index_level,
    b.index_label,
    b.index_band,
    b.color_hex,
    b.range_low,
    b.range_high,
    b.uom
  from uk_aq_aggdaily.aqi_breakpoints b
  join uk_aq_aggdaily.aqi_standard_versions v
    on v.standard_code = b.standard_code
   and v.version_code = b.version_code
  where (p_standard_code is null or b.standard_code = p_standard_code)
    and (p_version_code is null or b.version_code = p_version_code)
    and (v.valid_from is null or v.valid_from <= v_effective_date)
    and (v.valid_to is null or v.valid_to >= v_effective_date)
    and (b.valid_from is null or b.valid_from <= v_effective_date)
    and (b.valid_to is null or b.valid_to >= v_effective_date)
  order by
    b.standard_code,
    b.pollutant_code,
    b.averaging_code,
    b.index_level;
end;
$$;

drop function if exists uk_aq_public.uk_aq_rpc_station_aqi_hourly_upsert(
  jsonb,
  timestamptz,
  timestamptz
);

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

  with incoming as (
    select
      r.station_id,
      date_trunc('hour', r.timestamp_hour_utc) as timestamp_hour_utc,
      r.no2_hourly_mean_ugm3,
      r.pm25_hourly_mean_ugm3,
      r.pm10_hourly_mean_ugm3,
      r.pm25_rolling24h_mean_ugm3,
      r.pm10_rolling24h_mean_ugm3,
      r.no2_hourly_capture_ratio,
      r.pm25_hourly_capture_ratio,
      r.pm10_hourly_capture_ratio,
      r.no2_hourly_sample_count,
      r.pm25_hourly_sample_count,
      r.pm10_hourly_sample_count,
      r.no2_hourly_expected_count,
      r.pm25_hourly_expected_count,
      r.pm10_hourly_expected_count,
      r.pm25_rolling24h_valid_hours,
      r.pm10_rolling24h_valid_hours,
      r.daqi_no2_index_level,
      r.daqi_no2_index_band,
      r.daqi_pm25_index_level,
      r.daqi_pm25_index_band,
      r.daqi_pm10_index_level,
      r.daqi_pm10_index_band,
      r.eaqi_no2_index_level,
      r.eaqi_no2_index_band,
      r.eaqi_pm25_index_level,
      r.eaqi_pm25_index_band,
      r.eaqi_pm10_index_level,
      r.eaqi_pm10_index_band
    from jsonb_to_recordset(p_rows) as r(
      station_id bigint,
      timestamp_hour_utc timestamptz,
      no2_hourly_mean_ugm3 double precision,
      pm25_hourly_mean_ugm3 double precision,
      pm10_hourly_mean_ugm3 double precision,
      pm25_rolling24h_mean_ugm3 double precision,
      pm10_rolling24h_mean_ugm3 double precision,
      no2_hourly_capture_ratio real,
      pm25_hourly_capture_ratio real,
      pm10_hourly_capture_ratio real,
      no2_hourly_sample_count smallint,
      pm25_hourly_sample_count smallint,
      pm10_hourly_sample_count smallint,
      no2_hourly_expected_count smallint,
      pm25_hourly_expected_count smallint,
      pm10_hourly_expected_count smallint,
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
    where r.station_id is not null
      and r.timestamp_hour_utc is not null
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
            e.no2_hourly_capture_ratio,
            e.pm25_hourly_capture_ratio,
            e.pm10_hourly_capture_ratio,
            e.no2_hourly_sample_count,
            e.pm25_hourly_sample_count,
            e.pm10_hourly_sample_count,
            e.no2_hourly_expected_count,
            e.pm25_hourly_expected_count,
            e.pm10_hourly_expected_count,
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
            d.no2_hourly_mean_ugm3,
            d.pm25_hourly_mean_ugm3,
            d.pm10_hourly_mean_ugm3,
            d.pm25_rolling24h_mean_ugm3,
            d.pm10_rolling24h_mean_ugm3,
            d.no2_hourly_capture_ratio,
            d.pm25_hourly_capture_ratio,
            d.pm10_hourly_capture_ratio,
            d.no2_hourly_sample_count,
            d.pm25_hourly_sample_count,
            d.pm10_hourly_sample_count,
            d.no2_hourly_expected_count,
            d.pm25_hourly_expected_count,
            d.pm10_hourly_expected_count,
            d.pm25_rolling24h_valid_hours,
            d.pm10_rolling24h_valid_hours,
            d.daqi_no2_index_level,
            d.daqi_no2_index_band,
            d.daqi_pm25_index_level,
            d.daqi_pm25_index_band,
            d.daqi_pm10_index_level,
            d.daqi_pm10_index_band,
            d.eaqi_no2_index_level,
            d.eaqi_no2_index_band,
            d.eaqi_pm25_index_level,
            d.eaqi_pm25_index_band,
            d.eaqi_pm10_index_level,
            d.eaqi_pm10_index_band
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

  with incoming as (
    select
      r.station_id,
      date_trunc('hour', r.timestamp_hour_utc) as timestamp_hour_utc,
      r.no2_hourly_mean_ugm3,
      r.pm25_hourly_mean_ugm3,
      r.pm10_hourly_mean_ugm3,
      r.pm25_rolling24h_mean_ugm3,
      r.pm10_rolling24h_mean_ugm3,
      r.no2_hourly_capture_ratio,
      r.pm25_hourly_capture_ratio,
      r.pm10_hourly_capture_ratio,
      r.no2_hourly_sample_count,
      r.pm25_hourly_sample_count,
      r.pm10_hourly_sample_count,
      r.no2_hourly_expected_count,
      r.pm25_hourly_expected_count,
      r.pm10_hourly_expected_count,
      r.pm25_rolling24h_valid_hours,
      r.pm10_rolling24h_valid_hours,
      r.daqi_no2_index_level,
      r.daqi_no2_index_band,
      r.daqi_pm25_index_level,
      r.daqi_pm25_index_band,
      r.daqi_pm10_index_level,
      r.daqi_pm10_index_band,
      r.eaqi_no2_index_level,
      r.eaqi_no2_index_band,
      r.eaqi_pm25_index_level,
      r.eaqi_pm25_index_band,
      r.eaqi_pm10_index_level,
      r.eaqi_pm10_index_band
    from jsonb_to_recordset(p_rows) as r(
      station_id bigint,
      timestamp_hour_utc timestamptz,
      no2_hourly_mean_ugm3 double precision,
      pm25_hourly_mean_ugm3 double precision,
      pm10_hourly_mean_ugm3 double precision,
      pm25_rolling24h_mean_ugm3 double precision,
      pm10_rolling24h_mean_ugm3 double precision,
      no2_hourly_capture_ratio real,
      pm25_hourly_capture_ratio real,
      pm10_hourly_capture_ratio real,
      no2_hourly_sample_count smallint,
      pm25_hourly_sample_count smallint,
      pm10_hourly_sample_count smallint,
      no2_hourly_expected_count smallint,
      pm25_hourly_expected_count smallint,
      pm10_hourly_expected_count smallint,
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
    where r.station_id is not null
      and r.timestamp_hour_utc is not null
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
          e.no2_hourly_capture_ratio,
          e.pm25_hourly_capture_ratio,
          e.pm10_hourly_capture_ratio,
          e.no2_hourly_sample_count,
          e.pm25_hourly_sample_count,
          e.pm10_hourly_sample_count,
          e.no2_hourly_expected_count,
          e.pm25_hourly_expected_count,
          e.pm10_hourly_expected_count,
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
          d.no2_hourly_mean_ugm3,
          d.pm25_hourly_mean_ugm3,
          d.pm10_hourly_mean_ugm3,
          d.pm25_rolling24h_mean_ugm3,
          d.pm10_rolling24h_mean_ugm3,
          d.no2_hourly_capture_ratio,
          d.pm25_hourly_capture_ratio,
          d.pm10_hourly_capture_ratio,
          d.no2_hourly_sample_count,
          d.pm25_hourly_sample_count,
          d.pm10_hourly_sample_count,
          d.no2_hourly_expected_count,
          d.pm25_hourly_expected_count,
          d.pm10_hourly_expected_count,
          d.pm25_rolling24h_valid_hours,
          d.pm10_rolling24h_valid_hours,
          d.daqi_no2_index_level,
          d.daqi_no2_index_band,
          d.daqi_pm25_index_level,
          d.daqi_pm25_index_band,
          d.daqi_pm10_index_level,
          d.daqi_pm10_index_band,
          d.eaqi_no2_index_level,
          d.eaqi_no2_index_band,
          d.eaqi_pm25_index_level,
          d.eaqi_pm25_index_band,
          d.eaqi_pm10_index_level,
          d.eaqi_pm10_index_band
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
    no2_hourly_capture_ratio,
    pm25_hourly_capture_ratio,
    pm10_hourly_capture_ratio,
    no2_hourly_sample_count,
    pm25_hourly_sample_count,
    pm10_hourly_sample_count,
    no2_hourly_expected_count,
    pm25_hourly_expected_count,
    pm10_hourly_expected_count,
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
    c.no2_hourly_capture_ratio,
    c.pm25_hourly_capture_ratio,
    c.pm10_hourly_capture_ratio,
    c.no2_hourly_sample_count,
    c.pm25_hourly_sample_count,
    c.pm10_hourly_sample_count,
    c.no2_hourly_expected_count,
    c.pm25_hourly_expected_count,
    c.pm10_hourly_expected_count,
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
    no2_hourly_capture_ratio = excluded.no2_hourly_capture_ratio,
    pm25_hourly_capture_ratio = excluded.pm25_hourly_capture_ratio,
    pm10_hourly_capture_ratio = excluded.pm10_hourly_capture_ratio,
    no2_hourly_sample_count = excluded.no2_hourly_sample_count,
    pm25_hourly_sample_count = excluded.pm25_hourly_sample_count,
    pm10_hourly_sample_count = excluded.pm10_hourly_sample_count,
    no2_hourly_expected_count = excluded.no2_hourly_expected_count,
    pm25_hourly_expected_count = excluded.pm25_hourly_expected_count,
    pm10_hourly_expected_count = excluded.pm10_hourly_expected_count,
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
      uk_aq_aggdaily.station_aqi_hourly.no2_hourly_mean_ugm3,
      uk_aq_aggdaily.station_aqi_hourly.pm25_hourly_mean_ugm3,
      uk_aq_aggdaily.station_aqi_hourly.pm10_hourly_mean_ugm3,
      uk_aq_aggdaily.station_aqi_hourly.pm25_rolling24h_mean_ugm3,
      uk_aq_aggdaily.station_aqi_hourly.pm10_rolling24h_mean_ugm3,
      uk_aq_aggdaily.station_aqi_hourly.no2_hourly_capture_ratio,
      uk_aq_aggdaily.station_aqi_hourly.pm25_hourly_capture_ratio,
      uk_aq_aggdaily.station_aqi_hourly.pm10_hourly_capture_ratio,
      uk_aq_aggdaily.station_aqi_hourly.no2_hourly_sample_count,
      uk_aq_aggdaily.station_aqi_hourly.pm25_hourly_sample_count,
      uk_aq_aggdaily.station_aqi_hourly.pm10_hourly_sample_count,
      uk_aq_aggdaily.station_aqi_hourly.no2_hourly_expected_count,
      uk_aq_aggdaily.station_aqi_hourly.pm25_hourly_expected_count,
      uk_aq_aggdaily.station_aqi_hourly.pm10_hourly_expected_count,
      uk_aq_aggdaily.station_aqi_hourly.pm25_rolling24h_valid_hours,
      uk_aq_aggdaily.station_aqi_hourly.pm10_rolling24h_valid_hours,
      uk_aq_aggdaily.station_aqi_hourly.daqi_no2_index_level,
      uk_aq_aggdaily.station_aqi_hourly.daqi_no2_index_band,
      uk_aq_aggdaily.station_aqi_hourly.daqi_pm25_index_level,
      uk_aq_aggdaily.station_aqi_hourly.daqi_pm25_index_band,
      uk_aq_aggdaily.station_aqi_hourly.daqi_pm10_index_level,
      uk_aq_aggdaily.station_aqi_hourly.daqi_pm10_index_band,
      uk_aq_aggdaily.station_aqi_hourly.eaqi_no2_index_level,
      uk_aq_aggdaily.station_aqi_hourly.eaqi_no2_index_band,
      uk_aq_aggdaily.station_aqi_hourly.eaqi_pm25_index_level,
      uk_aq_aggdaily.station_aqi_hourly.eaqi_pm25_index_band,
      uk_aq_aggdaily.station_aqi_hourly.eaqi_pm10_index_level,
      uk_aq_aggdaily.station_aqi_hourly.eaqi_pm10_index_band
    )
    is distinct from
    (
      excluded.no2_hourly_mean_ugm3,
      excluded.pm25_hourly_mean_ugm3,
      excluded.pm10_hourly_mean_ugm3,
      excluded.pm25_rolling24h_mean_ugm3,
      excluded.pm10_rolling24h_mean_ugm3,
      excluded.no2_hourly_capture_ratio,
      excluded.pm25_hourly_capture_ratio,
      excluded.pm10_hourly_capture_ratio,
      excluded.no2_hourly_sample_count,
      excluded.pm25_hourly_sample_count,
      excluded.pm10_hourly_sample_count,
      excluded.no2_hourly_expected_count,
      excluded.pm25_hourly_expected_count,
      excluded.pm10_hourly_expected_count,
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

drop function if exists uk_aq_public.uk_aq_rpc_station_aqi_rollups_refresh(
  timestamptz,
  timestamptz,
  bigint[]
);

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
      h.daqi_pm25_index_level as index_level
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
      h.daqi_pm10_index_level
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
      h.daqi_pm25_index_level as index_level
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
      h.daqi_pm10_index_level
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

drop function if exists uk_aq_public.uk_aq_rpc_aqi_compute_run_log(
  text,
  text,
  timestamptz,
  timestamptz,
  integer,
  integer,
  integer,
  integer,
  integer,
  integer,
  numeric,
  boolean,
  integer,
  integer,
  text,
  text,
  integer
);

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

  if coalesce(nullif(trim(p_run_mode), ''), '') not in ('fast', 'reconcile_short', 'reconcile_deep', 'backfill') then
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

drop function if exists uk_aq_public.uk_aq_rpc_aqi_compute_runs_cleanup(integer);

create or replace function uk_aq_public.uk_aq_rpc_aqi_compute_runs_cleanup(
  p_retention_days integer default 7
)
returns table (
  rows_deleted bigint
)
language plpgsql
security definer
set search_path = uk_aq_ops, public, pg_catalog
as $$
declare
  v_days integer;
  v_rows bigint := 0;
begin
  if auth.role() <> 'service_role' then
    raise exception 'service_role required';
  end if;

  v_days := greatest(1, least(coalesce(p_retention_days, 7), 3650));

  delete from uk_aq_ops.aqi_compute_runs
  where started_at < now() - make_interval(days => v_days);

  get diagnostics v_rows = row_count;

  return query select coalesce(v_rows, 0);
end;
$$;

revoke all on function uk_aq_public.uk_aq_rpc_aqi_breakpoints_active(
  timestamptz,
  text,
  text
) from public;
grant execute on function uk_aq_public.uk_aq_rpc_aqi_breakpoints_active(
  timestamptz,
  text,
  text
) to service_role;

revoke all on function uk_aq_public.uk_aq_rpc_station_aqi_hourly_upsert(
  jsonb,
  timestamptz,
  timestamptz
) from public;
grant execute on function uk_aq_public.uk_aq_rpc_station_aqi_hourly_upsert(
  jsonb,
  timestamptz,
  timestamptz
) to service_role;

revoke all on function uk_aq_public.uk_aq_rpc_station_aqi_rollups_refresh(
  timestamptz,
  timestamptz,
  bigint[]
) from public;
grant execute on function uk_aq_public.uk_aq_rpc_station_aqi_rollups_refresh(
  timestamptz,
  timestamptz,
  bigint[]
) to service_role;

revoke all on function uk_aq_public.uk_aq_rpc_aqi_compute_run_log(
  text,
  text,
  timestamptz,
  timestamptz,
  integer,
  integer,
  integer,
  integer,
  integer,
  integer,
  numeric,
  boolean,
  integer,
  integer,
  text,
  text,
  integer
) from public;
grant execute on function uk_aq_public.uk_aq_rpc_aqi_compute_run_log(
  text,
  text,
  timestamptz,
  timestamptz,
  integer,
  integer,
  integer,
  integer,
  integer,
  integer,
  numeric,
  boolean,
  integer,
  integer,
  text,
  text,
  integer
) to service_role;

revoke all on function uk_aq_public.uk_aq_rpc_aqi_compute_runs_cleanup(integer) from public;
grant execute on function uk_aq_public.uk_aq_rpc_aqi_compute_runs_cleanup(integer) to service_role;
