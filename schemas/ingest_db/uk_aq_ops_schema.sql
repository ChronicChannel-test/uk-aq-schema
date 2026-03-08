-- uk_aq_ops schema (ingest DB): operational metrics tables, ops functions, and scheduler wiring.

create schema if not exists uk_aq_ops;

create table if not exists uk_aq_ops.db_size_metrics_hourly (
  bucket_hour timestamptz not null,
  database_label text not null check (database_label in ('ingestdb', 'obs_aqidb')),
  database_name text not null,
  size_bytes bigint not null check (size_bytes >= 0),
  oldest_observed_at timestamptz,
  source text not null default 'uk_aq_db_size_logger_cloud_run',
  recorded_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (bucket_hour, database_label)
);

create index if not exists db_size_metrics_hourly_database_label_idx
  on uk_aq_ops.db_size_metrics_hourly (database_label, bucket_hour desc);

create table if not exists uk_aq_ops.r2_domain_size_metrics_hourly (
  bucket_hour timestamptz not null,
  domain_name text not null check (domain_name in ('observations', 'aqilevels')),
  size_bytes bigint not null check (size_bytes >= 0),
  source text not null default 'uk_aq_db_size_logger_cloud_run',
  recorded_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (bucket_hour, domain_name)
);

create index if not exists r2_domain_size_metrics_hourly_domain_idx
  on uk_aq_ops.r2_domain_size_metrics_hourly (domain_name, bucket_hour desc);

do $$
declare
  v_fn record;
begin
  for v_fn in
    select
      p.proname,
      pg_get_function_identity_arguments(p.oid) as identity_args
    from pg_proc p
    join pg_namespace n on n.oid = p.pronamespace
    where n.nspname = 'uk_aq_ops'
      and p.proname = 'uk_aq_db_size_metric_sample_local'
  loop
    execute format(
      'drop function if exists uk_aq_ops.%I(%s)',
      v_fn.proname,
      v_fn.identity_args
    );
  end loop;
end
$$;

create extension if not exists pg_cron with schema extensions;

create or replace function uk_aq_ops.uk_aq_db_size_metric_sample_local(
  p_retention_days integer default 120,
  p_recorded_at timestamptz default now(),
  p_source text default 'uk_aq_db_size_logger_pg_cron'
)
returns table (
  rows_upserted int,
  rows_deleted bigint
)
language plpgsql
security definer
set search_path = uk_aq_ops, uk_aq_core, public, pg_catalog
as $$
declare
  v_days integer;
  v_bucket_hour timestamptz;
  v_rows_upserted int := 0;
  v_rows_deleted bigint := 0;
  v_source text;
begin
  v_days := greatest(1, least(coalesce(p_retention_days, 120), 3650));
  v_bucket_hour := date_trunc('hour', coalesce(p_recorded_at, now()));
  v_source := coalesce(nullif(btrim(p_source), ''), 'uk_aq_db_size_logger_pg_cron');

  insert into uk_aq_ops.db_size_metrics_hourly (
    bucket_hour,
    database_label,
    database_name,
    size_bytes,
    oldest_observed_at,
    source,
    recorded_at,
    updated_at
  )
  values (
    v_bucket_hour,
    'ingestdb',
    current_database()::text,
    (
      select coalesce(sum(pg_database_size(pg_database.datname)), 0)::bigint
      from pg_database
    ),
    (select min(o.observed_at) from uk_aq_core.observations o),
    v_source,
    coalesce(p_recorded_at, now()),
    now()
  )
  on conflict (bucket_hour, database_label) do update set
    database_name = excluded.database_name,
    size_bytes = excluded.size_bytes,
    oldest_observed_at = excluded.oldest_observed_at,
    source = excluded.source,
    recorded_at = excluded.recorded_at,
    updated_at = now();

  get diagnostics v_rows_upserted = row_count;

  delete from uk_aq_ops.db_size_metrics_hourly
  where bucket_hour < now() - make_interval(days => v_days);

  get diagnostics v_rows_deleted = row_count;

  return query select v_rows_upserted, v_rows_deleted;
end;
$$;

select cron.unschedule(jobid)
from cron.job
where jobname = 'uk_aq_ingest_db_size_metrics_hourly';

select cron.schedule(
  'uk_aq_ingest_db_size_metrics_hourly',
  '2 * * * *',
  $$select * from uk_aq_ops.uk_aq_db_size_metric_sample_local();$$
);

drop function if exists uk_aq_ops.uk_aq_station_aqi_hourly_ingest_tick(
  timestamptz,
  bigint[],
  integer
);

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

select cron.unschedule(jobid)
from cron.job
where jobname = 'uk_aq_ingest_station_aqi_hourly_helper_tick';

select cron.schedule(
  'uk_aq_ingest_station_aqi_hourly_helper_tick',
  '10 * * * *',
  $$select * from uk_aq_ops.uk_aq_station_aqi_hourly_ingest_tick();$$
);

grant usage on schema uk_aq_ops to service_role;
grant all on table uk_aq_ops.db_size_metrics_hourly to service_role;
grant all on table uk_aq_ops.r2_domain_size_metrics_hourly to service_role;

revoke all on function uk_aq_ops.uk_aq_db_size_metric_sample_local(integer, timestamptz, text) from public;
grant execute on function uk_aq_ops.uk_aq_db_size_metric_sample_local(integer, timestamptz, text) to service_role;

revoke all on function uk_aq_ops.uk_aq_station_aqi_hourly_ingest_tick(
  timestamptz,
  bigint[],
  integer
) from public;
grant execute on function uk_aq_ops.uk_aq_station_aqi_hourly_ingest_tick(
  timestamptz,
  bigint[],
  integer
) to service_role;
