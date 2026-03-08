-- Phase 2 (obs_aqidb -> obs_aqidb role): DB-size label hard cut.
-- Apply target: obs_aqidb (or current obs_aqidb before final rename)

create schema if not exists uk_aq_ops;

do $$
declare
  v_constraint_name text;
begin
  if to_regclass('uk_aq_ops.db_size_metrics_hourly') is not null then
    for v_constraint_name in
      select con.conname
      from pg_constraint con
      join pg_class rel on rel.oid = con.conrelid
      join pg_namespace nsp on nsp.oid = rel.relnamespace
      where nsp.nspname = 'uk_aq_ops'
        and rel.relname = 'db_size_metrics_hourly'
        and con.contype = 'c'
        and pg_get_constraintdef(con.oid) ilike '%database_label%'
    loop
      execute format(
        'alter table uk_aq_ops.db_size_metrics_hourly drop constraint if exists %I',
        v_constraint_name
      );
    end loop;

    if not exists (
      select 1
      from pg_constraint con
      join pg_class rel on rel.oid = con.conrelid
      join pg_namespace nsp on nsp.oid = rel.relnamespace
      where nsp.nspname = 'uk_aq_ops'
        and rel.relname = 'db_size_metrics_hourly'
        and con.conname = 'db_size_metrics_hourly_database_label_check'
    ) then
      execute $sql$
        alter table uk_aq_ops.db_size_metrics_hourly
        add constraint db_size_metrics_hourly_database_label_check
        check (database_label in ('ingestdb', 'obs_aqidb')) not valid
      $sql$;
    end if;
  end if;
end
$$;

create or replace function uk_aq_public.uk_aq_rpc_db_size_metric_upsert(
  p_database_label text,
  p_database_name text,
  p_size_bytes bigint,
  p_oldest_observed_at timestamptz default null,
  p_recorded_at timestamptz default now(),
  p_source text default null
)
returns table (rows_upserted int)
language plpgsql
security definer
set search_path = uk_aq_ops, public, pg_catalog
as $$
declare
  v_bucket_hour timestamptz;
  v_rows int := 0;
  v_source text;
begin
  if auth.role() <> 'service_role' then
    raise exception 'service_role required';
  end if;

  if p_database_label not in ('ingestdb', 'obs_aqidb') then
    raise exception 'invalid database_label: %', p_database_label;
  end if;

  if p_database_name is null or btrim(p_database_name) = '' then
    raise exception 'database_name is required';
  end if;

  if p_size_bytes is null or p_size_bytes < 0 then
    raise exception 'size_bytes must be >= 0';
  end if;

  v_bucket_hour := date_trunc('hour', coalesce(p_recorded_at, now()));
  v_source := coalesce(nullif(btrim(p_source), ''), 'uk_aq_db_size_logger_cloud_run');

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
    p_database_label,
    p_database_name,
    p_size_bytes,
    p_oldest_observed_at,
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

  get diagnostics v_rows = row_count;
  return query select v_rows;
end;
$$;

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
set search_path = uk_aq_ops, uk_aq_observs, public, pg_catalog
as $$
declare
  v_days integer;
  v_bucket_hour timestamptz;
  v_rows_upserted int := 0;
  v_rows_deleted bigint := 0;
  v_source text;
  v_oldest_observs timestamptz := null;
  v_oldest_aqilevels timestamptz := null;
  v_oldest_observed_at timestamptz := null;
begin
  v_days := greatest(1, least(coalesce(p_retention_days, 120), 3650));
  v_bucket_hour := date_trunc('hour', coalesce(p_recorded_at, now()));
  v_source := coalesce(nullif(btrim(p_source), ''), 'uk_aq_db_size_logger_pg_cron');

  if to_regclass('uk_aq_observs.observations') is not null then
    execute 'select min(o.observed_at) from uk_aq_observs.observations o'
      into v_oldest_observs;
  end if;

  if to_regclass('uk_aq_aqilevels.station_aqi_hourly') is not null then
    execute 'select min(a.timestamp_hour_utc) from uk_aq_aqilevels.station_aqi_hourly a'
      into v_oldest_aqilevels;
  end if;

  select min(v)
    into v_oldest_observed_at
  from (values (v_oldest_observs), (v_oldest_aqilevels)) as oldest(v);

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
    'obs_aqidb',
    current_database()::text,
    (
      select coalesce(sum(pg_database_size(pg_database.datname)), 0)::bigint
      from pg_database
    ),
    v_oldest_observed_at,
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

create or replace function uk_aq_public.uk_aq_rpc_schema_size_bytes(
  p_schema_name text default null
)
returns table (
  schema_name text,
  size_bytes bigint,
  oldest_observed_at timestamptz,
  sampled_at timestamptz
)
language plpgsql
security definer
set search_path = pg_catalog, public
as $$
declare
  v_schema_names text[];
  v_schema text;
  v_size_bytes bigint;
  v_oldest_observed_at timestamptz;
begin
  if auth.role() <> 'service_role' then
    raise exception 'service_role required';
  end if;

  if p_schema_name is null or btrim(p_schema_name) = '' then
    v_schema_names := array['uk_aq_observs', 'uk_aq_aqilevels'];
  elsif p_schema_name in ('uk_aq_observs', 'uk_aq_aqilevels') then
    v_schema_names := array[p_schema_name];
  else
    raise exception 'invalid schema_name: %', p_schema_name;
  end if;

  foreach v_schema in array v_schema_names loop
    v_size_bytes := 0;
    v_oldest_observed_at := null;

    if to_regnamespace(v_schema) is not null then
      execute format(
        $sql$
          select coalesce(sum(pg_total_relation_size(c.oid)), 0)::bigint
          from pg_class c
          join pg_namespace n on n.oid = c.relnamespace
          where n.nspname = %L
            and c.relkind in ('r', 'p', 'm', 't')
        $sql$,
        v_schema
      ) into v_size_bytes;

      if v_schema = 'uk_aq_observs'
         and to_regclass('uk_aq_observs.observations') is not null then
        execute 'select min(o.observed_at) from uk_aq_observs.observations o'
          into v_oldest_observed_at;
      elsif v_schema = 'uk_aq_aqilevels'
            and to_regclass('uk_aq_aqilevels.station_aqi_hourly') is not null then
        execute 'select min(a.timestamp_hour_utc) from uk_aq_aqilevels.station_aqi_hourly a'
          into v_oldest_observed_at;
      end if;
    end if;

    schema_name := v_schema;
    size_bytes := coalesce(v_size_bytes, 0);
    oldest_observed_at := v_oldest_observed_at;
    sampled_at := now();
    return next;
  end loop;
end;
$$;

revoke execute on function uk_aq_public.uk_aq_rpc_schema_size_bytes(text) from public;
revoke execute on function uk_aq_public.uk_aq_rpc_schema_size_bytes(text) from anon, authenticated;
grant execute on function uk_aq_public.uk_aq_rpc_schema_size_bytes(text) to service_role;

select cron.unschedule(jobid)
from cron.job
where jobname in ('uk_aq_observs_db_size_metrics_hourly', 'uk_aq_obs_aqidb_db_size_metrics_hourly');

select cron.schedule(
  'uk_aq_obs_aqidb_db_size_metrics_hourly',
  '2 * * * *',
  $$select * from uk_aq_ops.uk_aq_db_size_metric_sample_local();$$
);
