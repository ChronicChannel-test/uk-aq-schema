-- UK-AQ history schema (history-only observations in a separate schema).
-- Safe to rerun; uses IF NOT EXISTS where appropriate.

create schema if not exists uk_aq_history;
create schema if not exists uk_aq_public;
create schema if not exists uk_aq_ops;

create table if not exists uk_aq_history.observations (
  connector_id integer not null,
  timeseries_id integer not null,
  observed_at timestamptz not null,
  value double precision
) partition by range (observed_at);

create table if not exists uk_aq_history.observations_default
  partition of uk_aq_history.observations default;

comment on table uk_aq_history.observations_default is
  'Catch-all/default partition for out-of-range rows. Non-zero rows are treated as a maintenance alert signal.';

create index if not exists uk_aq_history_observations_default_observed_at_brin
  on uk_aq_history.observations_default using brin (observed_at);

do $$
declare
  v_today_utc date := (now() at time zone 'UTC')::date;
  v_day date;
  v_partition_name text;
begin
  for v_day in
    select generate_series(v_today_utc - 2, v_today_utc + 3, interval '1 day')::date
  loop
    v_partition_name := format('observations_%s', to_char(v_day, 'YYYYMMDD'));

    execute format(
      'create table if not exists uk_aq_history.%I '
      'partition of uk_aq_history.observations '
      'for values from (%L) to (%L)',
      v_partition_name,
      format('%s 00:00:00+00', v_day),
      format('%s 00:00:00+00', v_day + 1)
    );

    execute format(
      'create index if not exists %I on uk_aq_history.%I using brin (observed_at)',
      v_partition_name || '_observed_at_brin_idx',
      v_partition_name
    );

    if v_day between (v_today_utc - 2) and (v_today_utc + 3) then
      execute format(
        'create unique index if not exists %I on uk_aq_history.%I (connector_id, timeseries_id, observed_at)',
        v_partition_name || '_hot_key_uidx',
        v_partition_name
      );
    else
      execute format(
        'drop index if exists uk_aq_history.%I',
        v_partition_name || '_hot_key_uidx'
      );
    end if;
  end loop;
end $$;

-- RLS: history access is service_role only (Edge Functions / server).
alter table if exists uk_aq_history.observations enable row level security;

do $$
begin
  if not exists (
    select 1
    from pg_policies p
    where p.schemaname = 'uk_aq_history'
      and p.tablename = 'observations'
      and p.policyname = 'uk_aq_history_observations_service_role'
  ) then
    execute
      'create policy uk_aq_history_observations_service_role on uk_aq_history.observations '
      'for all using (auth.role() = ''service_role'') '
      'with check (auth.role() = ''service_role'');';
  end if;
end $$;

create table if not exists uk_aq_ops.db_size_metrics_hourly (
  bucket_hour timestamptz not null,
  database_label text not null check (database_label in ('ingestdb', 'historydb', 'aggdailydb')),
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

create or replace view uk_aq_public.uk_aq_db_size_metrics_hourly as
select
  bucket_hour,
  database_label,
  database_name,
  size_bytes,
  source,
  recorded_at,
  created_at,
  updated_at,
  oldest_observed_at
from uk_aq_ops.db_size_metrics_hourly;
alter view if exists uk_aq_public.uk_aq_db_size_metrics_hourly set (security_invoker = true);

create extension if not exists pg_cron with schema extensions;

-- Schedule daily history observations VACUUM FULL at 05:30 UTC.
select cron.unschedule(jobid)
from cron.job
where jobname = 'uk_aq_history_observations_vacuum_full_0530_utc';

select cron.schedule(
  'uk_aq_history_observations_vacuum_full_0530_utc',
  '30 5 * * *',
  $$vacuum (full, analyze, verbose) uk_aq_history.observations;$$
);

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
set search_path = uk_aq_ops, uk_aq_history, public, pg_catalog
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
    'historydb',
    current_database()::text,
    (
      select coalesce(sum(pg_database_size(pg_database.datname)), 0)::bigint
      from pg_database
    ),
    (select min(o.observed_at) from uk_aq_history.observations o),
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
where jobname = 'uk_aq_history_db_size_metrics_hourly';

select cron.schedule(
  'uk_aq_history_db_size_metrics_hourly',
  '2 * * * *',
  $$select * from uk_aq_ops.uk_aq_db_size_metric_sample_local();$$
);

create or replace function uk_aq_public.uk_aq_rpc_database_size_bytes()
returns table (
  database_name text,
  size_bytes bigint,
  oldest_observed_at timestamptz,
  sampled_at timestamptz
)
language plpgsql
security definer
set search_path = pg_catalog, public
as $$
begin
  if auth.role() <> 'service_role' then
    raise exception 'service_role required';
  end if;

  return query
  select
    current_database()::text as database_name,
    (
      select coalesce(sum(pg_database_size(pg_database.datname)), 0)::bigint
      from pg_database
    ) as size_bytes,
    (select min(o.observed_at) from uk_aq_history.observations o) as oldest_observed_at,
    now() as sampled_at;
end;
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

  if p_database_label not in ('ingestdb', 'historydb', 'aggdailydb') then
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

create or replace function uk_aq_public.uk_aq_rpc_db_size_metric_cleanup(
  p_retention_days integer default 120
)
returns table (rows_deleted bigint)
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

  v_days := greatest(1, least(coalesce(p_retention_days, 120), 3650));

  delete from uk_aq_ops.db_size_metrics_hourly
  where bucket_hour < now() - make_interval(days => v_days);

  get diagnostics v_rows = row_count;
  return query select v_rows;
end;
$$;

revoke all on function uk_aq_public.uk_aq_rpc_database_size_bytes() from public;
grant execute on function uk_aq_public.uk_aq_rpc_database_size_bytes() to service_role;

revoke all on function uk_aq_public.uk_aq_rpc_db_size_metric_upsert(
  text,
  text,
  bigint,
  timestamptz,
  timestamptz,
  text
) from public;
grant execute on function uk_aq_public.uk_aq_rpc_db_size_metric_upsert(
  text,
  text,
  bigint,
  timestamptz,
  timestamptz,
  text
) to service_role;

revoke all on function uk_aq_public.uk_aq_rpc_db_size_metric_cleanup(integer) from public;
grant execute on function uk_aq_public.uk_aq_rpc_db_size_metric_cleanup(integer) to service_role;

revoke all on function uk_aq_ops.uk_aq_db_size_metric_sample_local(integer, timestamptz, text) from public;
grant execute on function uk_aq_ops.uk_aq_db_size_metric_sample_local(integer, timestamptz, text) to service_role;

revoke all on uk_aq_public.uk_aq_db_size_metrics_hourly from public;
grant select on uk_aq_public.uk_aq_db_size_metrics_hourly to authenticated;
grant select on uk_aq_public.uk_aq_db_size_metrics_hourly to service_role;

grant usage on schema uk_aq_history to service_role;
grant usage on schema uk_aq_public to service_role;
grant usage on schema uk_aq_ops to service_role;
grant all on table uk_aq_history.observations to service_role;
grant all on table uk_aq_ops.db_size_metrics_hourly to service_role;
