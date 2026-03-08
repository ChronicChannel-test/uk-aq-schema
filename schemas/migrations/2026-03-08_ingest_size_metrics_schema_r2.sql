-- Phase 2 (ingest DB): hard-cut DB labels + R2 hourly size metrics.
-- Apply target: ingestdb

create schema if not exists uk_aq_ops;

do $$
declare
  v_constraint_name text;
begin
  -- Replace legacy DB-label check with hard-cut labels on existing table.
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

create or replace view uk_aq_public.uk_aq_r2_domain_size_metrics_hourly as
select
  bucket_hour,
  domain_name,
  size_bytes,
  source,
  recorded_at,
  created_at,
  updated_at
from uk_aq_ops.r2_domain_size_metrics_hourly;

alter view if exists uk_aq_public.uk_aq_r2_domain_size_metrics_hourly set (security_invoker = true);

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

create or replace function uk_aq_public.uk_aq_rpc_r2_domain_size_metric_upsert(
  p_domain_name text,
  p_size_bytes bigint,
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

  if p_domain_name not in ('observations', 'aqilevels') then
    raise exception 'invalid domain_name: %', p_domain_name;
  end if;

  if p_size_bytes is null or p_size_bytes < 0 then
    raise exception 'size_bytes must be >= 0';
  end if;

  v_bucket_hour := date_trunc('hour', coalesce(p_recorded_at, now()));
  v_source := coalesce(nullif(btrim(p_source), ''), 'uk_aq_db_size_logger_cloud_run');

  insert into uk_aq_ops.r2_domain_size_metrics_hourly (
    bucket_hour,
    domain_name,
    size_bytes,
    source,
    recorded_at,
    updated_at
  )
  values (
    v_bucket_hour,
    p_domain_name,
    p_size_bytes,
    v_source,
    coalesce(p_recorded_at, now()),
    now()
  )
  on conflict (bucket_hour, domain_name) do update set
    size_bytes = excluded.size_bytes,
    source = excluded.source,
    recorded_at = excluded.recorded_at,
    updated_at = now();

  get diagnostics v_rows = row_count;
  return query select v_rows;
end;
$$;

create or replace function uk_aq_public.uk_aq_rpc_r2_domain_size_metric_cleanup(
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

  delete from uk_aq_ops.r2_domain_size_metrics_hourly
  where bucket_hour < now() - make_interval(days => v_days);

  get diagnostics v_rows = row_count;
  return query select v_rows;
end;
$$;

grant usage on schema uk_aq_ops to service_role;
grant all on table uk_aq_ops.r2_domain_size_metrics_hourly to service_role;

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

revoke all on function uk_aq_public.uk_aq_rpc_r2_domain_size_metric_upsert(
  text,
  bigint,
  timestamptz,
  text
) from public;
grant execute on function uk_aq_public.uk_aq_rpc_r2_domain_size_metric_upsert(
  text,
  bigint,
  timestamptz,
  text
) to service_role;

revoke all on function uk_aq_public.uk_aq_rpc_r2_domain_size_metric_cleanup(integer) from public;
grant execute on function uk_aq_public.uk_aq_rpc_r2_domain_size_metric_cleanup(integer) to service_role;

revoke all on uk_aq_public.uk_aq_r2_domain_size_metrics_hourly from public;
grant select on uk_aq_public.uk_aq_r2_domain_size_metrics_hourly to authenticated;
grant select on uk_aq_public.uk_aq_r2_domain_size_metrics_hourly to service_role;
