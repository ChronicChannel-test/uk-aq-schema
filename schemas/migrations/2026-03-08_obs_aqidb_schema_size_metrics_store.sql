-- Phase 3: persist schema-size hourly metrics in obs_aqidb.
-- Apply target: obs_aqidb

create schema if not exists uk_aq_ops;

create table if not exists uk_aq_ops.schema_size_metrics_hourly (
  bucket_hour timestamptz not null,
  database_label text not null check (database_label in ('obs_aqidb')),
  schema_name text not null check (schema_name in ('uk_aq_observs', 'uk_aq_aqilevels')),
  size_bytes bigint not null check (size_bytes >= 0),
  oldest_observed_at timestamptz,
  source text not null default 'uk_aq_db_size_logger_cloud_run',
  recorded_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (bucket_hour, database_label, schema_name)
);

create index if not exists schema_size_metrics_hourly_label_schema_idx
  on uk_aq_ops.schema_size_metrics_hourly (database_label, schema_name, bucket_hour desc);

create or replace view uk_aq_public.uk_aq_schema_size_metrics_hourly as
select
  bucket_hour,
  database_label,
  schema_name,
  size_bytes,
  source,
  recorded_at,
  created_at,
  updated_at,
  oldest_observed_at
from uk_aq_ops.schema_size_metrics_hourly;

alter view if exists uk_aq_public.uk_aq_schema_size_metrics_hourly set (security_invoker = true);

create or replace function uk_aq_public.uk_aq_rpc_schema_size_metric_upsert(
  p_database_label text,
  p_schema_name text,
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

  if p_database_label not in ('obs_aqidb') then
    raise exception 'invalid database_label: %', p_database_label;
  end if;

  if p_schema_name not in ('uk_aq_observs', 'uk_aq_aqilevels') then
    raise exception 'invalid schema_name: %', p_schema_name;
  end if;

  if p_size_bytes is null or p_size_bytes < 0 then
    raise exception 'size_bytes must be >= 0';
  end if;

  v_bucket_hour := date_trunc('hour', coalesce(p_recorded_at, now()));
  v_source := coalesce(nullif(btrim(p_source), ''), 'uk_aq_db_size_logger_cloud_run');

  insert into uk_aq_ops.schema_size_metrics_hourly (
    bucket_hour,
    database_label,
    schema_name,
    size_bytes,
    oldest_observed_at,
    source,
    recorded_at,
    updated_at
  )
  values (
    v_bucket_hour,
    p_database_label,
    p_schema_name,
    p_size_bytes,
    p_oldest_observed_at,
    v_source,
    coalesce(p_recorded_at, now()),
    now()
  )
  on conflict (bucket_hour, database_label, schema_name) do update set
    size_bytes = excluded.size_bytes,
    oldest_observed_at = excluded.oldest_observed_at,
    source = excluded.source,
    recorded_at = excluded.recorded_at,
    updated_at = now();

  get diagnostics v_rows = row_count;
  return query select v_rows;
end;
$$;

create or replace function uk_aq_public.uk_aq_rpc_schema_size_metric_cleanup(
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

  delete from uk_aq_ops.schema_size_metrics_hourly
  where bucket_hour < now() - make_interval(days => v_days);

  get diagnostics v_rows = row_count;
  return query select v_rows;
end;
$$;

grant usage on schema uk_aq_ops to service_role;
grant all on table uk_aq_ops.schema_size_metrics_hourly to service_role;

revoke all on function uk_aq_public.uk_aq_rpc_schema_size_metric_upsert(
  text,
  text,
  bigint,
  timestamptz,
  timestamptz,
  text
) from public;
grant execute on function uk_aq_public.uk_aq_rpc_schema_size_metric_upsert(
  text,
  text,
  bigint,
  timestamptz,
  timestamptz,
  text
) to service_role;

revoke all on function uk_aq_public.uk_aq_rpc_schema_size_metric_cleanup(integer) from public;
grant execute on function uk_aq_public.uk_aq_rpc_schema_size_metric_cleanup(integer) to service_role;

revoke all on uk_aq_public.uk_aq_schema_size_metrics_hourly from public;
grant select on uk_aq_public.uk_aq_schema_size_metrics_hourly to authenticated;
grant select on uk_aq_public.uk_aq_schema_size_metrics_hourly to service_role;
