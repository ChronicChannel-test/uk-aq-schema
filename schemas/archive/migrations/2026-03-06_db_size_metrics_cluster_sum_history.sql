-- Align history DB-size metrics with cluster-wide storage usage (Supabase quota style).

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
