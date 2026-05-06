-- TEST-ONLY OpenAQ LIVE->TEST mirror schema objects.
-- Keep this file in the TEST schema repo only.

create schema if not exists uk_aq_ops;
create schema if not exists uk_aq_public;
create extension if not exists pgcrypto with schema extensions;
create extension if not exists pg_cron with schema extensions;
create extension if not exists pg_net with schema extensions;

create table if not exists uk_aq_ops.uk_aq_openaq_live_sync_state (
  job_name text primary key,
  cursor_observed_at timestamptz,
  cursor_timeseries_id integer,
  cursor_core_synced_at timestamptz,
  lock_owner uuid,
  lock_acquired_at timestamptz,
  lock_expires_at timestamptz,
  last_run_started_at timestamptz,
  last_run_finished_at timestamptz,
  last_status text,
  last_error text,
  rows_read bigint not null default 0,
  rows_written_ingest bigint not null default 0,
  rows_written_observs bigint not null default 0,
  updated_at timestamptz not null default now(),
  constraint uk_aq_openaq_live_sync_state_job_name_check
    check (job_name = any (array['observations'::text, 'core'::text, 'reseed'::text]))
);

create index if not exists uk_aq_openaq_live_sync_state_lock_idx
  on uk_aq_ops.uk_aq_openaq_live_sync_state (lock_expires_at);

grant select, insert, update, delete on uk_aq_ops.uk_aq_openaq_live_sync_state to service_role;

create or replace function uk_aq_public.uk_aq_rpc_openaq_live_sync_lock_acquire(
  p_job_name text,
  p_lock_owner uuid default gen_random_uuid(),
  p_lease_seconds integer default 1800
)
returns table (
  acquired boolean,
  job_name text,
  cursor_observed_at timestamptz,
  cursor_timeseries_id integer,
  cursor_core_synced_at timestamptz,
  lock_owner uuid,
  lock_expires_at timestamptz,
  last_status text,
  last_error text
)
language plpgsql
security definer
set search_path = uk_aq_ops, uk_aq_public, public, pg_catalog
as $function$
declare
  v_job_name text;
  v_lock_owner uuid;
  v_lease_seconds integer;
begin
  if auth.role() <> 'service_role' then
    raise exception 'service_role required';
  end if;

  v_job_name := lower(coalesce(nullif(trim(p_job_name), ''), ''));
  if v_job_name = '' or v_job_name not in ('observations', 'core', 'reseed') then
    raise exception 'unsupported job name: %', p_job_name;
  end if;

  v_lock_owner := coalesce(p_lock_owner, gen_random_uuid());
  v_lease_seconds := greatest(30, least(coalesce(p_lease_seconds, 1800), 7200));

  return query
  with attempt as (
    insert into uk_aq_ops.uk_aq_openaq_live_sync_state as s (
      job_name,
      lock_owner,
      lock_acquired_at,
      lock_expires_at,
      last_run_started_at,
      last_status,
      last_error,
      updated_at
    )
    values (
      v_job_name,
      v_lock_owner,
      now(),
      now() + make_interval(secs => v_lease_seconds),
      now(),
      'running',
      null,
      now()
    )
    on conflict on constraint uk_aq_openaq_live_sync_state_pkey do update
    set
      lock_owner = excluded.lock_owner,
      lock_acquired_at = excluded.lock_acquired_at,
      lock_expires_at = excluded.lock_expires_at,
      last_run_started_at = excluded.last_run_started_at,
      last_status = excluded.last_status,
      last_error = excluded.last_error,
      updated_at = excluded.updated_at
    where
      s.lock_expires_at is null
      or s.lock_expires_at <= now()
      or s.lock_owner = excluded.lock_owner
    returning
      true as acquired_flag,
      s.job_name,
      s.cursor_observed_at,
      s.cursor_timeseries_id,
      s.cursor_core_synced_at,
      s.lock_owner,
      s.lock_expires_at,
      s.last_status,
      s.last_error
  ),
  chosen as (
    select * from attempt
    union all
    select
      false as acquired_flag,
      s.job_name,
      s.cursor_observed_at,
      s.cursor_timeseries_id,
      s.cursor_core_synced_at,
      s.lock_owner,
      s.lock_expires_at,
      s.last_status,
      s.last_error
    from uk_aq_ops.uk_aq_openaq_live_sync_state s
    where s.job_name = v_job_name
      and not exists (select 1 from attempt)
  )
  select
    c.acquired_flag,
    c.job_name,
    c.cursor_observed_at,
    c.cursor_timeseries_id,
    c.cursor_core_synced_at,
    c.lock_owner,
    c.lock_expires_at,
    c.last_status,
    c.last_error
  from chosen c
  limit 1;
end;
$function$;

create or replace function uk_aq_public.uk_aq_rpc_openaq_live_sync_lock_release(
  p_job_name text,
  p_lock_owner uuid,
  p_status text default 'success',
  p_error text default null,
  p_cursor_observed_at timestamptz default null,
  p_cursor_timeseries_id integer default null,
  p_cursor_core_synced_at timestamptz default null,
  p_rows_read bigint default null,
  p_rows_written_ingest bigint default null,
  p_rows_written_observs bigint default null
)
returns table (rows_updated integer)
language plpgsql
security definer
set search_path = uk_aq_ops, uk_aq_public, public, pg_catalog
as $function$
declare
  v_job_name text;
  v_status text;
  v_rows_updated integer := 0;
begin
  if auth.role() <> 'service_role' then
    raise exception 'service_role required';
  end if;

  v_job_name := lower(coalesce(nullif(trim(p_job_name), ''), ''));
  if v_job_name = '' or v_job_name not in ('observations', 'core', 'reseed') then
    raise exception 'unsupported job name: %', p_job_name;
  end if;
  if p_lock_owner is null then
    raise exception 'p_lock_owner is required';
  end if;

  v_status := lower(coalesce(nullif(trim(p_status), ''), 'success'));

  update uk_aq_ops.uk_aq_openaq_live_sync_state s
  set
    cursor_observed_at = coalesce(p_cursor_observed_at, s.cursor_observed_at),
    cursor_timeseries_id = coalesce(p_cursor_timeseries_id, s.cursor_timeseries_id),
    cursor_core_synced_at = coalesce(p_cursor_core_synced_at, s.cursor_core_synced_at),
    lock_owner = null,
    lock_acquired_at = null,
    lock_expires_at = null,
    last_run_finished_at = now(),
    last_status = v_status,
    last_error = case when p_error is null or btrim(p_error) = '' then null else p_error end,
    rows_read = coalesce(p_rows_read, s.rows_read),
    rows_written_ingest = coalesce(p_rows_written_ingest, s.rows_written_ingest),
    rows_written_observs = coalesce(p_rows_written_observs, s.rows_written_observs),
    updated_at = now()
  where s.job_name = v_job_name
    and s.lock_owner = p_lock_owner;

  get diagnostics v_rows_updated = row_count;
  return query select coalesce(v_rows_updated, 0);
end;
$function$;

create or replace function uk_aq_public.uk_aq_rpc_openaq_live_sync_state_get(
  p_job_name text
)
returns table (
  job_name text,
  cursor_observed_at timestamptz,
  cursor_timeseries_id integer,
  cursor_core_synced_at timestamptz,
  lock_owner uuid,
  lock_expires_at timestamptz,
  last_status text,
  last_error text,
  rows_read bigint,
  rows_written_ingest bigint,
  rows_written_observs bigint,
  updated_at timestamptz
)
language plpgsql
security definer
set search_path = uk_aq_ops, uk_aq_public, public, pg_catalog
as $function$
declare
  v_job_name text;
begin
  if auth.role() <> 'service_role' then
    raise exception 'service_role required';
  end if;

  v_job_name := lower(coalesce(nullif(trim(p_job_name), ''), ''));
  if v_job_name = '' or v_job_name not in ('observations', 'core', 'reseed') then
    raise exception 'unsupported job name: %', p_job_name;
  end if;

  return query
  select
    s.job_name,
    s.cursor_observed_at,
    s.cursor_timeseries_id,
    s.cursor_core_synced_at,
    s.lock_owner,
    s.lock_expires_at,
    s.last_status,
    s.last_error,
    s.rows_read,
    s.rows_written_ingest,
    s.rows_written_observs,
    s.updated_at
  from uk_aq_ops.uk_aq_openaq_live_sync_state s
  where s.job_name = v_job_name;
end;
$function$;

create or replace function uk_aq_public.uk_aq_rpc_openaq_live_sync_sequence_reseed()
returns table (
  sequence_name text,
  set_to bigint
)
language plpgsql
security definer
set search_path = uk_aq_core, uk_aq_public, public, pg_catalog
as $function$
declare
  v_seq_name text;
  v_max_id bigint;
  v_table_name text;
  v_column_name text;
begin
  if auth.role() <> 'service_role' then
    raise exception 'service_role required';
  end if;

  for v_table_name, v_column_name in
    select *
    from (
      values
        ('uk_aq_core.connectors', 'id'),
        ('uk_aq_core.observed_properties', 'id'),
        ('uk_aq_core.categories', 'id'),
        ('uk_aq_core.phenomena', 'id'),
        ('uk_aq_core.offerings', 'id'),
        ('uk_aq_core.features', 'id'),
        ('uk_aq_core.procedures', 'id'),
        ('uk_aq_core.stations', 'id'),
        ('uk_aq_core.timeseries', 'id')
    ) t(table_name, column_name)
  loop
    v_seq_name := pg_get_serial_sequence(v_table_name, v_column_name);
    if v_seq_name is null then
      continue;
    end if;

    execute format(
      'select coalesce(max(%I), 0)::bigint from %s',
      v_column_name,
      v_table_name
    )
    into v_max_id;

    perform setval(v_seq_name, greatest(v_max_id, 1), true);
    return query select v_seq_name, greatest(v_max_id, 1);
  end loop;
end;
$function$;

create or replace function uk_aq_ops.uk_aq_openaq_live_sync_schedule_invoke(
  p_mode text,
  p_overlap_minutes integer default null
)
returns bigint
language plpgsql
security definer
set search_path = uk_aq_ops, public, extensions, pg_catalog
as $function$
declare
  v_mode text := lower(coalesce(nullif(trim(p_mode), ''), 'observations'));
  v_overlap_minutes integer;
  v_function_url text;
  v_auth_token text;
  v_cron_secret text;
  v_headers jsonb;
  v_body jsonb;
  v_request_id bigint;
begin
  if v_mode not in ('observations', 'core') then
    raise exception 'unsupported mode: %', p_mode;
  end if;

  v_overlap_minutes := greatest(0, least(coalesce(p_overlap_minutes, 0), 180));

  select decrypted_secret
  into v_function_url
  from vault.decrypted_secrets
  where name = 'UK_AQ_OPENAQ_MIRROR_FUNCTION_URL'
  order by created_at desc
  limit 1;

  select decrypted_secret
  into v_auth_token
  from vault.decrypted_secrets
  where name = 'UK_AQ_OPENAQ_MIRROR_AUTH_TOKEN'
  order by created_at desc
  limit 1;

  select decrypted_secret
  into v_cron_secret
  from vault.decrypted_secrets
  where name = 'SB_UK_AQ_CRON_SECRET'
  order by created_at desc
  limit 1;

  if coalesce(v_function_url, '') = '' then
    raise exception 'missing Vault secret: UK_AQ_OPENAQ_MIRROR_FUNCTION_URL';
  end if;
  if coalesce(v_auth_token, '') = '' then
    raise exception 'missing Vault secret: UK_AQ_OPENAQ_MIRROR_AUTH_TOKEN';
  end if;

  v_headers := jsonb_strip_nulls(jsonb_build_object(
    'Content-Type', 'application/json',
    'Authorization', 'Bearer ' || v_auth_token,
    'X-Cron-Secret', nullif(v_cron_secret, '')
  ));

  v_body := jsonb_build_object(
    'mode', v_mode,
    'overlap_minutes', v_overlap_minutes
  );

  select net.http_post(
    url := v_function_url,
    headers := v_headers,
    body := v_body
  )
  into v_request_id;

  return v_request_id;
end;
$function$;

grant execute on function uk_aq_public.uk_aq_rpc_openaq_live_sync_lock_acquire(text, uuid, integer)
  to service_role;
grant execute on function uk_aq_public.uk_aq_rpc_openaq_live_sync_lock_release(
  text, uuid, text, text, timestamptz, integer, timestamptz, bigint, bigint, bigint
)
  to service_role;
grant execute on function uk_aq_public.uk_aq_rpc_openaq_live_sync_state_get(text)
  to service_role;
grant execute on function uk_aq_public.uk_aq_rpc_openaq_live_sync_sequence_reseed()
  to service_role;
grant execute on function uk_aq_ops.uk_aq_openaq_live_sync_schedule_invoke(text, integer)
  to service_role;

select cron.unschedule(jobid)
from cron.job
where jobname in (
  'uk_aq_openaq_live_sync_observations_15m',
  'uk_aq_openaq_live_sync_core_hourly',
  'uk_aq_openaq_live_sync_core_6h'
);

select cron.schedule(
  'uk_aq_openaq_live_sync_observations_15m',
  '*/15 * * * *',
  $$select uk_aq_ops.uk_aq_openaq_live_sync_schedule_invoke('observations', 5);$$
);

select cron.schedule(
  'uk_aq_openaq_live_sync_core_6h',
  '5 */6 * * *',
  $$select uk_aq_ops.uk_aq_openaq_live_sync_schedule_invoke('core', 60);$$
);
