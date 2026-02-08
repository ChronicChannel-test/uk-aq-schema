-- Dual-write bootstrap for MAIN DB (uk_aq_core + uk_aq_raw).
-- Safe to run multiple times.

create extension if not exists pgcrypto;

create schema if not exists uk_aq_raw;
create schema if not exists uk_aq_public;

create table if not exists uk_aq_raw.history_observation_outbox (
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz not null default now(),
  next_attempt_at timestamptz not null default now(),
  attempts integer not null default 0,
  last_error text,
  payload jsonb not null
);

create index if not exists history_observation_outbox_next_attempt_at_idx
  on uk_aq_raw.history_observation_outbox (next_attempt_at);

create index if not exists history_observation_outbox_created_at_idx
  on uk_aq_raw.history_observation_outbox (created_at);

create table if not exists uk_aq_raw.history_sync_receipt_daily (
  connector_id bigint not null,
  timeseries_id bigint not null,
  observed_day date not null,
  synced_at timestamptz not null default now(),
  primary key (connector_id, timeseries_id, observed_day)
);

create index if not exists history_sync_receipt_daily_observed_day_idx
  on uk_aq_raw.history_sync_receipt_daily (observed_day);

alter table uk_aq_raw.history_observation_outbox enable row level security;
alter table uk_aq_raw.history_sync_receipt_daily enable row level security;

do $$
begin
  if not exists (
    select 1
    from pg_policies
    where schemaname = 'uk_aq_raw'
      and tablename = 'history_observation_outbox'
      and policyname = 'history_observation_outbox_select_service_role'
  ) then
    execute 'create policy history_observation_outbox_select_service_role '
      'on uk_aq_raw.history_observation_outbox '
      'for select using (auth.role() = ''service_role'')';
  end if;

  if not exists (
    select 1
    from pg_policies
    where schemaname = 'uk_aq_raw'
      and tablename = 'history_observation_outbox'
      and policyname = 'history_observation_outbox_write_service_role'
  ) then
    execute 'create policy history_observation_outbox_write_service_role '
      'on uk_aq_raw.history_observation_outbox '
      'for all using (auth.role() = ''service_role'') '
      'with check (auth.role() = ''service_role'')';
  end if;

  if not exists (
    select 1
    from pg_policies
    where schemaname = 'uk_aq_raw'
      and tablename = 'history_sync_receipt_daily'
      and policyname = 'history_sync_receipt_daily_select_service_role'
  ) then
    execute 'create policy history_sync_receipt_daily_select_service_role '
      'on uk_aq_raw.history_sync_receipt_daily '
      'for select using (auth.role() = ''service_role'')';
  end if;

  if not exists (
    select 1
    from pg_policies
    where schemaname = 'uk_aq_raw'
      and tablename = 'history_sync_receipt_daily'
      and policyname = 'history_sync_receipt_daily_write_service_role'
  ) then
    execute 'create policy history_sync_receipt_daily_write_service_role '
      'on uk_aq_raw.history_sync_receipt_daily '
      'for all using (auth.role() = ''service_role'') '
      'with check (auth.role() = ''service_role'')';
  end if;
end $$;

create or replace function uk_aq_public.uk_aq_rpc_history_outbox_enqueue(entries jsonb)
returns table(rows_enqueued int)
language plpgsql
security definer
set search_path = uk_aq_raw, uk_aq_core, public, pg_catalog
as $$
declare
  v_count int := 0;
begin
  if auth.role() <> 'service_role' then
    raise exception 'service_role required';
  end if;

  if entries is null
    or jsonb_typeof(entries) <> 'array'
    or jsonb_array_length(entries) = 0
  then
    return query select 0;
    return;
  end if;

  insert into uk_aq_raw.history_observation_outbox (
    payload,
    next_attempt_at
  )
  select
    row_payload.payload,
    coalesce(row_payload.next_attempt_at, now())
  from jsonb_to_recordset(entries) as row_payload(
    payload jsonb,
    next_attempt_at timestamptz
  )
  where row_payload.payload is not null;

  get diagnostics v_count = row_count;
  return query select coalesce(v_count, 0);
end;
$$;

create or replace function uk_aq_public.uk_aq_rpc_history_outbox_claim(batch_limit int default 10)
returns table(
  id uuid,
  payload jsonb,
  attempts int
)
language plpgsql
security definer
set search_path = uk_aq_raw, uk_aq_core, public, pg_catalog
as $$
begin
  if auth.role() <> 'service_role' then
    raise exception 'service_role required';
  end if;

  return query
  with due as (
    select
      o.id,
      o.next_attempt_at,
      o.created_at
    from uk_aq_raw.history_observation_outbox o
    where o.next_attempt_at <= now()
    order by o.next_attempt_at asc, o.created_at asc
    for update skip locked
    limit greatest(coalesce(batch_limit, 10), 1)
  ),
  claimed as (
    update uk_aq_raw.history_observation_outbox o
    set next_attempt_at = now() + interval '5 minutes'
    from due
    where o.id = due.id
    returning o.id, o.payload, o.attempts
  )
  select c.id, c.payload, c.attempts
  from claimed c
  join due d on d.id = c.id
  order by d.next_attempt_at asc, d.created_at asc;
end;
$$;

create or replace function uk_aq_public.uk_aq_rpc_history_outbox_resolve(resolutions jsonb)
returns table(rows_resolved int)
language plpgsql
security definer
set search_path = uk_aq_raw, uk_aq_core, public, pg_catalog
as $$
declare
  v_resolved int := 0;
begin
  if auth.role() <> 'service_role' then
    raise exception 'service_role required';
  end if;

  if resolutions is null
    or jsonb_typeof(resolutions) <> 'array'
    or jsonb_array_length(resolutions) = 0
  then
    return query select 0;
    return;
  end if;

  with incoming as (
    select
      nullif(trim(item->>'id'), '')::uuid as id,
      coalesce((item->>'ok')::boolean, false) as ok,
      nullif(item->>'error', '') as error_message,
      case
        when item ? 'retry_in_seconds'
          and (item->>'retry_in_seconds') ~ '^[0-9]+$'
        then greatest(0, least((item->>'retry_in_seconds')::int, 3600))
        else null
      end as retry_in_seconds
    from jsonb_array_elements(resolutions) item
    where item ? 'id'
  ),
  deleted as (
    delete from uk_aq_raw.history_observation_outbox o
    using incoming i
    where i.ok = true
      and o.id = i.id
    returning o.id
  ),
  failed as (
    update uk_aq_raw.history_observation_outbox o
    set
      attempts = o.attempts + 1,
      last_error = case
        when (o.attempts + 1) >= 20 then
          concat(
            '[dead_letter_threshold_reached] ',
            coalesce(i.error_message, o.last_error, 'history delivery failed')
          )
        else
          coalesce(i.error_message, o.last_error, 'history delivery failed')
      end,
      next_attempt_at = now() + make_interval(
        secs => case
          when (o.attempts + 1) >= 20 then 3600
          else coalesce(
            i.retry_in_seconds,
            case o.attempts
              when 0 then 30
              when 1 then 120
              when 2 then 600
              else 3600
            end
          )
        end
      )
    from incoming i
    where i.ok = false
      and o.id = i.id
    returning o.id
  )
  select
    (select count(*) from deleted) + (select count(*) from failed)
  into v_resolved;

  return query select coalesce(v_resolved, 0);
end;
$$;

create or replace function uk_aq_public.uk_aq_rpc_history_sync_receipt_daily_upsert(rows jsonb)
returns table(rows_upserted int)
language plpgsql
security definer
set search_path = uk_aq_raw, uk_aq_core, public, pg_catalog
as $$
declare
  v_count int := 0;
begin
  if auth.role() <> 'service_role' then
    raise exception 'service_role required';
  end if;

  if rows is null
    or jsonb_typeof(rows) <> 'array'
    or jsonb_array_length(rows) = 0
  then
    return query select 0;
    return;
  end if;

  insert into uk_aq_raw.history_sync_receipt_daily (
    connector_id,
    timeseries_id,
    observed_day,
    synced_at
  )
  select
    input.connector_id,
    input.timeseries_id,
    input.observed_day,
    now()
  from jsonb_to_recordset(rows) as input(
    connector_id bigint,
    timeseries_id bigint,
    observed_day date
  )
  where input.connector_id is not null
    and input.timeseries_id is not null
    and input.observed_day is not null
  on conflict (connector_id, timeseries_id, observed_day)
  do update set
    synced_at = now();

  get diagnostics v_count = row_count;
  return query select coalesce(v_count, 0);
end;
$$;

revoke all on table uk_aq_raw.history_observation_outbox from public, anon, authenticated;
revoke all on table uk_aq_raw.history_sync_receipt_daily from public, anon, authenticated;
grant all on table uk_aq_raw.history_observation_outbox to service_role;
grant all on table uk_aq_raw.history_sync_receipt_daily to service_role;

grant usage on schema uk_aq_raw to service_role;
grant usage on schema uk_aq_public to service_role;

revoke all on function uk_aq_public.uk_aq_rpc_history_outbox_enqueue(jsonb) from public;
revoke all on function uk_aq_public.uk_aq_rpc_history_outbox_claim(int) from public;
revoke all on function uk_aq_public.uk_aq_rpc_history_outbox_resolve(jsonb) from public;
revoke all on function uk_aq_public.uk_aq_rpc_history_sync_receipt_daily_upsert(jsonb) from public;

grant execute on function uk_aq_public.uk_aq_rpc_history_outbox_enqueue(jsonb) to service_role;
grant execute on function uk_aq_public.uk_aq_rpc_history_outbox_claim(int) to service_role;
grant execute on function uk_aq_public.uk_aq_rpc_history_outbox_resolve(jsonb) to service_role;
grant execute on function uk_aq_public.uk_aq_rpc_history_sync_receipt_daily_upsert(jsonb) to service_role;
