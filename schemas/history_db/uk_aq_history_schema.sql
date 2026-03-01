-- UK-AQ history schema (history-only observations in a separate schema).
-- Safe to rerun; uses IF NOT EXISTS where appropriate.

create schema if not exists uk_aq_history;
create schema if not exists uk_aq_public;

create table if not exists uk_aq_history.observations (
  connector_id integer not null,
  timeseries_id integer not null,
  observed_at timestamptz not null,
  value double precision,
  created_at timestamptz not null default now()
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
    select generate_series(v_today_utc - 2, v_today_utc + 7, interval '1 day')::date
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

    if v_day between (v_today_utc - 2) and v_today_utc then
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

create or replace function uk_aq_public.uk_aq_rpc_database_size_bytes()
returns table (
  database_name text,
  size_bytes bigint,
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
    pg_database_size(current_database())::bigint as size_bytes,
    now() as sampled_at;
end;
$$;

revoke all on function uk_aq_public.uk_aq_rpc_database_size_bytes() from public;
grant execute on function uk_aq_public.uk_aq_rpc_database_size_bytes() to service_role;

grant usage on schema uk_aq_history to service_role;
grant usage on schema uk_aq_public to service_role;
grant all on table uk_aq_history.observations to service_role;
