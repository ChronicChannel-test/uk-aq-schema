-- UK-AQ history schema (history-only observations in a separate schema).
-- Safe to rerun; uses IF NOT EXISTS where appropriate.

create schema if not exists uk_aq_history;
create schema if not exists uk_aq_public;

create table if not exists uk_aq_history.observations (
  connector_id bigint not null,
  timeseries_id bigint not null,
  observed_at timestamptz not null,
  value double precision,
  status text,
  created_at timestamptz not null default now(),
  primary key (connector_id, timeseries_id, observed_at)
);

create index if not exists uk_aq_history_observations_observed_at_brin
  on uk_aq_history.observations using brin (observed_at);

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

grant usage on schema uk_aq_public to service_role;
