-- UK-AQ history schema (history-only observations in a separate schema).
-- Safe to rerun; uses IF NOT EXISTS where appropriate.

create schema if not exists uk_aq_history;

create table if not exists uk_aq_history.observations (
  connector_code text not null,
  service_ref text not null,
  timeseries_ref text not null,
  observed_at timestamptz not null,
  value double precision,
  status text,
  moved_at timestamptz default now(),
  primary key (connector_code, service_ref, timeseries_ref, observed_at)
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
