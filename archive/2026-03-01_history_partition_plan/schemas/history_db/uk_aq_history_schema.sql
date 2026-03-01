-- UK-AQ history schema (history-only observations in a separate schema).
-- Safe to rerun; uses IF NOT EXISTS where appropriate.

create schema if not exists uk_aq_history;
create schema if not exists uk_aq_public;

create table if not exists uk_aq_history.status_codes (
  status_id smallint primary key,
  code text not null unique,
  description text,
  severity smallint,
  is_public boolean not null default true,
  created_at timestamptz not null default now()
);

comment on table uk_aq_history.status_codes is
  'Canonical status dictionary for history observations, intended for QA scripts and future validation, not ingest writes.';
comment on column uk_aq_history.status_codes.status_id is
  'Canonical smallint status identifier for QA scripts and future validation, not ingest writes.';
comment on column uk_aq_history.status_codes.code is
  'Stable canonical status code used for QA scripts and future validation, not ingest writes.';
comment on column uk_aq_history.status_codes.description is
  'Optional human-readable status description for QA scripts and future validation, not ingest writes.';
comment on column uk_aq_history.status_codes.severity is
  'Optional canonical severity ranking for QA scripts and future validation, not ingest writes.';
comment on column uk_aq_history.status_codes.is_public is
  'Flag for whether the canonical status is suitable for public-facing use in QA/validation outputs; not ingest writes.';
comment on column uk_aq_history.status_codes.created_at is
  'Creation timestamp for canonical status dictionary rows maintained for QA scripts and future validation, not ingest writes.';

create table if not exists uk_aq_history.observations (
  connector_id integer not null,
  timeseries_id integer not null,
  observed_at timestamptz not null,
  value double precision,
  status_id smallint,
  created_at timestamptz not null default now(),
  constraint uk_aq_history_observations_status_id_fkey
    foreign key (status_id)
    references uk_aq_history.status_codes(status_id)
    on delete set null,
  primary key (connector_id, timeseries_id, observed_at)
);

alter table if exists uk_aq_history.observations
  add column if not exists status_id smallint;

alter table if exists uk_aq_history.observations
  drop column if exists status;

do $$
begin
  if not exists (
    select 1
    from pg_constraint c
    join pg_namespace n on n.oid = c.connamespace
    where n.nspname = 'uk_aq_history'
      and c.conname = 'uk_aq_history_observations_status_id_fkey'
  ) then
    execute
      'alter table uk_aq_history.observations '
      'add constraint uk_aq_history_observations_status_id_fkey '
      'foreign key (status_id) '
      'references uk_aq_history.status_codes(status_id) '
      'on delete set null';
  end if;
end $$;

create index if not exists uk_aq_history_observations_observed_at_brin
  on uk_aq_history.observations using brin (observed_at);

-- RLS: history access is service_role only (Edge Functions / server).
alter table if exists uk_aq_history.observations enable row level security;
alter table if exists uk_aq_history.status_codes enable row level security;

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

revoke all on table uk_aq_history.status_codes from public;
revoke all on table uk_aq_history.status_codes from service_role;
do $$
declare
  v_role text;
begin
  for v_role in
    select rolname
    from pg_roles
    where rolname ilike '%ingest%'
  loop
    execute format(
      'revoke all on table uk_aq_history.status_codes from %I',
      v_role
    );
  end loop;
end $$;

grant usage on schema uk_aq_history to service_role;
grant usage on schema uk_aq_public to service_role;
grant all on table uk_aq_history.observations to service_role;
