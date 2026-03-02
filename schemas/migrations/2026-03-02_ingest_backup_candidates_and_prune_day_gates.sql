-- Phase B backup control tables for ingest prune flow.
-- Adds candidate tracking (day+connector) and per-day prune gate state.

create schema if not exists uk_aq_ops;

create or replace function uk_aq_ops.uk_aq_touch_updated_at()
returns trigger
language plpgsql
set search_path = uk_aq_ops, public, pg_catalog
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

create table if not exists uk_aq_ops.backup_candidates (
  day_utc date not null,
  connector_id integer not null,
  expected_row_count bigint not null,
  min_observed_at timestamptz,
  max_observed_at timestamptz,
  status text not null default 'pending',
  run_id text,
  last_error text,
  manifest_key text,
  backup_row_count bigint,
  backup_file_count integer,
  backup_total_bytes bigint,
  backup_completed_at timestamptz,
  created_at timestamptz default now(),
  updated_at timestamptz default now(),
  primary key (day_utc, connector_id)
);

alter table if exists uk_aq_ops.backup_candidates
  add column if not exists expected_row_count bigint;
alter table if exists uk_aq_ops.backup_candidates
  add column if not exists min_observed_at timestamptz;
alter table if exists uk_aq_ops.backup_candidates
  add column if not exists max_observed_at timestamptz;
alter table if exists uk_aq_ops.backup_candidates
  add column if not exists status text;
alter table if exists uk_aq_ops.backup_candidates
  add column if not exists run_id text;
alter table if exists uk_aq_ops.backup_candidates
  add column if not exists last_error text;
alter table if exists uk_aq_ops.backup_candidates
  add column if not exists manifest_key text;
alter table if exists uk_aq_ops.backup_candidates
  add column if not exists backup_row_count bigint;
alter table if exists uk_aq_ops.backup_candidates
  add column if not exists backup_file_count integer;
alter table if exists uk_aq_ops.backup_candidates
  add column if not exists backup_total_bytes bigint;
alter table if exists uk_aq_ops.backup_candidates
  add column if not exists backup_completed_at timestamptz;
alter table if exists uk_aq_ops.backup_candidates
  add column if not exists created_at timestamptz default now();
alter table if exists uk_aq_ops.backup_candidates
  add column if not exists updated_at timestamptz default now();

update uk_aq_ops.backup_candidates
set
  status = coalesce(nullif(btrim(status), ''), 'pending'),
  expected_row_count = coalesce(expected_row_count, 0),
  updated_at = coalesce(updated_at, now()),
  created_at = coalesce(created_at, now())
where
  status is null
  or btrim(status) = ''
  or expected_row_count is null
  or updated_at is null
  or created_at is null;

alter table uk_aq_ops.backup_candidates
  alter column expected_row_count set not null;
alter table uk_aq_ops.backup_candidates
  alter column status set not null;
alter table uk_aq_ops.backup_candidates
  alter column status set default 'pending';

create index if not exists backup_candidates_status_day_idx
  on uk_aq_ops.backup_candidates(status, day_utc);

create index if not exists backup_candidates_day_status_idx
  on uk_aq_ops.backup_candidates(day_utc, status, connector_id);

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'backup_candidates_status_check'
      and conrelid = 'uk_aq_ops.backup_candidates'::regclass
  ) then
    alter table uk_aq_ops.backup_candidates
      add constraint backup_candidates_status_check
      check (status in ('pending', 'in_progress', 'complete', 'failed'));
  end if;
end
$$;

drop trigger if exists backup_candidates_touch_updated_at on uk_aq_ops.backup_candidates;
create trigger backup_candidates_touch_updated_at
before update on uk_aq_ops.backup_candidates
for each row execute function uk_aq_ops.uk_aq_touch_updated_at();

create table if not exists uk_aq_ops.prune_day_gates (
  day_utc date primary key,
  backup_done boolean not null default false,
  backup_run_id text,
  backup_manifest_key text,
  backup_row_count bigint,
  backup_file_count integer,
  backup_total_bytes bigint,
  backup_completed_at timestamptz,
  aggregate_done boolean not null default false,
  history_repair_status text not null default 'not_required',
  updated_at timestamptz default now()
);

alter table if exists uk_aq_ops.prune_day_gates
  add column if not exists backup_done boolean default false;
alter table if exists uk_aq_ops.prune_day_gates
  add column if not exists backup_run_id text;
alter table if exists uk_aq_ops.prune_day_gates
  add column if not exists backup_manifest_key text;
alter table if exists uk_aq_ops.prune_day_gates
  add column if not exists backup_row_count bigint;
alter table if exists uk_aq_ops.prune_day_gates
  add column if not exists backup_file_count integer;
alter table if exists uk_aq_ops.prune_day_gates
  add column if not exists backup_total_bytes bigint;
alter table if exists uk_aq_ops.prune_day_gates
  add column if not exists backup_completed_at timestamptz;
alter table if exists uk_aq_ops.prune_day_gates
  add column if not exists aggregate_done boolean default false;
alter table if exists uk_aq_ops.prune_day_gates
  add column if not exists history_repair_status text default 'not_required';
alter table if exists uk_aq_ops.prune_day_gates
  add column if not exists updated_at timestamptz default now();

update uk_aq_ops.prune_day_gates
set
  backup_done = coalesce(backup_done, false),
  aggregate_done = coalesce(aggregate_done, false),
  history_repair_status = coalesce(nullif(btrim(history_repair_status), ''), 'not_required'),
  updated_at = coalesce(updated_at, now())
where
  backup_done is null
  or aggregate_done is null
  or history_repair_status is null
  or btrim(history_repair_status) = ''
  or updated_at is null;

alter table uk_aq_ops.prune_day_gates
  alter column backup_done set not null;
alter table uk_aq_ops.prune_day_gates
  alter column backup_done set default false;
alter table uk_aq_ops.prune_day_gates
  alter column aggregate_done set not null;
alter table uk_aq_ops.prune_day_gates
  alter column aggregate_done set default false;
alter table uk_aq_ops.prune_day_gates
  alter column history_repair_status set not null;
alter table uk_aq_ops.prune_day_gates
  alter column history_repair_status set default 'not_required';

create index if not exists prune_day_gates_backup_done_idx
  on uk_aq_ops.prune_day_gates(backup_done, day_utc);

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'prune_day_gates_history_repair_status_check'
      and conrelid = 'uk_aq_ops.prune_day_gates'::regclass
  ) then
    alter table uk_aq_ops.prune_day_gates
      add constraint prune_day_gates_history_repair_status_check
      check (history_repair_status in ('not_required', 'queued', 'in_progress', 'resolved', 'failed'));
  end if;
end
$$;

drop trigger if exists prune_day_gates_touch_updated_at on uk_aq_ops.prune_day_gates;
create trigger prune_day_gates_touch_updated_at
before update on uk_aq_ops.prune_day_gates
for each row execute function uk_aq_ops.uk_aq_touch_updated_at();

grant usage on schema uk_aq_ops to service_role;
grant all on all tables in schema uk_aq_ops to service_role;
grant execute on all functions in schema uk_aq_ops to service_role;

alter default privileges in schema uk_aq_ops
  grant all on tables to service_role;
alter default privileges in schema uk_aq_ops
  grant execute on functions to service_role;
