-- Phase B backup v2:
-- 1) server-side projection contract for backup row shape
-- 2) resume checkpoint state on uk_aq_ops.backup_candidates

create schema if not exists uk_aq_ops;

create or replace function uk_aq_ops.uk_aq_phase_b_backup_rows(
  p_connector_id integer,
  p_day_start timestamptz,
  p_day_end timestamptz,
  p_after_timeseries_id integer default null,
  p_after_observed_at timestamptz default null
)
returns table (
  connector_id integer,
  timeseries_id integer,
  observed_at timestamptz,
  value double precision
)
language sql
stable
set search_path = uk_aq_core, uk_aq_ops, public, pg_catalog
as $$
  select
    o.connector_id,
    o.timeseries_id,
    o.observed_at,
    o.value
  from uk_aq_core.observations o
  where o.connector_id = p_connector_id
    and o.observed_at >= p_day_start
    and o.observed_at < p_day_end
    and (
      p_after_timeseries_id is null
      or p_after_observed_at is null
      or (o.timeseries_id, o.observed_at) > (p_after_timeseries_id, p_after_observed_at)
    )
  order by o.timeseries_id asc, o.observed_at asc
$$;

grant execute on function uk_aq_ops.uk_aq_phase_b_backup_rows(
  integer,
  timestamptz,
  timestamptz,
  integer,
  timestamptz
) to service_role;

alter table if exists uk_aq_ops.backup_candidates
  add column if not exists resume_last_timeseries_id integer;
alter table if exists uk_aq_ops.backup_candidates
  add column if not exists resume_last_observed_at timestamptz;
alter table if exists uk_aq_ops.backup_candidates
  add column if not exists resume_part_index integer default 0;
alter table if exists uk_aq_ops.backup_candidates
  add column if not exists resume_exported_row_count bigint default 0;
alter table if exists uk_aq_ops.backup_candidates
  add column if not exists resume_parts_json jsonb default '[]'::jsonb;

update uk_aq_ops.backup_candidates
set
  resume_part_index = coalesce(resume_part_index, 0),
  resume_exported_row_count = coalesce(resume_exported_row_count, 0),
  resume_parts_json = coalesce(resume_parts_json, '[]'::jsonb)
where
  resume_part_index is null
  or resume_exported_row_count is null
  or resume_parts_json is null;

alter table uk_aq_ops.backup_candidates
  alter column resume_part_index set not null;
alter table uk_aq_ops.backup_candidates
  alter column resume_part_index set default 0;
alter table uk_aq_ops.backup_candidates
  alter column resume_exported_row_count set not null;
alter table uk_aq_ops.backup_candidates
  alter column resume_exported_row_count set default 0;
alter table uk_aq_ops.backup_candidates
  alter column resume_parts_json set not null;
alter table uk_aq_ops.backup_candidates
  alter column resume_parts_json set default '[]'::jsonb;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'backup_candidates_resume_nonnegative_check'
      and conrelid = 'uk_aq_ops.backup_candidates'::regclass
  ) then
    alter table uk_aq_ops.backup_candidates
      add constraint backup_candidates_resume_nonnegative_check
      check (
        resume_part_index >= 0
        and resume_exported_row_count >= 0
      );
  end if;
end
$$;
