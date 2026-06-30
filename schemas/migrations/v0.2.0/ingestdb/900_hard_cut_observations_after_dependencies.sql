-- ============================================================================
-- DANGER: DEFERRED HARD CUT - NOT PART OF THE DEFAULT MIGRATION SEQUENCE
-- ============================================================================
--
-- DO NOT RUN until ALL of the following are complete:
-- - RPCs and public views use the v0.2.0 observation key
-- - AQI functions and jobs no longer require observations.connector_id
-- - ingest scripts write the v0.2.0 observation shape
-- - replacement-table permissions, grants, ownership and RLS are approved
-- - all dependency reports have been reviewed and cleared
-- - observation writes are paused for a maintenance window
--
-- This script takes an ACCESS EXCLUSIVE lock and must run with writes paused.
-- It is intentionally non-rerunnable after a successful swap. Test rollback
-- and recovery procedures before using it outside a disposable TEST database.
-- ============================================================================
--
-- UK AQ v0.2.0 deferred TEST migration: observations hard cut only.
--
-- Safety: the old observations table is renamed to observations_legacy_v020.
-- It is not dropped. This file does not remove timeseries legacy columns,
-- station_metadata or phenomena.

begin;

set search_path = uk_aq_core, public, pg_catalog;

lock table uk_aq_core.observations in access exclusive mode;
lock table uk_aq_core.timeseries in share mode;

-- Blocking preconditions. No row is filtered or silently deduplicated.
do $$
begin
  if to_regclass('uk_aq_core.observations_legacy_v020') is not null then
    raise exception 'observations_legacy_v020 already exists; investigate the previous hard-cut attempt before continuing';
  end if;

  if to_regclass('uk_aq_core.observations_v020') is not null then
    raise exception 'observations_v020 already exists; investigate the previous hard-cut attempt before continuing';
  end if;

  if exists (
    select 1 from observations where timeseries_id is null
  ) then
    raise exception 'Cannot rebuild observations: null timeseries_id rows exist';
  end if;

  if exists (
    select 1
    from observations o
    left join timeseries t on t.id = o.timeseries_id
    where t.id is null
  ) then
    raise exception 'Cannot rebuild observations: orphan timeseries_id rows exist';
  end if;

  if exists (
    select 1
    from observations
    group by timeseries_id, observed_at
    having count(*) > 1
  ) then
    raise exception 'Cannot rebuild observations: rows collide under (timeseries_id, observed_at)';
  end if;
end
$$;

do $$
declare
  has_status boolean;
  has_metadata boolean;
  has_created_at boolean;
  source_count bigint;
  target_count bigint;
  insert_sql text;
begin
  select exists (
    select 1 from information_schema.columns
    where table_schema = 'uk_aq_core'
      and table_name = 'observations'
      and column_name = 'status'
  ) into has_status;

  select exists (
    select 1 from information_schema.columns
    where table_schema = 'uk_aq_core'
      and table_name = 'observations'
      and column_name = 'metadata'
  ) into has_metadata;

  select exists (
    select 1 from information_schema.columns
    where table_schema = 'uk_aq_core'
      and table_name = 'observations'
      and column_name = 'created_at'
  ) into has_created_at;

  select count(*) into source_count from observations;

  create table observations_v020 (
    timeseries_id integer not null references timeseries(id) on delete cascade,
    observed_at timestamptz not null,
    value double precision,
    status text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    primary key (timeseries_id, observed_at)
  );

  insert_sql := format(
    'insert into observations_v020 (timeseries_id, observed_at, value, status, metadata, created_at)
     select
       timeseries_id,
       observed_at,
       value,
       %s as status,
       %s as metadata,
       %s as created_at
     from observations',
    case when has_status then 'status' else 'null::text' end,
    case when has_metadata then 'coalesce(metadata, ''{}''::jsonb)' else '''{}''::jsonb' end,
    case when has_created_at then 'coalesce(created_at, now())' else 'now()' end
  );

  execute insert_sql;

  select count(*) into target_count from observations_v020;
  if target_count <> source_count then
    raise exception
      'Observation copy count mismatch: source %, target %',
      source_count,
      target_count;
  end if;

  alter table observations rename to observations_legacy_v020;
  alter table observations_v020 rename to observations;
end
$$;

create index if not exists observations_observed_at_idx
  on observations(observed_at);

-- The primary key supports timeseries/time scans in either direction. Do not
-- reuse an old index name that remains attached to observations_legacy_v020.

-- A replacement table does not inherit the old table's RLS state, policies,
-- grants, owner, comments or triggers. The approved hard-cut implementation
-- must recreate and verify all of them here before COMMIT. This draft stops
-- deliberately so it cannot be mistaken for an executable production cut.
do $$
begin
  raise exception
    'DEFERRED DRAFT STOP: add and verify approved observations owner, RLS policies, grants, comments and triggers before enabling this hard cut';
end
$$;

commit;
