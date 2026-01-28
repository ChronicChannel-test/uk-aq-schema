-- 01_migration.sql
-- Partition uk_aq_core.observations by connector_id.
-- Note: In this repo, connector_id is bigint (matches uk_aq_core.timeseries.connector_id).
-- If your target DB uses uuid for connector_id, change bigint -> uuid in this script.

set search_path = uk_aq_core, public;

-- Enforce connector_id consistency with timeseries.connector_id.
create or replace function uk_aq_core.observations_set_connector_id()
returns trigger
language plpgsql
set search_path = uk_aq_core, public, pg_catalog
as $$
declare
  v_connector_id bigint;
begin
  select connector_id into v_connector_id
  from uk_aq_core.timeseries
  where id = new.timeseries_id;

  if v_connector_id is null then
    raise exception 'timeseries_id % does not exist', new.timeseries_id;
  end if;

  if new.connector_id is null then
    new.connector_id := v_connector_id;
  elsif new.connector_id <> v_connector_id then
    raise exception 'observations.connector_id (%) does not match timeseries.connector_id (%) for timeseries_id %',
      new.connector_id, v_connector_id, new.timeseries_id;
  end if;

  return new;
end;
$$;

-- New partitioned table.
create table if not exists uk_aq_core.observations_new (
  connector_id bigint not null references uk_aq_core.connectors(id) on delete cascade,
  timeseries_id bigint references uk_aq_core.timeseries(id) on delete cascade,
  observed_at timestamptz not null,
  value numeric,
  status text,
  created_at timestamptz default now(),
  primary key (connector_id, timeseries_id, observed_at)
) partition by list (connector_id);

create table if not exists uk_aq_core.observations_new_default
  partition of uk_aq_core.observations_new default;

drop trigger if exists observations_set_connector_id on uk_aq_core.observations_new;
create trigger observations_set_connector_id
before insert or update on uk_aq_core.observations_new
for each row execute function uk_aq_core.observations_set_connector_id();

-- Create partitions for all existing connectors.
do $$
declare
  r record;
  partition_name text;
begin
  for r in
    select id from uk_aq_core.connectors
  loop
    partition_name := format('observations_new_c_%s', replace(r.id::text, '-', '_'));
    execute format(
      'create table if not exists uk_aq_core.%I partition of uk_aq_core.observations_new for values in (%L);',
      partition_name,
      r.id
    );
  end loop;
end $$;

-- Backfill with connector_id sourced from timeseries.
insert into uk_aq_core.observations_new (
  connector_id,
  timeseries_id,
  observed_at,
  value,
  status,
  created_at
)
select
  ts.connector_id,
  obs.timeseries_id,
  obs.observed_at,
  obs.value,
  obs.status,
  obs.created_at
from uk_aq_core.observations obs
join uk_aq_core.timeseries ts on ts.id = obs.timeseries_id
on conflict do nothing;

-- Validate counts and primary key uniqueness.
do $$
declare
  old_count bigint;
  new_count bigint;
  distinct_count bigint;
begin
  select count(*) into old_count from uk_aq_core.observations;
  select count(*) into new_count from uk_aq_core.observations_new;
  select count(*) into distinct_count
  from (
    select connector_id, timeseries_id, observed_at
    from uk_aq_core.observations_new
    group by connector_id, timeseries_id, observed_at
  ) s;

  if new_count <> old_count then
    raise exception 'observations_new row count % does not match observations %', new_count, old_count;
  end if;

  if distinct_count <> new_count then
    raise exception 'observations_new contains duplicate primary keys';
  end if;
end $$;

-- Swap tables.
do $$
begin
  if to_regclass('uk_aq_core.observations') is not null
     and to_regclass('uk_aq_core.observations_new') is not null
     and to_regclass('uk_aq_core.observations_old') is null then
    execute 'alter table uk_aq_core.observations rename to observations_old';
    execute 'alter table uk_aq_core.observations_new rename to observations';
  end if;
end $$;
