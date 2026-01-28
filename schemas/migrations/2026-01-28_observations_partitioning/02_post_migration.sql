-- 02_post_migration.sql
-- Recreate indexes, policies, grants, and views after swapping observations.

set search_path = uk_aq_core, public;

-- Ensure connector partitions exist (in case connectors were added during migration).
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
      'create table if not exists uk_aq_core.%I partition of uk_aq_core.observations for values in (%L);',
      partition_name,
      r.id
    );
  end loop;
end $$;

-- Optional rename of the default partition if it still has the _new name.
do $$
begin
  if to_regclass('uk_aq_core.observations_default') is null
     and to_regclass('uk_aq_core.observations_new_default') is not null then
    execute 'alter table uk_aq_core.observations_new_default rename to observations_default';
  end if;
end $$;

-- Reattach trigger to the swapped table.
drop trigger if exists observations_set_connector_id on uk_aq_core.observations;
create trigger observations_set_connector_id
before insert or update on uk_aq_core.observations
for each row execute function uk_aq_core.observations_set_connector_id();

-- Auto-create a partition when a new connector is inserted.
create or replace function uk_aq_core.observations_create_partition_for_connector()
returns trigger
language plpgsql
set search_path = uk_aq_core, public, pg_catalog
as $$
declare
  partition_name text;
begin
  partition_name := format('observations_new_c_%s', replace(new.id::text, '-', '_'));
  execute format(
    'create table if not exists uk_aq_core.%I partition of uk_aq_core.observations for values in (%L);',
    partition_name,
    new.id
  );
  return new;
end;
$$;

drop trigger if exists connectors_create_observations_partition on uk_aq_core.connectors;
create trigger connectors_create_observations_partition
after insert on uk_aq_core.connectors
for each row execute function uk_aq_core.observations_create_partition_for_connector();

-- Recreate indexes (drop old name if it was attached to observations_old).
drop index if exists uk_aq_core.observations_time_idx;
create index if not exists observations_time_idx
  on uk_aq_core.observations(observed_at);

-- RLS and policies.
alter table uk_aq_core.observations enable row level security;

do $$
begin
  if not exists (
    select 1 from pg_policies p
    where p.schemaname = 'uk_aq_core'
      and p.tablename = 'observations'
      and p.policyname = 'observations_select_authenticated'
  ) then
    execute 'create policy observations_select_authenticated on uk_aq_core.observations for select using (auth.role() in (''authenticated'',''service_role''));';
  end if;
  if not exists (
    select 1 from pg_policies p
    where p.schemaname = 'uk_aq_core'
      and p.tablename = 'observations'
      and p.policyname = 'observations_write_service_role'
  ) then
    execute 'create policy observations_write_service_role on uk_aq_core.observations for all using (auth.role() = ''service_role'') with check (auth.role() = ''service_role'');';
  end if;
end $$;

-- Enable RLS on partitions and grant service_role access.
do $$
declare
  r record;
begin
  for r in
    select inhrelid::regclass as child
    from pg_inherits
    join pg_class parent on parent.oid = pg_inherits.inhparent
    join pg_namespace nsp on nsp.oid = parent.relnamespace
    where nsp.nspname = 'uk_aq_core'
      and parent.relname = 'observations'
  loop
    execute format('alter table %s enable row level security', r.child);
    execute format('grant all on table %s to service_role', r.child);
  end loop;
end $$;

grant all on uk_aq_core.observations to service_role;

-- Recreate views that reference observations so they bind to the new table.
set search_path = uk_aq_public, public;

create or replace view observations as
select timeseries_id, observed_at, value, status, created_at
from uk_aq_core.observations;

create or replace view bristol_latest_pollutants as
with target_service as (
  select id
  from uk_aq_core.connectors
  where lower(label) like '%uk%' and lower(label) like '%air%'
  order by created_at asc
  limit 1
),
bristol_stations as (
  select stn.*
  from uk_aq_core.stations stn, target_service ts
  where stn.connector_id = ts.id
    and stn.geometry && ST_MakeEnvelope(-2.75, 51.30, -2.45, 51.55, 4326)
),
latest as (
  select distinct on (obs.timeseries_id) obs.timeseries_id, obs.observed_at, obs.value, obs.status
  from uk_aq_core.observations obs
  order by obs.timeseries_id, obs.observed_at desc
)
select
  ts.id as timeseries_id,
  stn.id as station_id,
  stn.label as station_label,
  phen.id as phenomenon_id,
  phen.label as pollutant,
  ts.uom,
  latest.value as latest_value,
  latest.observed_at as observed_at,
  latest.status as status_flag,
  ts.last_value_at,
  ts.last_value,
  stn.geometry,
  coalesce(
    th.color,
    '#9ca3af'
  ) as color,
  ts.rendering_hints,
  ts.status_intervals,
  (ts.last_value_at is null or ts.last_value_at < now() - interval '3 hours') as is_stale
from uk_aq_core.timeseries ts
join bristol_stations stn
  on ts.station_id = stn.id
left join latest on latest.timeseries_id = ts.id
left join uk_aq_core.phenomena phen on phen.id = ts.phenomenon_id
left join uk_aq_core.pollutant_thresholds th
  on lower(phen.label) = th.pollutant
  and (
    (th.upper_value is null and latest.value is not null and latest.value >= th.lower_value) or
    (latest.value between th.lower_value and th.upper_value)
  );

alter view if exists observations set (security_invoker = true);
alter view if exists bristol_latest_pollutants set (security_invoker = true);
