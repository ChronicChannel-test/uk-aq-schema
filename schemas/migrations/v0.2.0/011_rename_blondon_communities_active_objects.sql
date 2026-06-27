-- UK AQ v0.2.0 TEST patch: make Breathe London Communities object names explicit.
--
-- This migration preserves checkpoint rows by renaming existing tables. It
-- does not change networks.network_code or station/timeseries service_ref,
-- which remain breathelondon.

begin;

do $$
begin
  if to_regclass('uk_aq_raw.breathelondon_station_checkpoints') is not null
     and to_regclass('uk_aq_raw.blondon_communities_station_checkpoints') is not null then
    raise exception
      'Both old and new Communities station checkpoint tables exist; reconcile them before applying this migration.';
  end if;

  if to_regclass('uk_aq_raw.breathelondon_timeseries_checkpoints') is not null
     and to_regclass('uk_aq_raw.blondon_communities_timeseries_checkpoints') is not null then
    raise exception
      'Both old and new Communities timeseries checkpoint tables exist; reconcile them before applying this migration.';
  end if;

  if to_regprocedure('uk_aq_core.breathelondon_select_station_refs(integer,integer)') is not null
     and to_regprocedure('uk_aq_core.blondon_communities_select_station_refs(integer,integer)') is not null then
    raise exception
      'Both old and new Communities station selector RPCs exist; reconcile them before applying this migration.';
  end if;
end
$$;

do $$
begin
  drop function if exists uk_aq_core.breathelondon_select_station_refs(integer, boolean);

  if to_regclass('uk_aq_raw.breathelondon_station_checkpoints') is not null then
    alter table uk_aq_raw.breathelondon_station_checkpoints
      rename to blondon_communities_station_checkpoints;
  end if;

  if to_regclass('uk_aq_raw.breathelondon_timeseries_checkpoints') is not null then
    alter table uk_aq_raw.breathelondon_timeseries_checkpoints
      rename to blondon_communities_timeseries_checkpoints;
  end if;

  if to_regclass('uk_aq_raw.breathelondon_station_checkpoints_next_due_at_idx') is not null then
    alter index uk_aq_raw.breathelondon_station_checkpoints_next_due_at_idx
      rename to blondon_communities_station_checkpoints_next_due_at_idx;
  end if;

  if to_regclass('uk_aq_raw.breathelondon_station_checkpoints_last_polled_at_idx') is not null then
    alter index uk_aq_raw.breathelondon_station_checkpoints_last_polled_at_idx
      rename to blondon_communities_station_checkpoints_last_polled_at_idx;
  end if;

  if to_regclass('uk_aq_raw.breathelondon_timeseries_checkpoints_last_obs_idx') is not null then
    alter index uk_aq_raw.breathelondon_timeseries_checkpoints_last_obs_idx
      rename to blondon_communities_timeseries_checkpoints_last_obs_idx;
  end if;

  if to_regprocedure('uk_aq_core.breathelondon_select_station_refs(integer,integer)') is not null then
    alter function uk_aq_core.breathelondon_select_station_refs(integer, integer)
      rename to blondon_communities_select_station_refs;
  end if;
end
$$;

set local search_path = uk_aq_raw, uk_aq_core, public;

create table if not exists blondon_communities_timeseries_checkpoints (
  station_id bigint not null references stations(id) on delete cascade,
  species text not null,
  timeseries_id bigint references timeseries(id) on delete set null,
  last_observed_at timestamptz,
  last_polled_at timestamptz,
  last_error text,
  created_at timestamptz default now(),
  updated_at timestamptz default now(),
  primary key (station_id, species)
);

create index if not exists blondon_communities_timeseries_checkpoints_last_obs_idx
  on blondon_communities_timeseries_checkpoints(last_observed_at);

create table if not exists blondon_communities_station_checkpoints (
  station_id bigint primary key references stations(id) on delete cascade,
  next_due_at timestamptz,
  last_observed_at timestamptz,
  ingest_lag_samples int[] not null default '{}'::int[],
  last_polled_at timestamptz,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create index if not exists blondon_communities_station_checkpoints_next_due_at_idx
  on blondon_communities_station_checkpoints(next_due_at);
create index if not exists blondon_communities_station_checkpoints_last_polled_at_idx
  on blondon_communities_station_checkpoints(last_polled_at);

create or replace function uk_aq_core.blondon_communities_select_station_refs(
  batch_limit integer default 10,
  stale_limit integer default 4
)
returns text[]
language plpgsql
set search_path = uk_aq_core, uk_aq_raw, public, pg_catalog
as $$
declare
  v_connector_id integer;
  station_refs text[];
begin
  select id into v_connector_id
  from connectors
  where connector_code = 'blondon_communities'
  limit 1;

  if v_connector_id is null then
    return null;
  end if;

  with latest_obs as (
    select
      t.station_id,
      max(t.last_value_at) as last_observed_at
    from timeseries t
    where t.connector_id = v_connector_id
      and t.service_ref = 'breathelondon'
    group by t.station_id
  ),
  candidates as (
    select
      stn.id as station_id,
      stn.station_ref,
      bsc.next_due_at,
      bsc.last_polled_at,
      coalesce(bsc.last_observed_at, lo.last_observed_at) as last_observed_at,
      coalesce(bsc.next_due_at, now()) as due_at
    from stations stn
    left join blondon_communities_station_checkpoints bsc
      on bsc.station_id = stn.id
    left join latest_obs lo
      on lo.station_id = stn.id
    left join station_metadata sm
      on sm.station_id = stn.id
    where stn.connector_id = v_connector_id
      and stn.service_ref = 'breathelondon'
      and stn.station_ref is not null
      and stn.removed_at is null
      and (
        lower(coalesce(sm.attributes->>'enabled', '')) in ('y','yes','true','1')
        or lower(coalesce(sm.attributes->>'site_active', '')) in ('y','yes','true','1')
      )
  ),
  tiered as (
    select station_id, station_ref, due_at, last_polled_at
    from candidates
    where due_at <= now()
      and due_at >= now() - interval '3 hours'
      and (last_polled_at is null or last_polled_at <= now() - interval '5 minutes')
    union all
    select station_id, station_ref, due_at, last_polled_at
    from candidates
    where due_at < now() - interval '3 hours'
      and due_at >= now() - interval '24 hours'
      and (last_polled_at is null or last_polled_at <= now() - interval '1 hour')
  ),
  tiered_limited as (
    select *
    from tiered
    order by last_polled_at asc nulls first, due_at asc
    limit batch_limit
  ),
  stale as (
    select c.station_id, c.station_ref, c.last_observed_at
    from candidates c
    where c.due_at <= now()
      and (c.last_observed_at is null or c.last_observed_at <= now() - interval '24 hours')
      and (c.last_polled_at is null or c.last_polled_at <= now() - interval '12 hours')
      and not exists (
        select 1 from tiered_limited t where t.station_id = c.station_id
      )
    order by c.last_observed_at nulls first
    limit stale_limit
  ),
  combined as (
    select station_ref, 1 as group_order, due_at as sort_at
    from tiered_limited
    union all
    select station_ref, 2 as group_order, null as sort_at
    from stale
  )
  select array_agg(combined.station_ref order by group_order, sort_at nulls last)
  into station_refs
  from combined;

  return station_refs;
end;
$$;

alter table uk_aq_raw.blondon_communities_station_checkpoints enable row level security;
alter table uk_aq_raw.blondon_communities_timeseries_checkpoints enable row level security;

do $$
declare
  table_name text;
  old_table_name text;
begin
  foreach old_table_name in array array[
    'breathelondon_station_checkpoints',
    'breathelondon_timeseries_checkpoints'
  ]
  loop
    table_name := replace(
      old_table_name,
      'breathelondon_',
      'blondon_communities_'
    );

    if exists (
      select 1
      from pg_policies
      where schemaname = 'uk_aq_raw'
        and tablename = table_name
        and policyname = old_table_name || '_select_service_role'
    ) then
      execute format(
        'alter policy %I on uk_aq_raw.%I rename to %I',
        old_table_name || '_select_service_role',
        table_name,
        table_name || '_select_service_role'
      );
    end if;

    if exists (
      select 1
      from pg_policies
      where schemaname = 'uk_aq_raw'
        and tablename = table_name
        and policyname = old_table_name || '_write_service_role'
    ) then
      execute format(
        'alter policy %I on uk_aq_raw.%I rename to %I',
        old_table_name || '_write_service_role',
        table_name,
        table_name || '_write_service_role'
      );
    end if;
  end loop;

  foreach table_name in array array[
    'blondon_communities_station_checkpoints',
    'blondon_communities_timeseries_checkpoints'
  ]
  loop
    if not exists (
      select 1
      from pg_policies
      where schemaname = 'uk_aq_raw'
        and tablename = table_name
        and policyname = table_name || '_select_service_role'
    ) then
      execute format(
        'create policy %I on uk_aq_raw.%I for select using ((select auth.role()) = ''service_role'')',
        table_name || '_select_service_role',
        table_name
      );
    end if;

    if not exists (
      select 1
      from pg_policies
      where schemaname = 'uk_aq_raw'
        and tablename = table_name
        and policyname = table_name || '_write_service_role'
    ) then
      execute format(
        'create policy %I on uk_aq_raw.%I for all using ((select auth.role()) = ''service_role'') with check ((select auth.role()) = ''service_role'')',
        table_name || '_write_service_role',
        table_name
      );
    end if;
  end loop;
end
$$;

grant usage on schema uk_aq_raw, uk_aq_core to service_role;
grant all on uk_aq_raw.blondon_communities_station_checkpoints to service_role;
grant all on uk_aq_raw.blondon_communities_timeseries_checkpoints to service_role;
grant execute on function uk_aq_core.blondon_communities_select_station_refs(integer, integer)
  to service_role;

commit;
