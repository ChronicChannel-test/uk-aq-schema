-- UK AQ website/API v2.0.0 hard cut: Phase 1 schema foundations.
-- Keep aligned with schemas/ingest_db/uk_aq_network_public_contract_phase1.sql.

begin;

set local lock_timeout = '10s';
set local statement_timeout = '5min';
set search_path = uk_aq_core, public, pg_catalog;

alter table uk_aq_core.networks
  add column if not exists network_type text;

update uk_aq_core.networks n
set network_type = seed.network_type
from (
  values
    ('gov_uk_aurn'::text, 'official'::text),
    ('breathelondon', 'community'),
    ('openaq', 'aggregator'),
    ('sensorcommunity', 'community'),
    ('laqn', 'official')
) as seed(network_code, network_type)
where n.network_code = seed.network_code
  and n.network_type is distinct from seed.network_type;

do $$
begin
  if exists (
    select 1
    from uk_aq_core.networks
    where network_type is null
       or network_type not in ('official', 'community', 'aggregator')
  ) then
    raise exception
      'Cannot enforce networks.network_type: unmapped or invalid rows exist';
  end if;
end
$$;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conrelid = 'uk_aq_core.networks'::regclass
      and conname = 'networks_network_type_check'
  ) then
    alter table uk_aq_core.networks
      add constraint networks_network_type_check
      check (network_type in ('official', 'community', 'aggregator'))
      not valid;
  end if;
end
$$;

alter table uk_aq_core.networks
  validate constraint networks_network_type_check;

alter table uk_aq_core.networks
  alter column network_type set not null;

update uk_aq_core.stations s
set network_id = c.default_network_id
from uk_aq_core.connectors c
where c.id = s.connector_id
  and s.network_id is null
  and c.default_network_id is not null;

do $$
begin
  if exists (
    select 1
    from uk_aq_core.connectors
    where default_network_id is null
  ) then
    raise exception
      'Cannot enforce station network assignment: connectors without default_network_id exist';
  end if;

  if exists (
    select 1
    from uk_aq_core.stations
    where network_id is null
  ) then
    raise exception
      'Cannot enforce stations.network_id: null values remain';
  end if;
end
$$;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conrelid = 'uk_aq_core.connectors'::regclass
      and conname = 'connectors_default_network_id_fkey'
  ) then
    alter table uk_aq_core.connectors
      add constraint connectors_default_network_id_fkey
      foreign key (default_network_id)
      references uk_aq_core.networks(id)
      on delete set null
      not valid;
  end if;

  if not exists (
    select 1
    from pg_constraint
    where conrelid = 'uk_aq_core.stations'::regclass
      and conname = 'stations_network_id_fkey'
  ) then
    alter table uk_aq_core.stations
      add constraint stations_network_id_fkey
      foreign key (network_id)
      references uk_aq_core.networks(id)
      not valid;
  end if;
end
$$;

alter table uk_aq_core.connectors
  validate constraint connectors_default_network_id_fkey;

alter table uk_aq_core.stations
  validate constraint stations_network_id_fkey;

create or replace function uk_aq_core.uk_aq_assign_station_network_default()
returns trigger
language plpgsql
set search_path = uk_aq_core, public, pg_catalog
as $$
begin
  if new.network_id is null then
    select c.default_network_id
    into new.network_id
    from uk_aq_core.connectors c
    where c.id = new.connector_id;

    if new.network_id is null then
      raise exception
        'Station connector_id % has no default_network_id',
        new.connector_id;
    end if;
  end if;

  return new;
end;
$$;

drop trigger if exists stations_assign_network_default
  on uk_aq_core.stations;

create trigger stations_assign_network_default
before insert or update of connector_id, network_id
on uk_aq_core.stations
for each row
execute function uk_aq_core.uk_aq_assign_station_network_default();

alter table uk_aq_core.stations
  alter column network_id set not null;

create schema if not exists uk_aq_public;

create or replace view uk_aq_public.networks as
select
  n.id as network_id,
  n.network_code,
  n.display_name as network_label,
  n.network_type,
  n.public_display_enabled,
  n.default_priority
from uk_aq_core.networks n
where n.public_display_enabled is true;

drop view if exists uk_aq_public.stations;

create view uk_aq_public.stations as
select
  s.id,
  s.station_ref,
  s.service_ref,
  s.label,
  s.station_name,
  s.station_type,
  s.station_exposure,
  s.region,
  s.la_code,
  s.la_version,
  s.pcon_code,
  s.pcon_version,
  s.geometry,
  s.connector_id,
  s.first_seen_at,
  s.last_seen_at,
  s.removed_at,
  s.created_at,
  s.network_id,
  n.network_code,
  n.display_name as network_label,
  c.connector_code,
  coalesce(c.display_name, c.label) as connector_label
from uk_aq_core.stations s
join uk_aq_core.networks n
  on n.id = s.network_id
 and n.public_display_enabled is true
join uk_aq_core.connectors c
  on c.id = s.connector_id;

create or replace view uk_aq_public.uk_aq_station_lat_lon as
select
  n.display_name as network,
  s.label as station_label,
  s.station_ref,
  concat_ws(
    ' ',
    st_y(s.geometry::geometry),
    st_x(s.geometry::geometry)
  ) as lat_lon
from uk_aq_core.stations s
join uk_aq_core.networks n
  on n.id = s.network_id
 and n.public_display_enabled is true
where s.geometry is not null;

alter view uk_aq_public.networks set (security_invoker = true);
alter view uk_aq_public.stations set (security_invoker = true);
alter view uk_aq_public.uk_aq_station_lat_lon
  set (security_invoker = true);

grant usage on schema uk_aq_public to authenticated, service_role;
grant select on
  uk_aq_public.networks,
  uk_aq_public.stations,
  uk_aq_public.uk_aq_station_lat_lon
to authenticated, service_role;

notify pgrst, 'reload schema';

commit;
