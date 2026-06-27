-- UK AQ v0.2.0 additive TEST migration: station fields and initial metadata.

set search_path = uk_aq_core, public, pg_catalog;

update stations s
set network_id = c.default_network_id
from connectors c
where c.id = s.connector_id
  and s.network_id is null
  and c.default_network_id is not null;

update stations s
set priority = n.default_priority
from networks n
where n.id = s.network_id
  and s.priority = 100
  and n.default_priority is distinct from 100;

update stations
set
  longitude = st_x(geometry::geometry),
  latitude = st_y(geometry::geometry)
where geometry is not null
  and (longitude is null or latitude is null);

update stations s
set station_device_ref = coalesce(
  nullif(sm.attributes ->> 'InstallationCode', ''),
  nullif(sm.attributes ->> 'installation_code', ''),
  nullif(sm.attributes ->> 'station_device_ref', '')
)
from station_metadata sm
where sm.station_id = s.id
  and s.station_device_ref is null
  and coalesce(
    nullif(sm.attributes ->> 'InstallationCode', ''),
    nullif(sm.attributes ->> 'installation_code', ''),
    nullif(sm.attributes ->> 'station_device_ref', '')
  ) is not null;

insert into station_initial_metadata (
  station_id,
  attributes,
  captured_at,
  created_at
)
select
  s.id,
  coalesce(sm.attributes, '{}'::jsonb),
  coalesce(sm.created_at, s.created_at, now()),
  coalesce(sm.created_at, s.created_at, now())
from stations s
left join station_metadata sm
  on sm.station_id = s.id
on conflict (station_id) do nothing;

alter table stations
  alter column service_ref drop not null,
  alter column service_ref set default 'default';

-- New ingest contract:
-- insert into uk_aq_core.station_initial_metadata(station_id, attributes)
-- values (<station_id>, <attributes>)
-- on conflict (station_id) do nothing;

-- network_id remains nullable during the additive TEST phase. File 009 lists
-- every exception before the canonical NOT NULL rule is considered.
