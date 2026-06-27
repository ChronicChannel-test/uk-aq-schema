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
  nullif(sm.attributes ->> 'device_code', ''),
  nullif(sm.attributes ->> 'DeviceCode', ''),
  nullif(sm.attributes ->> 'station_device_ref', '')
)
from station_metadata sm
where sm.station_id = s.id
  and s.station_device_ref is null
  and coalesce(
    nullif(sm.attributes ->> 'device_code', ''),
    nullif(sm.attributes ->> 'DeviceCode', ''),
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
  sm.attributes,
  coalesce(sm.created_at, s.created_at, now()),
  coalesce(sm.created_at, s.created_at, now())
from station_metadata sm
join stations s
  on s.id = sm.station_id
on conflict (station_id) do nothing;

-- The complete legacy attributes object is preserved above, including
-- InstallationCode where supplied by Breathe London Nodes. InstallationCode
-- is the Nodes-to-Communities matching bridge; it is not a device code.
--
-- TODO(v0.2.0 matching phase): when a Nodes InstallationCode matches the
-- Communities station_ref, create/reuse:
--   station_matches.match_key =
--     'blondon_installation:' || <InstallationCode>
-- and set both stations.match_id to that station_matches row.

alter table stations
  alter column service_ref drop not null,
  alter column service_ref set default 'default';

-- New ingest contract:
-- insert into uk_aq_core.station_initial_metadata(station_id, attributes)
-- values (<station_id>, <attributes>)
-- on conflict (station_id) do nothing;

-- Final NOT NULL rules are deferred to the 900-series migrations. Run 005b
-- and 009 to inspect exceptions before considering enforcement.
