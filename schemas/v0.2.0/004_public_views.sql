-- UK AQ v0.2.0 Phase 1 public network and station projections.

create schema if not exists uk_aq_public;
set search_path = uk_aq_public, public, pg_catalog;

create or replace view networks as
select
  n.id as network_id,
  n.network_code,
  n.display_name as network_label,
  n.network_type,
  n.public_display_enabled,
  n.default_priority
from uk_aq_core.networks n
where n.public_display_enabled is true;

create or replace view stations as
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

create or replace view uk_aq_station_lat_lon as
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

alter view networks set (security_invoker = true);
alter view stations set (security_invoker = true);
alter view uk_aq_station_lat_lon set (security_invoker = true);

grant usage on schema uk_aq_public to authenticated, service_role;
grant select on networks, stations, uk_aq_station_lat_lon
  to authenticated, service_role;
