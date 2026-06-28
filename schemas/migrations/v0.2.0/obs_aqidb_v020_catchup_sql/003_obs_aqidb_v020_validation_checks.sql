-- Obs AQI DB v0.2.0 catch-up validation checks.

-- Connector/network sanity.
select
  c.id,
  c.connector_code,
  c.label,
  c.display_name,
  c.default_network_id,
  n.network_code,
  n.display_name as network_display_name
from uk_aq_core.connectors c
left join uk_aq_core.networks n on n.id = c.default_network_id
where c.connector_code in ('uk_air_sos', 'blondon_nodes', 'blondon_communities', 'openaq', 'sensorcommunity', 'breathelondon')
order by c.id;

-- Stale connector must be zero.
select count(*) as stale_breathelondon_connectors
from uk_aq_core.connectors
where connector_code = 'breathelondon';

-- Station/network backfill summary.
select
  c.connector_code,
  count(*) as stations,
  count(*) filter (where s.network_id is null) as missing_network_id,
  count(*) filter (where s.removed_at is null) as current_stations,
  count(*) filter (where s.removed_at is not null) as removed_stations
from uk_aq_core.stations s
join uk_aq_core.connectors c on c.id = s.connector_id
where c.connector_code in ('uk_air_sos', 'blondon_nodes', 'blondon_communities', 'openaq', 'sensorcommunity')
group by c.connector_code
order by c.connector_code;

-- Columns expected by v0.2.0 mirror/reference shape.
select
  table_name,
  column_name
from information_schema.columns
where table_schema = 'uk_aq_core'
  and (
    (table_name = 'connectors' and column_name in ('default_network_id', 'config', 'metadata', 'updated_at'))
    or (table_name = 'stations' and column_name in ('network_id', 'match_id', 'priority', 'station_device_ref', 'latitude', 'longitude', 'updated_at'))
    or (table_name = 'timeseries' and column_name in ('observed_property_id', 'status', 'metadata'))
    or (table_name = 'observed_properties' and column_name in ('display_order', 'metadata'))
  )
order by table_name, column_name;
