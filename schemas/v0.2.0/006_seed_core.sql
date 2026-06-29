-- UK AQ v0.2.0 canonical core seed data.
-- Apply after 001_core_schema.sql on a clean v0.2.0 database.

set search_path = uk_aq_core, public, pg_catalog;

insert into networks (
  network_code,
  display_name,
  network_type,
  ingest_enabled,
  public_display_enabled,
  default_priority,
  metadata
)
values
  ('gov_uk_aurn', 'GOV.UK AURN', 'official', true, true, 10, '{}'::jsonb),
  ('breathelondon', 'Breathe London', 'community', true, true, 20, '{}'::jsonb),
  ('openaq', 'OpenAQ', 'aggregator', true, false, 100, '{}'::jsonb),
  ('sensorcommunity', 'Sensor.Community', 'community', true, false, 100, '{}'::jsonb),
  ('laqn', 'LAQN', 'official', true, false, 50, '{}'::jsonb)
on conflict (network_code) do update
set
  display_name = excluded.display_name,
  network_type = excluded.network_type,
  ingest_enabled = excluded.ingest_enabled,
  public_display_enabled = excluded.public_display_enabled,
  default_priority = excluded.default_priority,
  updated_at = now();

insert into connectors (
  connector_code,
  label,
  display_name,
  service_url,
  default_network_id,
  station_display_name_template,
  overwrite_station_name,
  poll_enabled,
  poll_interval_minutes,
  poll_window_hours,
  poll_timeseries_batch_size,
  scheduler_backend,
  stations_bbox_supported,
  timeseries_station_filter_supported,
  config,
  metadata
)
select
  seed.connector_code,
  seed.label,
  seed.display_name,
  seed.service_url,
  n.id,
  seed.station_display_name_template,
  seed.overwrite_station_name,
  true,
  seed.poll_interval_minutes,
  seed.poll_window_hours,
  seed.poll_timeseries_batch_size,
  seed.scheduler_backend,
  seed.stations_bbox_supported,
  seed.timeseries_station_filter_supported,
  '{}'::jsonb,
  '{}'::jsonb
from (
  values
    (
      'uk_air_sos',
      'UK-AIR SOS',
      'UK-AIR SOS',
      'https://uk-air.defra.gov.uk/sos-ukair/api/v1',
      'gov_uk_aurn',
      null::text,
      true,
      60,
      6,
      null::integer,
      'google_cloud_run',
      false,
      false
    ),
    (
      'blondon_communities',
      'Breathe London Communities',
      'Breathe London Communities',
      'https://api.breathelondon-communities.org/api',
      'breathelondon',
      null::text,
      true,
      60,
      6,
      null::integer,
      'google_cloud_run',
      true,
      true
    ),
    (
      'blondon_nodes',
      'Breathe London Nodes',
      'Breathe London Nodes',
      'https://breathe-london-7x54d7qf.ew.gateway.dev',
      'breathelondon',
      null::text,
      true,
      60,
      6,
      null::integer,
      'google_cloud_run',
      true,
      true
    ),
    (
      'openaq',
      'OpenAQ',
      'OpenAQ',
      'https://api.openaq.org/v3',
      'openaq',
      null::text,
      true,
      60,
      6,
      null::integer,
      'google_cloud_run',
      true,
      true
    ),
    (
      'sensorcommunity',
      'Sensor.Community',
      'Sensor.Community',
      'https://data.sensor.community',
      'sensorcommunity',
      null::text,
      false,
      60,
      6,
      null::integer,
      'google_cloud_run',
      true,
      true
    )
) as seed(
  connector_code,
  label,
  display_name,
  service_url,
  network_code,
  station_display_name_template,
  overwrite_station_name,
  poll_interval_minutes,
  poll_window_hours,
  poll_timeseries_batch_size,
  scheduler_backend,
  stations_bbox_supported,
  timeseries_station_filter_supported
)
join networks n
  on n.network_code = seed.network_code
on conflict (connector_code) do update
set
  label = excluded.label,
  display_name = excluded.display_name,
  service_url = excluded.service_url,
  default_network_id = excluded.default_network_id,
  station_display_name_template = coalesce(
    connectors.station_display_name_template,
    excluded.station_display_name_template
  ),
  overwrite_station_name = coalesce(
    connectors.overwrite_station_name,
    excluded.overwrite_station_name
  ),
  poll_enabled = coalesce(connectors.poll_enabled, excluded.poll_enabled),
  poll_interval_minutes = coalesce(
    connectors.poll_interval_minutes,
    excluded.poll_interval_minutes
  ),
  poll_window_hours = coalesce(
    connectors.poll_window_hours,
    excluded.poll_window_hours
  ),
  poll_timeseries_batch_size = coalesce(
    connectors.poll_timeseries_batch_size,
    excluded.poll_timeseries_batch_size
  ),
  scheduler_backend = coalesce(
    connectors.scheduler_backend,
    excluded.scheduler_backend
  ),
  stations_bbox_supported = coalesce(
    connectors.stations_bbox_supported,
    excluded.stations_bbox_supported
  ),
  timeseries_station_filter_supported = coalesce(
    connectors.timeseries_station_filter_supported,
    excluded.timeseries_station_filter_supported
  ),
  config = coalesce(connectors.config, excluded.config),
  metadata = coalesce(connectors.metadata, excluded.metadata),
  updated_at = now();

insert into observed_properties (
  code,
  display_name,
  domain,
  canonical_uom,
  display_order,
  metadata
)
values
  ('no2', 'Nitrogen dioxide', 'aq', 'ug/m3', 10, '{}'::jsonb),
  ('pm25', 'PM2.5', 'aq', 'ug/m3', 20, '{}'::jsonb),
  ('pm10', 'PM10', 'aq', 'ug/m3', 30, '{}'::jsonb),
  ('o3', 'Ozone', 'aq', 'ug/m3', 40, '{}'::jsonb),
  ('so2', 'Sulphur dioxide', 'aq', 'ug/m3', 50, '{}'::jsonb),
  ('co', 'Carbon monoxide', 'aq', 'mg/m3', 60, '{}'::jsonb),
  ('temperature', 'Temperature', 'met', 'degC', 100, '{}'::jsonb),
  ('humidity', 'Relative humidity', 'met', '%', 110, '{}'::jsonb),
  ('pressure', 'Air pressure', 'met', 'hPa', 120, '{}'::jsonb)
on conflict (code) do update
set
  display_name = excluded.display_name,
  domain = excluded.domain,
  canonical_uom = excluded.canonical_uom,
  display_order = excluded.display_order,
  updated_at = now();
