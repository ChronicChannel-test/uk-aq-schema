-- Obs AQI DB v0.2.0 network and connector seed/catch-up.
-- Run after 001_obs_aqidb_v020_core_additive_catchup.sql.

begin;

set search_path = uk_aq_core, public, pg_catalog;

insert into networks (
  network_code,
  display_name,
  ingest_enabled,
  public_display_enabled,
  default_priority,
  metadata
)
values
  ('gov_uk_aurn', 'GOV.UK AURN', true, true, 10, '{}'::jsonb),
  ('breathelondon', 'Breathe London', true, true, 20, '{}'::jsonb),
  ('openaq', 'OpenAQ', true, false, 100, '{}'::jsonb),
  ('sensorcommunity', 'Sensor.Community', true, false, 100, '{}'::jsonb),
  ('laqn', 'LAQN', false, false, 900, '{}'::jsonb)
on conflict (network_code) do update
set
  display_name = excluded.display_name,
  ingest_enabled = excluded.ingest_enabled,
  public_display_enabled = excluded.public_display_enabled,
  default_priority = excluded.default_priority,
  updated_at = now();

-- Stale compatibility connector is not allowed. If both exist, stop rather than guessing.
do $$
begin
  if exists (select 1 from connectors where connector_code = 'breathelondon')
     and exists (select 1 from connectors where connector_code = 'blondon_communities') then
    raise exception 'Both breathelondon and blondon_communities connectors exist; resolve before running seed';
  end if;

  update connectors
  set
    connector_code = 'blondon_communities',
    label = 'Breathe London Communities',
    display_name = 'Breathe London Communities',
    service_url = 'https://api.breathelondon-communities.org/api',
    updated_at = now()
  where connector_code = 'breathelondon';
end
$$;

insert into connectors (
  connector_code,
  label,
  display_name,
  service_url,
  default_network_id,
  poll_enabled,
  poll_interval_minutes,
  poll_window_hours,
  scheduler_backend,
  config,
  metadata
)
select
  seed.connector_code,
  seed.label,
  seed.display_name,
  seed.service_url,
  n.id,
  true,
  seed.poll_interval_minutes,
  seed.poll_window_hours,
  seed.scheduler_backend,
  '{}'::jsonb,
  '{}'::jsonb
from (
  values
    ('uk_air_sos', 'UK-AIR SOS', 'GOV.UK AURN', 'https://uk-air.defra.gov.uk/sos-ukair/api/v1', 'gov_uk_aurn', 1, 6, 'google_cloud_run'),
    ('blondon_communities', 'Breathe London Communities', 'Breathe London Communities', 'https://api.breathelondon-communities.org/api', 'breathelondon', 1, 6, 'google_cloud_run'),
    ('blondon_nodes', 'Breathe London Nodes', 'Breathe London Nodes', 'https://breathe-london-7x54d7qf.ew.gateway.dev', 'breathelondon', 1, 6, 'google_cloud_run'),
    ('openaq', 'OpenAQ', 'OpenAQ', 'https://api.openaq.org/v3', 'openaq', 1, 6, 'supabase_function'),
    ('sensorcommunity', 'Sensor.Community', 'Sensor.Community', 'https://data.sensor.community', 'sensorcommunity', 5, 6, 'google_cloud_run')
) as seed(
  connector_code,
  label,
  display_name,
  service_url,
  network_code,
  poll_interval_minutes,
  poll_window_hours,
  scheduler_backend
)
join networks n
  on n.network_code = seed.network_code
on conflict (connector_code) do update
set
  label = excluded.label,
  display_name = excluded.display_name,
  service_url = excluded.service_url,
  default_network_id = excluded.default_network_id,
  -- Preserve manual poll_enabled state on existing destination rows.
  poll_interval_minutes = excluded.poll_interval_minutes,
  poll_window_hours = excluded.poll_window_hours,
  scheduler_backend = excluded.scheduler_backend,
  config = coalesce(connectors.config, '{}'::jsonb),
  metadata = coalesce(connectors.metadata, '{}'::jsonb),
  updated_at = now();

-- Backfill stations.network_id from their connector default network.
update stations s
set network_id = c.default_network_id,
    updated_at = now()
from connectors c
where s.connector_id = c.id
  and s.network_id is null
  and c.default_network_id is not null;

commit;
