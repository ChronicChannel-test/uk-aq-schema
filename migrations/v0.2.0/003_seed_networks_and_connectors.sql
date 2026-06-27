-- UK AQ v0.2.0 additive TEST migration: networks and connectors.

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
  ('laqn', 'LAQN', true, false, 50, '{}'::jsonb)
on conflict (network_code) do update
set
  display_name = excluded.display_name,
  ingest_enabled = excluded.ingest_enabled,
  public_display_enabled = excluded.public_display_enabled,
  default_priority = excluded.default_priority,
  updated_at = now();

do $$
begin
  if exists (
    select 1 from connectors where connector_code = 'breathelondon'
  ) and exists (
    select 1 from connectors where connector_code = 'blondon_communities'
  ) then
    raise exception
      'Both breathelondon and blondon_communities connectors exist; resolve before rename';
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
    (
      'uk_air_sos',
      'UK-AIR SOS',
      'GOV.UK AURN',
      'https://uk-air.defra.gov.uk/sos-ukair/api/v1',
      'gov_uk_aurn',
      60,
      6,
      'supabase_function'
    ),
    (
      'blondon_communities',
      'Breathe London Communities',
      'Breathe London Communities',
      'https://api.breathelondon-communities.org/api',
      'breathelondon',
      60,
      6,
      'google_cloud_run'
    ),
    (
      'blondon_nodes',
      'Breathe London Nodes',
      'Breathe London Nodes',
      'https://breathe-london-7x54d7qf.ew.gateway.dev',
      'breathelondon',
      60,
      6,
      'google_cloud_run'
    ),
    (
      'openaq',
      'OpenAQ',
      'OpenAQ',
      'https://api.openaq.org/v3',
      'openaq',
      60,
      6,
      'supabase_function'
    ),
    (
      'sensorcommunity',
      'Sensor.Community',
      'Sensor.Community',
      'https://data.sensor.community',
      'sensorcommunity',
      60,
      6,
      'supabase_function'
    )
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
  poll_enabled = excluded.poll_enabled,
  poll_interval_minutes = excluded.poll_interval_minutes,
  poll_window_hours = excluded.poll_window_hours,
  scheduler_backend = excluded.scheduler_backend,
  updated_at = now();
