-- UK AQ v0.2.0 phase 1 additive TEST migration:
-- backfill direct observed-property links where a verified mapping exists.
--
-- This file deliberately does not enforce NOT NULL and does not remove
-- phenomenon_id. Run 005b next to inspect every unmapped timeseries.

set search_path = uk_aq_core, public, pg_catalog;

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

update timeseries t
set
  observed_property_id = p.observed_property_id,
  metadata = coalesce(t.metadata, '{}'::jsonb)
    || jsonb_strip_nulls(jsonb_build_object(
      'legacy_extras', t.extras,
      'legacy_rendering_hints', t.rendering_hints,
      'legacy_status_intervals', t.status_intervals,
      'source_phenomenon', jsonb_build_object(
        'id', p.id,
        'label', p.label,
        'source_label', p.source_label,
        'notation', p.notation,
        'pollutant_label', p.pollutant_label
      )
    ))
from phenomena p
where p.id = t.phenomenon_id
  and p.observed_property_id is not null
  and (
    t.observed_property_id is distinct from p.observed_property_id
    or not (coalesce(t.metadata, '{}'::jsonb) ? 'source_phenomenon')
  );

alter table timeseries
  alter column service_ref drop not null,
  alter column service_ref set default 'default';

-- Source-specific phenomenon/species/parameter details are retained in
-- timeseries.metadata.source_phenomenon. The legacy phenomenon_id column
-- remains available throughout phase 1.
