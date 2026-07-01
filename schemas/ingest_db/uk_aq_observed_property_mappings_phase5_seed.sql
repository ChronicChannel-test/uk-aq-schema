-- Phase 5: preserve current canonical mappings as authoritative connector policy.
-- Existing null-source-label legacy phenomena are intentionally excluded.

set search_path = uk_aq_core, public, pg_catalog;

insert into observed_property_mappings (
  connector_id,
  source_label,
  notation,
  pollutant_label,
  observed_property_id,
  observed_property_code,
  mapping_kind,
  is_aqi_eligible,
  is_active,
  confidence,
  notes
)
select
  p.connector_id,
  p.source_label,
  p.notation,
  p.pollutant_label,
  op.id,
  op.code,
  case
    when op.domain = 'met' then 'meteorological'
    else 'raw_observed_property'
  end,
  op.code in ('pm25', 'pm10', 'no2'),
  true,
  'legacy_backfill',
  'Phase 5 policy seed preserving the verified pre-migration phenomenon mapping.'
from phenomena p
join connectors c
  on c.id = p.connector_id
join observed_properties op
  on op.id = p.observed_property_id
where p.source_label is not null
  and c.connector_code <> 'blondon_nodes'
on conflict (connector_id, source_label) do update
set
  notation = excluded.notation,
  pollutant_label = excluded.pollutant_label,
  observed_property_id = excluded.observed_property_id,
  observed_property_code = excluded.observed_property_code,
  mapping_kind = excluded.mapping_kind,
  is_aqi_eligible = excluded.is_aqi_eligible,
  is_active = true,
  confidence = excluded.confidence,
  notes = excluded.notes
where (
  observed_property_mappings.notation,
  observed_property_mappings.pollutant_label,
  observed_property_mappings.observed_property_id,
  observed_property_mappings.observed_property_code,
  observed_property_mappings.mapping_kind,
  observed_property_mappings.is_aqi_eligible,
  observed_property_mappings.is_active,
  observed_property_mappings.confidence,
  observed_property_mappings.notes
) is distinct from (
  excluded.notation,
  excluded.pollutant_label,
  excluded.observed_property_id,
  excluded.observed_property_code,
  excluded.mapping_kind,
  excluded.is_aqi_eligible,
  true,
  excluded.confidence,
  excluded.notes
);
