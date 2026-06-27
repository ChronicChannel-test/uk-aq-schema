-- UK AQ v0.2.0 additive TEST migration: retire known placeholder stations.

set search_path = uk_aq_core, public, pg_catalog;

update stations s
set
  removed_at = coalesce(s.removed_at, now()),
  updated_at = now()
from connectors c
where c.id = s.connector_id
  and c.connector_code = 'uk_air_sos'
  and (
    s.id = 3291
    or s.station_ref = '9999999999'
    or s.label = 'GB_SamplingFeature_missingFOI'
    or s.station_name = 'GB_SamplingFeature_missingFOI'
  )
  and s.removed_at is null;

-- No placeholder flag is added. Public display excludes removed stations.
