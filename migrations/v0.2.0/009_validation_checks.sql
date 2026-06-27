-- UK AQ v0.2.0 additive TEST post-migration validation.
-- Read-only. Compare baseline metrics with 001_preflight_checks.sql output.

set search_path = uk_aq_core, public, pg_catalog;

select
  'post_migration_counts_compare_to_preflight' as check_name,
  (select count(*) from uk_aq_core.connectors) as connectors_after,
  (select count(*) from uk_aq_core.networks) as networks_after,
  (select count(*) from uk_aq_core.stations) as stations_after,
  (select count(*) from uk_aq_core.station_metadata) as old_station_metadata_after,
  (select count(*) from uk_aq_core.station_initial_metadata) as station_initial_metadata_after,
  (select count(*) from uk_aq_core.observed_properties) as observed_properties_after,
  (select count(*) from uk_aq_core.timeseries) as timeseries_after,
  (select count(*) from uk_aq_core.observations) as observations_after,
  (select count(*) from uk_aq_core.uk_aq_ingest_runs) as ingest_runs_after;

select
  s.id,
  c.connector_code,
  s.station_ref,
  s.label,
  s.service_ref
from uk_aq_core.stations s
join uk_aq_core.connectors c
  on c.id = s.connector_id
where s.network_id is null
order by c.connector_code, s.id;

select
  t.id,
  c.connector_code,
  t.timeseries_ref,
  t.label,
  t.phenomenon_id
from uk_aq_core.timeseries t
join uk_aq_core.connectors c
  on c.id = t.connector_id
where t.observed_property_id is null
order by c.connector_code, t.id;

select
  s.id,
  c.connector_code,
  s.station_ref,
  s.label,
  s.station_name,
  s.removed_at,
  (s.removed_at is not null) as is_removed
from uk_aq_core.stations s
join uk_aq_core.connectors c
  on c.id = s.connector_id
where s.id = 3291
   or s.station_ref = '9999999999'
   or s.label = 'GB_SamplingFeature_missingFOI'
order by s.id;

select
  (select count(*) from uk_aq_core.stations) as station_rows,
  (select count(*) from uk_aq_core.station_initial_metadata) as initial_metadata_rows,
  (select count(*) from uk_aq_core.station_metadata) as legacy_metadata_rows,
  (
    select count(*)
    from uk_aq_core.stations s
    left join uk_aq_core.station_initial_metadata sim
      on sim.station_id = s.id
    where sim.station_id is null
  ) as stations_missing_initial_metadata,
  (
    select count(*)
    from uk_aq_core.station_initial_metadata sim
    join uk_aq_core.station_metadata sm
      on sm.station_id = sim.station_id
  ) as migrated_initial_metadata_rows,
  (
    select count(*)
    from uk_aq_core.station_metadata sm
    left join uk_aq_core.station_initial_metadata sim
      on sim.station_id = sm.station_id
    where sim.station_id is null
  ) as missing_initial_metadata_rows;

select
  array_agg(network_code order by network_code)
    filter (where public_display_enabled) as public_network_codes,
  array_agg(network_code order by network_code)
    filter (where not public_display_enabled) as non_public_network_codes,
  (
    array_agg(network_code order by network_code)
      filter (where public_display_enabled)
    = array['breathelondon', 'gov_uk_aurn']::text[]
  ) as public_network_set_is_expected
from uk_aq_core.networks;

select
  connector_code,
  label,
  display_name,
  service_url,
  default_network_id
from uk_aq_core.connectors
where connector_code in (
  'breathelondon',
  'blondon_communities',
  'blondon_nodes'
)
order by connector_code;

select
  count(*) filter (where connector_code = 'breathelondon') as old_code_count,
  count(*) filter (where connector_code = 'blondon_communities') as communities_count,
  count(*) filter (where connector_code = 'blondon_nodes') as nodes_count
from uk_aq_core.connectors;

select
  connector_id,
  station_ref,
  count(*) as duplicate_count,
  array_agg(id order by id) as station_ids
from uk_aq_core.stations
group by connector_id, station_ref
having count(*) > 1
order by connector_id, station_ref;

select
  connector_id,
  timeseries_ref,
  count(*) as duplicate_count,
  array_agg(id order by id) as timeseries_ids
from uk_aq_core.timeseries
group by connector_id, timeseries_ref
having count(*) > 1
order by connector_id, timeseries_ref;

select
  count(*) as orphan_observation_count
from uk_aq_core.observations o
left join uk_aq_core.timeseries t
  on t.id = o.timeseries_id
where o.timeseries_id is null
   or t.id is null;

select
  conname,
  convalidated
from pg_constraint
where conrelid in (
  'uk_aq_core.connectors'::regclass,
  'uk_aq_core.stations'::regclass,
  'uk_aq_core.timeseries'::regclass,
  'uk_aq_core.uk_aq_ingest_runs'::regclass
)
  and conname in (
    'connectors_default_network_id_fkey',
    'stations_network_id_fkey',
    'stations_match_id_fkey',
    'timeseries_observed_property_id_fkey',
    'uk_aq_ingest_runs_network_id_fkey'
  )
order by conname;

-- Expected in this additive phase: connector_id remains on the partitioned
-- observations table and the legacy PK remains unchanged. This query records
-- that deferred cut explicitly for review.
select
  c.column_name,
  c.data_type,
  c.is_nullable
from information_schema.columns c
where c.table_schema = 'uk_aq_core'
  and c.table_name = 'observations'
  and c.column_name in (
    'connector_id',
    'timeseries_id',
    'observed_at',
    'metadata'
  )
order by c.ordinal_position;
