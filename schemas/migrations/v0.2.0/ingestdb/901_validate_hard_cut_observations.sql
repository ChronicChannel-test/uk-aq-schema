-- UK AQ v0.2.0 deferred hard-cut validation checks.
-- Read-only. Do not run in phase 1.
-- Run only after 900_hard_cut_observations_after_dependencies.sql has an
-- approved security section and has completed successfully.

set search_path = uk_aq_core, public, pg_catalog;

select
  'final_counts' as check_name,
  (select count(*) from connectors) as connectors,
  (select count(*) from networks) as networks,
  (select count(*) from stations) as stations,
  (select count(*) from station_initial_metadata) as station_initial_metadata,
  (select count(*) from observed_properties) as observed_properties,
  (select count(*) from timeseries) as timeseries,
  (select count(*) from observations) as observations,
  (select count(*) from uk_aq_ingest_runs) as ingest_runs;

select
  'observation_copy_count_match' as check_name,
  (select count(*) from observations_legacy_v020) as legacy_observations,
  (select count(*) from observations) as final_observations,
  (
    (select count(*) from observations_legacy_v020)
    =
    (select count(*) from observations)
  ) as passed;

select 'stations_without_network_id' as check_name, count(*) as n
from stations
where network_id is null;

select 'timeseries_without_station_or_property' as check_name, count(*) as n
from timeseries
where station_id is null
   or observed_property_id is null;

select 'duplicate_stations_connector_station_ref' as check_name, count(*) as duplicate_keys
from (
  select connector_id, station_ref
  from stations
  group by connector_id, station_ref
  having count(*) > 1
) d;

select 'duplicate_timeseries_connector_ref' as check_name, count(*) as duplicate_keys
from (
  select connector_id, timeseries_ref
  from timeseries
  group by connector_id, timeseries_ref
  having count(*) > 1
) d;

select 'duplicate_observations_timeseries_observed_at' as check_name, count(*) as duplicate_keys
from (
  select timeseries_id, observed_at
  from observations
  group by timeseries_id, observed_at
  having count(*) > 1
) d;

select 'orphan_observations' as check_name, count(*) as n
from observations o
left join timeseries t on t.id = o.timeseries_id
where t.id is null;

select
  'observations_columns' as check_name,
  array_agg(column_name order by ordinal_position) as columns
from information_schema.columns
where table_schema = 'uk_aq_core'
  and table_name = 'observations';

select
  'observations_has_no_connector_id' as check_name,
  not exists (
    select 1
    from information_schema.columns
    where table_schema = 'uk_aq_core'
      and table_name = 'observations'
      and column_name = 'connector_id'
  ) as passed;

select
  s.id,
  c.connector_code,
  s.station_ref,
  s.label,
  s.station_name,
  s.removed_at,
  (s.removed_at is not null) as placeholder_removed
from stations s
join connectors c on c.id = s.connector_id
where s.id = 3291
   or s.station_ref = '9999999999'
   or s.label = 'GB_SamplingFeature_missingFOI'
order by s.id;

select
  array_agg(network_code order by network_code)
    filter (where public_display_enabled) as public_network_codes,
  (
    array_agg(network_code order by network_code)
      filter (where public_display_enabled)
    = array['breathelondon', 'gov_uk_aurn']::text[]
  ) as public_network_set_is_expected
from networks;

select
  count(*) filter (where connector_code = 'breathelondon') as old_breathelondon_code_count,
  count(*) filter (where connector_code = 'blondon_communities') as blondon_communities_count,
  count(*) filter (where connector_code = 'blondon_nodes') as blondon_nodes_count
from connectors;
