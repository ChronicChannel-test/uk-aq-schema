-- UK AQ v0.2.0 TEST preflight checks.
-- Read-only. Save this output before running any later migration file.

set search_path = uk_aq_core, public, pg_catalog;

select
  'baseline_counts' as check_name,
  (select count(*) from uk_aq_core.connectors) as connectors,
  (select count(*) from uk_aq_core.stations) as stations,
  (select count(*) from uk_aq_core.station_metadata) as station_metadata,
  (select count(*) from uk_aq_core.observed_properties) as observed_properties,
  (select count(*) from uk_aq_core.phenomena) as phenomena,
  (select count(*) from uk_aq_core.timeseries) as timeseries,
  (select count(*) from uk_aq_core.observations) as observations,
  (select count(*) from uk_aq_core.uk_aq_ingest_runs) as ingest_runs;

select
  connector_code,
  count(*) as duplicate_count
from uk_aq_core.connectors
group by connector_code
having count(*) > 1
order by connector_code;

-- The planned seed itself must not contain duplicate network codes.
with planned_networks(network_code) as (
  values
    ('gov_uk_aurn'),
    ('breathelondon'),
    ('openaq'),
    ('sensorcommunity'),
    ('laqn')
)
select
  network_code,
  count(*) as duplicate_count
from planned_networks
group by network_code
having count(*) > 1
order by network_code;

-- If a previous draft already created uk_aq_core.networks, fail on duplicates.
do $$
declare
  duplicate_exists boolean := false;
begin
  if to_regclass('uk_aq_core.networks') is not null then
    execute
      'select exists (
         select 1
         from uk_aq_core.networks
         group by network_code
         having count(*) > 1
       )'
    into duplicate_exists;
    if duplicate_exists then
      raise exception 'Duplicate uk_aq_core.networks.network_code values found';
    end if;
  end if;
end
$$;

select
  connector_id,
  station_ref,
  count(*) as duplicate_count,
  array_agg(id order by id) as station_ids,
  array_agg(service_ref order by id) as service_refs
from uk_aq_core.stations
group by connector_id, station_ref
having count(*) > 1
order by connector_id, station_ref;

select
  connector_id,
  timeseries_ref,
  count(*) as duplicate_count,
  array_agg(id order by id) as timeseries_ids,
  array_agg(service_ref order by id) as service_refs
from uk_aq_core.timeseries
group by connector_id, timeseries_ref
having count(*) > 1
order by connector_id, timeseries_ref;

select
  t.id as timeseries_id,
  t.connector_id,
  t.station_id,
  t.timeseries_ref
from uk_aq_core.timeseries t
left join uk_aq_core.stations s
  on s.id = t.station_id
where t.station_id is null
   or s.id is null
order by t.id;

select
  count(*) as orphan_observation_count
from uk_aq_core.observations o
left join uk_aq_core.timeseries t
  on t.id = o.timeseries_id
where o.timeseries_id is null
   or t.id is null;

select
  count(*) as placeholder_station_count,
  array_agg(s.id order by s.id) as station_ids
from uk_aq_core.stations s
left join uk_aq_core.station_metadata sm
  on sm.station_id = s.id
where s.station_ref = '9999999999'
   or s.label = 'GB_SamplingFeature_missingFOI'
   or lower(coalesce(sm.attributes ->> 'is_placeholder', ''))
      in ('true', 't', '1', 'yes');

select
  count(*) as old_station_metadata_count
from uk_aq_core.station_metadata;

select
  c.connector_code,
  s.service_ref,
  count(*) as station_count
from uk_aq_core.stations s
join uk_aq_core.connectors c
  on c.id = s.connector_id
group by c.connector_code, s.service_ref
order by c.connector_code, s.service_ref;

select
  c.connector_code,
  t.service_ref,
  count(*) as timeseries_count
from uk_aq_core.timeseries t
join uk_aq_core.connectors c
  on c.id = t.connector_id
group by c.connector_code, t.service_ref
order by c.connector_code, t.service_ref;

select
  p.id as phenomenon_id,
  c.connector_code,
  p.label,
  p.source_label,
  p.notation,
  p.pollutant_label,
  p.observed_property_id,
  op.code as observed_property_code,
  case
    when p.observed_property_id is null then 'missing_mapping'
    when op.id is null then 'orphan_mapping'
    else 'mapped'
  end as mapping_status
from uk_aq_core.phenomena p
join uk_aq_core.connectors c
  on c.id = p.connector_id
left join uk_aq_core.observed_properties op
  on op.id = p.observed_property_id
order by mapping_status, c.connector_code, p.id;
