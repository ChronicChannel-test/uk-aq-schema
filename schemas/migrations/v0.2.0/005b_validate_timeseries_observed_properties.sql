-- UK AQ v0.2.0 phase 1 validation: observed-property mapping coverage.
-- Read-only. This reports exceptions and deliberately does not block phase 1.

set search_path = uk_aq_core, public, pg_catalog;

select
  'timeseries_observed_property_mapping_summary' as check_name,
  count(*) as total_timeseries,
  count(*) filter (where observed_property_id is not null) as mapped_timeseries,
  count(*) filter (where observed_property_id is null) as unmapped_timeseries
from timeseries;

select
  t.id as timeseries_id,
  c.connector_code,
  t.station_id,
  t.timeseries_ref,
  t.label as timeseries_label,
  t.uom,
  t.phenomenon_id,
  p.label as phenomenon_label,
  p.source_label as phenomenon_source_label,
  p.notation as phenomenon_notation,
  p.pollutant_label,
  p.observed_property_id as phenomenon_observed_property_id
from timeseries t
join connectors c
  on c.id = t.connector_id
left join phenomena p
  on p.id = t.phenomenon_id
where t.observed_property_id is null
order by c.connector_code, t.id;

select
  c.connector_code,
  count(*) as unmapped_timeseries
from timeseries t
join connectors c
  on c.id = t.connector_id
where t.observed_property_id is null
group by c.connector_code
order by c.connector_code;
