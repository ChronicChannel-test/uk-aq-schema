begin;

-- Phase 8 ingest cleanup:
-- 1) remove legacy history-named observations upsert RPC left from pre-hard-cut state
-- 2) re-create AQI lookup function to guarantee uk_aq_aqilevels references only

drop function if exists uk_aq_public.uk_aq_rpc_history_observations_upsert(jsonb);

drop function if exists uk_aq_aqilevels.uk_aq_aqi_index_lookup(
  text,
  text,
  text,
  double precision,
  date
);

create or replace function uk_aq_aqilevels.uk_aq_aqi_index_lookup(
  p_standard_code text,
  p_pollutant_code text,
  p_averaging_code text,
  p_value double precision,
  p_effective_date date default ((now() at time zone 'UTC')::date)
)
returns table (
  index_level smallint,
  index_band text
)
language sql
stable
set search_path = uk_aq_aqilevels, public, pg_catalog
as $$
  select
    b.index_level,
    b.index_band
  from uk_aq_aqilevels.aqi_breakpoints b
  join uk_aq_aqilevels.aqi_standard_versions v
    on v.standard_code = b.standard_code
   and v.version_code = b.version_code
  where p_value is not null
    and b.standard_code = p_standard_code
    and b.pollutant_code = p_pollutant_code
    and b.averaging_code = p_averaging_code
    and (v.valid_from is null or v.valid_from <= p_effective_date)
    and (v.valid_to is null or v.valid_to >= p_effective_date)
    and (b.valid_from is null or b.valid_from <= p_effective_date)
    and (b.valid_to is null or b.valid_to >= p_effective_date)
    and p_value >= b.range_low
    and (b.range_high is null or p_value <= b.range_high)
  order by b.index_level
  limit 1;
$$;

revoke all on function uk_aq_aqilevels.uk_aq_aqi_index_lookup(
  text,
  text,
  text,
  double precision,
  date
) from public;

commit;
