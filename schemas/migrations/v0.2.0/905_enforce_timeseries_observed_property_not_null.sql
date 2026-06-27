-- ============================================================================
-- DEFERRED ENFORCEMENT - NOT PART OF THE DEFAULT PHASE 1 MIGRATION SEQUENCE
-- ============================================================================
--
-- Run only after:
-- - 005b reports zero unmapped timeseries
-- - every ingest writer supplies observed_property_id
-- - views, RPCs and AQI functions have moved away from phenomenon_id
-- - the dependency report has been reviewed
--
-- This file does not drop phenomenon_id or any other legacy column.

set search_path = uk_aq_core, public, pg_catalog;

do $$
begin
  if exists (
    select 1
    from timeseries
    where observed_property_id is null
  ) then
    raise exception
      'Cannot enforce timeseries.observed_property_id NOT NULL: unmapped timeseries remain';
  end if;
end
$$;

alter table timeseries
  alter column observed_property_id set not null;
