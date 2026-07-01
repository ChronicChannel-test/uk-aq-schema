-- Narrow Obs AQI DB REST view for service station-FK preflight checks.
-- This does not expose uk_aq_core itself through PostgREST.

create schema if not exists uk_aq_public;

create or replace view uk_aq_public.stations_fk_check as
select
  id
from uk_aq_core.stations;

alter view if exists uk_aq_public.stations_fk_check
  set (security_invoker = true);

comment on view uk_aq_public.stations_fk_check is
  'Station ID-only public view used by service FK preflight checks. Exposes only mirrored station parent IDs so workers can validate station_id references without exposing uk_aq_core over PostgREST.';

revoke all on uk_aq_public.stations_fk_check from public;
grant select on uk_aq_public.stations_fk_check to service_role;
