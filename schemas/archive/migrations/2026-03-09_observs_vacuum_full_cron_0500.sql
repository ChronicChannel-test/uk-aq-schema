-- Align OBS AQI DB VACUUM FULL schedule at 05:00 UTC.
-- Removes legacy observs/history/aqilevels job names and ensures one shared 05:00 full-database job.

create extension if not exists pg_cron with schema extensions;

select cron.unschedule(jobid)
from cron.job
where jobname in (
  'uk_aq_history_observations_vacuum_full_0530_utc',
  'uk_aq_observs_observations_vacuum_full_0530_utc',
  'uk_aq_observs_vacuum_full_0500_utc',
  'uk_aq_aqilevels_vacuum_full_0500_utc',
  'uk_aq_obs_aqidb_vacuum_full_0500_utc'
);

select cron.schedule(
  'uk_aq_obs_aqidb_vacuum_full_0500_utc',
  '0 5 * * *',
  $$vacuum (full, analyze, verbose);$$
);
