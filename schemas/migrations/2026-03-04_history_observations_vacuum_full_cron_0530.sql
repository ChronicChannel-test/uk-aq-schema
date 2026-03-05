-- Schedule daily VACUUM FULL for history observations at 05:30 UTC.

create extension if not exists pg_cron with schema extensions;

-- Replace existing job if present.
select cron.unschedule(jobid)
from cron.job
where jobname = 'uk_aq_history_observations_vacuum_full_0530_utc';

select cron.schedule(
  'uk_aq_history_observations_vacuum_full_0530_utc',
  '30 5 * * *',
  $$vacuum (full, analyze, verbose) uk_aq_history.observations;$$
);
