-- Schedule daily VACUUM FULL for aggdaily database at 05:00 UTC.

create extension if not exists pg_cron with schema extensions;

-- Replace existing job if present.
select cron.unschedule(jobid)
from cron.job
where jobname = 'uk_aq_aggdaily_vacuum_full_0500_utc';

select cron.schedule(
  'uk_aq_aggdaily_vacuum_full_0500_utc',
  '0 5 * * *',
  $$vacuum (full, analyze, verbose);$$
);
