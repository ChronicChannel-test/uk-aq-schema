# Supabase Resource Exhaustion Diagnostics

## Purpose

Use this runbook to identify the source of Obs AQI DB resource exhaustion
before changing schema, indexes, constraints, RLS, or worker behavior. All SQL
below is read-only unless a section explicitly warns otherwise.

The current evidence points more strongly to background burst contention than
frontend chart latency. Likely contributors are:

- AQI `sync_hourly` at `*:20`
- AQI `reconcile_short` at `*:35`
- AQI `reconcile_deep_rolling` at `*:50`
- `uk_aq_rpc_timeseries_aqi_hourly_upsert`
- `uk_aq_rpc_timeseries_aqi_rollups_refresh`
- the hourly day-count refresh at `*:55`
- repeated `stations_fk_check` ID preflight requests

The canonical day-count SQL schedules
`uk_aq_obs_aqidb_day_counts_current_hourly` at `55 * * * *`. Its refresh
function groups the complete retained `uk_aq_observs.observations` and
`uk_aq_aqilevels.timeseries_aqi_hourly` tables, then repeats those scans while
removing stale summary rows. A reported duration near 40 seconds is therefore
credible. A rolling AQI run that starts at `*:50` and lasts more than five
minutes overlaps it directly; shorter runs can still produce closely spaced
resource bursts.

## 1. Recent AQI compute runs

`uk_aq_public.uk_aq_rpc_aqi_compute_run_log` writes
`uk_aq_ops.aqi_compute_runs`. The table has `started_at` and `duration_ms`, but
no separate `completed_at`; derive the approximate completion time.

Recent run detail:

```sql
select
  started_at,
  started_at + make_interval(secs => coalesce(duration_ms, 0) / 1000.0)
    as approximate_completed_at,
  run_mode,
  trigger_mode,
  run_status,
  duration_ms,
  source_rows,
  candidate_station_hours,
  rows_upserted,
  rows_changed,
  station_hours_changed,
  station_hours_changed_gt_36h,
  daily_rows_upserted,
  monthly_rows_upserted,
  error_message
from uk_aq_ops.aqi_compute_runs
where started_at >= now() - interval '48 hours'
order by started_at desc;
```

Mode-level duration and workload:

```sql
select
  run_mode,
  trigger_mode,
  run_status,
  count(*) as run_count,
  round(avg(duration_ms)) as avg_duration_ms,
  percentile_cont(0.50) within group (order by duration_ms) as p50_duration_ms,
  percentile_cont(0.95) within group (order by duration_ms) as p95_duration_ms,
  max(duration_ms) as max_duration_ms,
  sum(source_rows) as source_rows,
  sum(rows_changed) as rows_changed,
  sum(station_hours_changed) as station_hours_changed
from uk_aq_ops.aqi_compute_runs
where started_at >= now() - interval '7 days'
group by run_mode, trigger_mode, run_status
order by max_duration_ms desc nulls last;
```

Slow or failed runs:

```sql
select
  started_at,
  run_mode,
  trigger_mode,
  run_status,
  duration_ms,
  source_rows,
  rows_changed,
  station_hours_changed,
  error_message,
  case
    when run_status <> 'ok' then 'failed'
    when duration_ms > 300000 then 'over_300s'
    when duration_ms > 180000 then 'over_180s'
  end as threshold
from uk_aq_ops.aqi_compute_runs
where started_at >= now() - interval '7 days'
  and (
    run_status <> 'ok'
    or duration_ms > 180000
  )
order by duration_ms desc nulls last, started_at desc;
```

## 2. Scheduler overlap

The intended Cloud Scheduler times are `*:20`, `*:35`, and `*:50`. Inspect
database cron jobs alongside them:

```sql
select
  jobid,
  jobname,
  schedule,
  active,
  database,
  username,
  command
from cron.job
where jobname ilike '%uk_aq%'
   or command ilike '%uk_aq%'
order by schedule, jobname;
```

Recent `pg_cron` outcomes, where `cron.job_run_details` is available:

```sql
select
  j.jobname,
  j.schedule,
  d.status,
  d.start_time,
  d.end_time,
  extract(epoch from (d.end_time - d.start_time)) as duration_seconds,
  d.return_message
from cron.job_run_details d
join cron.job j on j.jobid = d.jobid
where d.start_time >= now() - interval '48 hours'
order by d.start_time desc;
```

Measured overlap between logged AQI runs and the hourly day-count cron:

```sql
select
  a.run_mode,
  a.started_at as aqi_started_at,
  a.started_at + a.duration_ms * interval '1 millisecond' as aqi_ended_at,
  d.start_time as day_count_started_at,
  d.end_time as day_count_ended_at,
  d.status as day_count_status
from uk_aq_ops.aqi_compute_runs a
join cron.job j
  on j.jobname = 'uk_aq_obs_aqidb_day_counts_current_hourly'
join cron.job_run_details d
  on d.jobid = j.jobid
 and tstzrange(
       a.started_at,
       a.started_at + a.duration_ms * interval '1 millisecond',
       '[)'
     ) && tstzrange(d.start_time, coalesce(d.end_time, now()), '[)')
where a.started_at >= now() - interval '7 days'
  and a.duration_ms is not null
order by a.started_at desc;
```

Compare actual start/end times with:

| Minute | Work |
|---:|---|
| `*:20` | `sync_hourly` |
| `*:35` | `reconcile_short` |
| `*:50` | `reconcile_deep_rolling` |
| `*:55` | `uk_aq_obs_aqidb_day_counts_current_hourly` |
| `06:10` | daily day-count reconcile |

First determine whether rolling deep remains active at `*:55`. If it does,
moving the day-count job away from AQI work is the lowest-risk mitigation to
evaluate before indexes or schema changes. Do not change the schedule until
the overlap is confirmed.

## 3. Function and view definitions

Inspect all relevant function overloads:

```sql
select
  n.nspname as schema_name,
  p.proname as function_name,
  pg_get_function_identity_arguments(p.oid) as identity_arguments,
  pg_get_functiondef(p.oid) as function_definition
from pg_proc p
join pg_namespace n on n.oid = p.pronamespace
where (n.nspname, p.proname) in (
  ('uk_aq_ops', 'uk_aq_obs_aqidb_day_counts_refresh_current'),
  ('uk_aq_public', 'uk_aq_rpc_timeseries_aqi_hourly_upsert'),
  ('uk_aq_public', 'uk_aq_rpc_timeseries_aqi_rollups_refresh'),
  ('uk_aq_public', 'uk_aq_rpc_timeseries_aqi_hourly_helper_upsert'),
  ('uk_aq_public', 'uk_aq_rpc_timeseries_aqi_hourly_helper_window')
)
order by schema_name, function_name, identity_arguments;
```

The two helper RPCs normally live in Ingest DB, while the hourly upsert and
rollup RPCs live in Obs AQI DB. An empty result can therefore mean the query is
being run against the other database, not that the function is missing.

Inspect the FK preflight view:

```sql
select pg_get_viewdef('uk_aq_public.stations_fk_check'::regclass, true);
```

If it is a direct projection of `uk_aq_core.stations.id`, and `id` is the
station table primary key, each batched `id in (...)` lookup should be
index-supported. Confirm with statistics before treating it as the main issue.

## 4. `pg_stat_statements`

Confirm availability:

```sql
select extname, extversion
from pg_extension
where extname = 'pg_stat_statements';
```

Filtered workload:

```sql
select
  calls,
  round(total_exec_time::numeric, 2) as total_exec_time_ms,
  round(mean_exec_time::numeric, 2) as mean_exec_time_ms,
  rows,
  shared_blks_read,
  shared_blks_hit,
  temp_blks_written,
  left(regexp_replace(query, '\s+', ' ', 'g'), 500) as query
from pg_stat_statements
where lower(query) similar to any (array[
  '%uk_aq_rpc_timeseries_aqi_hourly_upsert%',
  '%stations_fk_check%',
  '%uk_aq_obs_aqidb_day_counts_refresh_current%',
  '%timeseries_aqi_hourly%',
  '%timeseries_aqi_daily%',
  '%rollup%'
])
order by total_exec_time desc
limit 100;
```

Re-run that query ordering separately by `mean_exec_time desc`, `calls desc`,
`rows desc`, `shared_blks_read desc`, and `temp_blks_written desc`. High total
time can mean either a few expensive calls or many individually cheap calls;
those cases need different responses. Statistics are cumulative since the last
reset, so record the stats reset time:

```sql
select stats_reset
from pg_stat_database
where datname = current_database();
```

## 5. Active queries, locks, and blockers

Long-running activity:

```sql
select
  pid,
  usename,
  application_name,
  state,
  wait_event_type,
  wait_event,
  now() - query_start as query_age,
  now() - xact_start as transaction_age,
  pg_blocking_pids(pid) as blocking_pids,
  left(query, 1000) as query
from pg_stat_activity
where datname = current_database()
  and pid <> pg_backend_pid()
  and state <> 'idle'
order by query_start;
```

Blocked and blocking sessions:

```sql
select
  blocked.pid as blocked_pid,
  now() - blocked.query_start as blocked_for,
  blocked.wait_event_type,
  blocked.wait_event,
  blocker.pid as blocker_pid,
  now() - blocker.query_start as blocker_age,
  left(blocked.query, 500) as blocked_query,
  left(blocker.query, 500) as blocker_query
from pg_stat_activity blocked
cross join lateral unnest(pg_blocking_pids(blocked.pid)) as b(blocker_pid)
join pg_stat_activity blocker on blocker.pid = b.blocker_pid
order by blocked.query_start;
```

Lock inventory:

```sql
select
  a.pid,
  l.locktype,
  l.mode,
  l.granted,
  l.relation::regclass as relation,
  a.wait_event_type,
  a.wait_event,
  now() - a.query_start as query_age,
  left(a.query, 500) as query
from pg_locks l
join pg_stat_activity a on a.pid = l.pid
where a.datname = current_database()
order by l.granted, a.query_start;
```

## 6. Read-only index coverage

Foreign keys without an index whose leading columns match the FK:

```sql
with foreign_keys as (
  select
    c.oid as constraint_oid,
    c.conrelid,
    c.conname,
    c.conkey,
    n.nspname as schema_name,
    t.relname as table_name,
    pg_get_constraintdef(c.oid) as definition
  from pg_constraint c
  join pg_class t on t.oid = c.conrelid
  join pg_namespace n on n.oid = t.relnamespace
  where c.contype = 'f'
    and n.nspname in ('uk_aq_aqilevels', 'uk_aq_ops')
)
select
  fk.schema_name,
  fk.table_name,
  fk.conname,
  fk.definition
from foreign_keys fk
where not exists (
  select 1
  from pg_index i
  where i.indrelid = fk.conrelid
    and i.indisvalid
    and i.indpred is null
    and (
      select array_agg(k.attnum order by k.ordinality)
      from unnest(i.indkey::smallint[]) with ordinality
        as k(attnum, ordinality)
      where k.ordinality <= cardinality(fk.conkey)
    ) = fk.conkey
)
order by fk.schema_name, fk.table_name, fk.conname;
```

Relevant table indexes:

```sql
select schemaname, tablename, indexname, indexdef
from pg_indexes
where (schemaname, tablename) in (
  ('uk_aq_core', 'stations'),
  ('uk_aq_core', 'timeseries'),
  ('uk_aq_aqilevels', 'timeseries_aqi_hourly'),
  ('uk_aq_aqilevels', 'timeseries_aqi_daily'),
  ('uk_aq_aqilevels', 'timeseries_aqi_monthly'),
  ('uk_aq_ops', 'obs_aqidb_day_counts_current'),
  ('uk_aq_ops', 'aqi_compute_runs')
)
order by schemaname, tablename, indexname;
```

Usage and write statistics:

```sql
select
  schemaname,
  relname,
  indexrelname,
  idx_scan,
  idx_tup_read,
  idx_tup_fetch
from pg_stat_user_indexes
where schemaname in ('uk_aq_core', 'uk_aq_aqilevels', 'uk_aq_ops')
order by schemaname, relname, idx_scan desc;
```

Structurally duplicate indexes:

```sql
select
  n.nspname as schema_name,
  t.relname as table_name,
  i1.indexrelid::regclass as first_index,
  i2.indexrelid::regclass as second_index,
  pg_get_indexdef(i1.indexrelid) as first_definition,
  pg_get_indexdef(i2.indexrelid) as second_definition
from pg_index i1
join pg_index i2
  on i2.indrelid = i1.indrelid
 and i2.indexrelid > i1.indexrelid
 and i2.indkey = i1.indkey
 and i2.indclass = i1.indclass
 and i2.indcollation = i1.indcollation
 and i2.indoption = i1.indoption
 and i2.indisunique = i1.indisunique
 and i2.indisprimary = i1.indisprimary
 and coalesce(pg_get_expr(i2.indexprs, i2.indrelid), '') =
     coalesce(pg_get_expr(i1.indexprs, i1.indrelid), '')
 and coalesce(pg_get_expr(i2.indpred, i2.indrelid), '') =
     coalesce(pg_get_expr(i1.indpred, i1.indrelid), '')
join pg_class t on t.oid = i1.indrelid
join pg_namespace n on n.oid = t.relnamespace
where n.nspname in ('uk_aq_core', 'uk_aq_aqilevels', 'uk_aq_ops')
order by schema_name, table_name, first_index;
```

Tables without a primary key:

```sql
select
  n.nspname as schema_name,
  c.relname as table_name,
  c.reltuples::bigint as estimated_rows
from pg_class c
join pg_namespace n on n.oid = c.relnamespace
where c.relkind in ('r', 'p')
  and n.nspname in ('uk_aq_core', 'uk_aq_observs', 'uk_aq_aqilevels', 'uk_aq_ops')
  and not exists (
    select 1
    from pg_constraint pk
    where pk.conrelid = c.oid
      and pk.contype = 'p'
  )
order by estimated_rows desc, schema_name, table_name;
```

Permissive policy inventory, for review rather than automatic consolidation:

```sql
select
  schemaname,
  tablename,
  policyname,
  permissive,
  roles,
  cmd,
  qual,
  with_check
from pg_policies
where schemaname in (
  'uk_aq_core',
  'uk_aq_observs',
  'uk_aq_aqilevels',
  'uk_aq_ops',
  'uk_aq_public'
)
order by schemaname, tablename, cmd, policyname;
```

These are candidates for review only. Do not drop an index based solely on
structural similarity or low `idx_scan`: primary/unique enforcement, FK
behavior, write overhead, reset times, and production plans all matter.

## 7. Day-count refresh analysis

Definition and schedule:

```sql
select pg_get_functiondef(
  'uk_aq_ops.uk_aq_obs_aqidb_day_counts_refresh_current(timestamptz,text)'
    ::regprocedure
);

select jobid, jobname, schedule, active, command
from cron.job
where jobname in (
  'uk_aq_obs_aqidb_day_counts_current_hourly',
  'uk_aq_obs_aqidb_day_counts_current_reconcile_daily'
);
```

The current definition touches:

- `uk_aq_observs.observations`
- `uk_aq_aqilevels.timeseries_aqi_hourly`
- `uk_aq_ops.obs_aqidb_day_counts_current`

It computes complete per-day counts twice per invocation. Inspect the internal
read queries with plain `EXPLAIN`, which does not execute them:

```sql
explain (verbose, costs, settings)
select
  (o.observed_at at time zone 'UTC')::date as day_utc,
  count(*)::bigint as row_count
from uk_aq_observs.observations o
group by 1;

explain (verbose, costs, settings)
select
  (h.timestamp_hour_utc at time zone 'UTC')::date as day_utc,
  count(*)::bigint as row_count
from uk_aq_aqilevels.timeseries_aqi_hourly h
group by 1;
```

Do not use `EXPLAIN ANALYZE` on the refresh function during contention:
`ANALYZE` executes the function, including writes and repeated full scans. If
execution timing is later required, do it during a controlled maintenance
window or against a representative non-production database. A transaction
rollback would undo writes but would not undo load, locks, or resource usage.

Possible later mitigations, only after evidence collection:

1. Move the `*:55` hourly refresh away from AQI schedules.
2. Reassess whether exact full-window counts are needed hourly.
3. Refresh only changed/current days while retaining raw granularity.
4. Add targeted expression or timestamp indexes only when plans justify them.
5. Maintain a small summary table incrementally.

Moving or staggering the schedule is the first low-risk option when overlap is
confirmed. Do not reduce website polling frequency, and do not downsample raw
observation history.

## 8. Decision tree

```text
Day-count refresh slow and overlapping AQI?
  Yes -> first evaluate moving/staggering the day-count schedule.
  No  -> compare pg_stat_statements and active-query evidence.

Rolling AQI runs exceed 600 seconds?
  Yes -> reduce rolling window or batch size before increasing deadlines.

Hourly upsert dominates total_exec_time?
  Yes -> inspect its function plan, conflict checks, and supporting indexes.

Station FK checks dominate total_exec_time?
  Yes -> inspect the view definition and station primary-key/index usage.

Duplicate indexes appear on high-write AQI tables?
  Yes -> review constraints, plans, and index statistics before any drop.

Charts remain responsive while Supabase reports exhaustion?
  Yes -> treat background burst contention as the leading hypothesis.
```

## 9. Recommended first manual checks

1. Query `cron.job` and `cron.job_run_details`; verify whether the `*:55`
   day-count refresh overlaps rolling AQI runs that start at `*:50`.
2. Query `aqi_compute_runs` for recent duration and failure thresholds.
3. Capture filtered `pg_stat_statements` totals and means before any reset.
4. During a warning, capture `pg_stat_activity`, blocking PIDs, and locks.
5. Inspect the day-count function and its two full-table grouping plans.

Do not yet add or drop indexes, add primary keys, change RLS, alter AQI tables,
change raw-history granularity, or tune function SQL without measured plans and
schedule-overlap evidence.
