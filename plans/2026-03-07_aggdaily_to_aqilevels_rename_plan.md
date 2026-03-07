# AggDaily -> AQI Levels Rename Plan

Date: 2026-03-07
Owner: UK AQ project
Status: Draft

## Goal
Rename:
- logical DB label: `aggdailydb` -> `aqilevelsdb`
- schema name: `uk_aq_aggdaily` -> `uk_aq_aqilevels`

Requested constraint:
- do not spend effort updating historical migration SQL files
- ensure main schema files are the source of truth

## Important Clarification
In Supabase, the underlying database name is typically `postgres`.
`aggdailydb` in this project is used as a logical label in code, metrics, and dashboards.

## Scope
In scope:
- runtime SQL/schema files
- scripts/workers/workflows/config used by deploy/runtime
- code references to `aggdailydb`, `uk_aq_aggdaily`, and `aggdaily` naming in active runtime paths

Out of scope (for this plan):
- markdown docs updates
- historical migration rewrites

## Impact Summary
- Core functional rename set (label + schema): ~11 non-migration files
- Full naming cleanup (`aggdaily` wording across runtime code/config): ~29 non-migration files

No direct impact from rename itself on:
- Supabase billable egress
- database size

## Core Files (must change for label + schema rename)

### Schema repo
- `schemas/aggdaily_db/uk_aq_aggdaily_schema.sql`
- `schemas/ingest_db/uk_aq_aggdaily_schema.sql`
- `schemas/ingest_db/uk_aq_rpc.sql`
- `schemas/ingest_db/uk_aq_ops_schema.sql`
- `schemas/history_db/uk_aq_history_schema.sql`

### Ops repo
- `workers/uk_aq_db_size_logger_cloud_run/run_job.ts`
- `workers/uk_aq_db_size_metrics_api_worker/worker.mjs`
- `.github/workflows/uk_aq_db_size_logger_cloud_run_deploy.yml`

### Ingest repo
- `scripts/uk_aq_dashboard_local.py`
- `data/uk_aq_dashboard/uk_aq_dashboard.html`
- `scripts/stations_daily/sync_aggdaily_uk_aq_core.py`

## Additional Files for Full Naming Cleanup
If we also remove `aggdaily` terminology from runtime naming (service names, env var names, workflow vars, script paths), include:

### Ops repo
- `workers/uk_aq_aqi_station_aggdaily_cloud_run/Dockerfile`
- `workers/uk_aq_aqi_station_aggdaily_cloud_run/run_job.ts`
- `workers/uk_aq_aqi_station_aggdaily_cloud_run/run_service.ts`
- `workers/uk_aq_backfill_cloud_run/run_job.ts`
- `workers/uk_aq_backfill_cloud_run/run_service.ts`
- `workers/uk_aq_backfill_cloud_run/backfill_core.mjs`
- `workers/uk_aq_backfill_cloud_run/sql/uk_aq_backfill_ops_aggdaily.sql`
- `scripts/gcp/uk_aq_backfill_cloud_run_call.sh`
- `.github/workflows/uk_aq_aqi_station_aggdaily_cloud_run_deploy.yml`
- `.github/workflows/uk_aq_backfill_cloud_run_deploy.yml`
- `.github/workflows/uk_aq_db_size_metrics_api_worker_deploy.yml`
- `config/uk_aq_github_env_targets.csv`

### Ingest repo
- `.github/workflows/uk_aq_stations_daily.yml`
- `config/uk_aq_github_env_targets.csv`

### Schema repo
- `schemas/aggdaily_db/uk_aq_backfill_ops_aggdaily.sql`

## Non-file Work Required
1. Run one-time DB DDL in live environments:
   - `alter schema uk_aq_aggdaily rename to uk_aq_aqilevels;`
2. Re-apply/redeploy updated main schema SQL so function bodies and search paths no longer reference `uk_aq_aggdaily`.
3. Update deploy variables/secrets if full naming cleanup includes env key renames.
4. Redeploy affected Cloud Run services/workers and ingest dashboard service.

## Phased Rollout

### Phase 1: Safe functional rename
- Update core 11 files only
- Keep existing AGGDAILY_* env var names for compatibility
- Change runtime labels and schema references
- Deploy + validate

### Phase 2: Full terminology cleanup (optional)
- Rename worker/service/workflow/script/env naming from `aggdaily` to `aqilevels`
- Update GitHub Actions variables and secrets naming
- Redeploy all affected services

## Validation Checklist
- DB size logger writes `database_label='aqilevelsdb'`
- Dashboard accepts/plots `aqilevelsdb`
- Backfill + AQI compute RPCs execute successfully against `uk_aq_aqilevels`
- Hourly/daily/monthly AQI reads and rollups succeed
- No runtime errors referencing `uk_aq_aggdaily`

## Rollback
- Revert code/workflow changes
- Rename schema back if needed:
  - `alter schema uk_aq_aqilevels rename to uk_aq_aggdaily;`
- Redeploy previous worker/service versions

## Recommendation
Use Phase 1 first (minimal risk), then Phase 2 only after stable runtime validation.
