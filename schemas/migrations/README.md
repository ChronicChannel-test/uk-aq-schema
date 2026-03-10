# Schema Migrations (obs_aqidb refactor)

Date: 2026-03-08

These SQL files are the targeted cutover migrations for Phase 2/3 hard-cut rename work.

## Files
- `2026-03-08_ingest_size_metrics_schema_r2.sql`
  - apply to `ingestdb`
  - adds R2 hourly size metric tables/views/RPCs
  - hard-cuts DB labels for DB-size upsert RPCs
- `2026-03-08_obs_aqidb_schema_size_metrics_store.sql`
  - apply to `obs_aqidb`
  - adds schema hourly size metric table/view/RPCs in `obs_aqidb`
  - stores `uk_aq_observs` + `uk_aq_aqilevels` schema metrics in the obs cluster (not ingestdb)
- `2026-03-08_ingest_drop_schema_size_metrics_store.sql`
  - apply to `ingestdb`
  - removes legacy ingest schema-size metric table/view/RPCs
  - ensures schema-size metric storage exists only in `obs_aqidb`
- `2026-03-08_obs_aqidb_db_size_label_cutover.sql`
  - apply to `obs_aqidb`
  - hard-cuts DB label checks/RPC validation to `ingestdb` + `obs_aqidb`
- `2026-03-08_phase3_obs_aqidb_schema_hard_cut.sql`
  - apply to both `obs_aqidb` and `ingestdb`
  - renames schemas to final names
  - updates backfill run-mode and metric column names
  - normalizes DB-size metric labels (`historydb`, `aggdailydb` -> `obs_aqidb`)
- `2026-03-08_obs_aqidb_db_size_oldest_combined_fix.sql`
  - apply to `obs_aqidb`
  - fixes DB-size oldest timestamp sampling to use the minimum across:
    - `uk_aq_observs.observations.observed_at`
    - `uk_aq_aqilevels.station_aqi_hourly.timestamp_hour_utc`
  - updates both local pg_cron sampler and public DB-size RPC
- `2026-03-09_observs_vacuum_full_cron_0500.sql`
  - apply to `obs_aqidb`
  - removes legacy observs/history/aqilevels vacuum-full job names
  - ensures one shared 05:00 full-database vacuum job:
    - `uk_aq_obs_aqidb_vacuum_full_0500_utc`
- `2026-03-10_obs_aqidb_db_size_current_database_fast_path.sql`
  - apply to `obs_aqidb`
  - replaces cluster-sum DB size reads with `pg_database_size(current_database())`
  - prevents statement-timeout failures in DB-size RPC sampling paths

## Recommended Apply Order
1. `obs_aqidb`: `2026-03-08_obs_aqidb_db_size_label_cutover.sql`
2. `obs_aqidb`: `2026-03-08_phase3_obs_aqidb_schema_hard_cut.sql`
3. `obs_aqidb`: `../aqilevels_db/uk_aq_aqilevels_schema.sql` (ensure AQI schema exists in obs_aqidb)
4. `obs_aqidb`: `2026-03-08_obs_aqidb_db_size_oldest_combined_fix.sql`
5. `obs_aqidb`: `2026-03-08_obs_aqidb_schema_size_metrics_store.sql`
6. `ingestdb`: `2026-03-08_ingest_size_metrics_schema_r2.sql`
7. `ingestdb`: `2026-03-08_phase3_obs_aqidb_schema_hard_cut.sql`
8. `ingestdb`: `2026-03-08_ingest_drop_schema_size_metrics_store.sql`
9. `obs_aqidb`: `2026-03-09_observs_vacuum_full_cron_0500.sql`
10. `obs_aqidb`: `2026-03-10_obs_aqidb_db_size_current_database_fast_path.sql`

Runbook with full verification queries lives in ingest repo:
`plans/obs_aqidb_refactor_phase3_runbook.md`
