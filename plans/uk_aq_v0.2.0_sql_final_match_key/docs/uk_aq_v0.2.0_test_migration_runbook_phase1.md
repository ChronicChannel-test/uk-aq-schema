# UK AQ v0.2.0 Test Migration Runbook, Phase 1 Additive

Status: draft runbook for applying the corrected v0.2.0 migration set to the test database.

Scope: this runbook is for the **test database first**. The live beta should only be migrated after this has completed successfully on test and the dependent code/views/RPCs have been updated and retested.

## 1. Migration approach

The current migration set is split into two categories:

1. **Phase 1 additive migrations**  
   These add the v0.2.0 structure and backfill the new fields, while leaving legacy columns/tables in place where active code still depends on them.

2. **Deferred hard-cut migrations**  
   These are deliberately not part of the normal run sequence yet. They must wait until dependencies are fixed, writes are paused, RLS/grants are handled, and validation is clean.

Do **not** run these deferred files during Phase 1:

```text
900_hard_cut_observations_after_dependencies.sql
901_validate_hard_cut_observations.sql
905_enforce_timeseries_observed_property_not_null.sql
```

## 2. Final target schema decisions

The final v0.2.0 model is:

```text
connectors
networks
stations
station_initial_metadata
station_matches
observed_properties
timeseries
observations
uk_aq_ingest_runs
```

Key locked decisions:

```text
- observations stays in uk_aq_core
- final observations has no connector_id and no network_id
- final observations primary key is (timeseries_id, observed_at)
- timeseries has connector_id but no network_id
- timeseries has observed_property_id directly, no phenomenon_id in the final model
- stations has network_id, match_id and priority
- stations has service_ref as a normal column
- stations unique identity is (connector_id, station_ref)
- timeseries unique identity is (connector_id, timeseries_ref)
- station_initial_metadata replaces station_metadata for initial metadata snapshots
- station_initial_metadata is insert-once and not normally updated
- station_matches has match_key for automated matching
- station_matches.match_key is internal/generated, not an external ref
- networks has public_display_enabled
- stations does not have public_display_enabled
- use stations.removed_at to exclude stations from display
- no generic connector_checkpoints table
- keep connector/network-specific checkpoint tables where needed
```

## 3. Before starting

### Backup

A database backup from this morning exists and is considered sufficient for this test run.

Completed:

- [X] Confirm backup exists and is restorable if needed.

Notes:

```text

```

### Confirm target database

Make sure all commands or SQL editor sessions are connected to the **test** database, not live beta.

Completed:

- [X] Confirm test database connection.
- [X] Confirm project/environment name in Supabase UI.

Notes:

```text

```

### Confirm migration folder

Expected folder:

```text
schemas/migrations/v0.2.0
```

Completed:

- [X] Confirm the corrected Codex migration files are present.
- [X] Confirm `station_matches.match_key` exists in `002_add_v020_core_tables_and_columns.sql`.
- [X] Confirm `900_hard_cut_observations_after_dependencies.sql` is marked deferred and not in the Phase 1 run list.

Notes:

```text
'station_matches.match_key' doesn't exist as text. But match_key is in the station_matches table.
```

## 4. Recommended execution method

Preferred method is `psql`, because it gives better error handling, clearer logs, and exact file execution.

Example:

```bash
psql "$SUPABASE_DB_URL" \
  -v ON_ERROR_STOP=1 \
  -f plans/uk_aq_v0.2.0_sql_final_match_key/migrations/v0.2.0/001_preflight_checks.sql
```

Run one file at a time. Stop on the first error.

Supabase SQL Editor can be used for smaller scripts, but it is less ideal for migration work. See section 9.

Completed:

- [X] Chosen execution method.
- [X] If using `psql`, confirmed `$SUPABASE_DB_URL` points to test.
- [X] If using Supabase UI, confirmed SQL editor points to test project.

Notes:

```text

```

## 5. Phase 1 run order

Run the files in this order.

### Step 1: Preflight checks

File:

```text
001_preflight_checks.sql
```

Purpose:

```text
Checks current state before adding/backfilling v0.2.0 fields.
Looks for duplicate keys, orphan data, placeholder stations, service_ref combinations and mapping coverage.
```

Expected outcome:

```text
No blocker findings.
If it only prints diagnostic rows, review them before continuing.
```

Completed:

- [X] Ran `001_preflight_checks.sql`.
- [X] Reviewed output.
- [X] No blocker found, or blocker understood and fixed.

Notes/output summary:

```text

```

### Step 2: Add v0.2.0 core tables and columns

File:

```text
002_add_v020_core_tables_and_columns.sql
```

Purpose:

```text
Adds networks, station_matches, station_initial_metadata and new v0.2.0 columns/indexes.
Adds station_matches.match_key with nullable unique index.
Adds additive observations/timeseries support without performing hard cut.
```

Expected outcome:

```text
Creates/adds objects without dropping old dependencies.
```

Completed:

- [X] Ran on ingestdb `002_add_v020_core_tables_and_columns.sql`.
- [X] Ran on obsaqidb `002_add_v020_core_tables_and_columns.sql`.
- [X] Confirmed no errors.

Notes/output summary:

```text

```

### Step 3: Seed networks and connectors

File:

```text
003_seed_networks_and_connectors.sql
```

Purpose:

```text
Seeds v0.2.0 networks and connectors.
Renames old Breathe London connector from breathelondon to blondon_communities.
Adds blondon_nodes.
Preserves existing scheduler_backend values for existing connectors.
```

Expected key rows:

```text
Networks:
- gov_uk_aurn, GOV.UK AURN, public_display_enabled true
- breathelondon, Breathe London, public_display_enabled true
- openaq, OpenAQ, public_display_enabled false
- sensorcommunity, Sensor.Community, public_display_enabled false
- laqn, LAQN, public_display_enabled false

Connectors:
- uk_air_sos, UK-AIR SOS
- blondon_communities, Breathe London Communities
- blondon_nodes, Breathe London Nodes
- openaq, OpenAQ
- sensorcommunity, Sensor.Community
```

Completed:

- [X] Ran `003_seed_networks_and_connectors.sql`.
- [X] Confirmed Breathe London old connector became `blondon_communities`.
- [X] Confirmed `blondon_nodes` exists.
- [X] Confirmed `UK-AIR SOS` label/display name is correct.
- [X] Confirmed scheduler_backend was not unexpectedly changed.

Notes/output summary:

```text

```

### Step 4: Migrate station fields and initial metadata

File:

```text
004_migrate_station_fields_and_initial_metadata.sql
```

Purpose:

```text
Backfills stations.network_id, match_id/priority defaults and promoted station columns where possible.
Copies old station_metadata rows into station_initial_metadata.
Does not create station_initial_metadata rows for stations with no old metadata.
Does not update station_initial_metadata on conflict.
```

Important checks:

```text
- station_initial_metadata count should be close to old station_metadata count, not all stations.
- Breathe London device_code should be considered for station_device_ref.
- InstallationCode should remain available for later Breathe London Nodes/Communities matching.
```

Completed:

- [X] Ran `004_migrate_station_fields_and_initial_metadata.sql`.
- [X] Confirmed station_initial_metadata count is sensible.
- [X] Confirmed Breathe London metadata was copied.
- [X] Confirmed station network_id backfill is sensible.

Notes/output summary:

```text

```

### Step 5: Migrate timeseries observed properties

File:

```text
005a_migrate_timeseries_observed_properties.sql
```

Purpose:

```text
Backfills timeseries.observed_property_id using existing phenomena/observed_properties mapping.
Does not drop phenomenon_id yet.
Does not enforce observed_property_id not null yet.
```

Completed:

- [X] Ran `005a_migrate_timeseries_observed_properties.sql`.
- [X] Confirmed no errors.

Notes/output summary:

```text

```

### Step 6: Validate timeseries observed properties

File:

```text
005b_validate_timeseries_observed_properties.sql
```

Purpose:

```text
Lists any timeseries still missing observed_property_id.
This should be diagnostic and should not perform the final not-null hard cut.
```

Completed:

- [X] Ran `005b_validate_timeseries_observed_properties.sql`.
- [X] Reviewed missing mappings, if any.
- [X] No missing mappings, or missing mappings recorded for follow-up.

Notes/output summary:

```text
[
  {
    "check_name": "timeseries_observed_property_mapping_summary",
    "total_timeseries": 5483,
    "mapped_timeseries": 5483,
    "unmapped_timeseries": 0
  }
]
```

### Step 7: Migrate ingest run fields

File:

```text
006_migrate_ingest_runs.sql
```

Purpose:

```text
Adds/backfills uk_aq_ingest_runs.network_id and network_code where obvious.
Keeps existing connector_id and connector_code.
```

Completed:

- [X] Ran `006_migrate_ingest_runs.sql`.
- [X] Confirmed no errors.

Notes/output summary:

```text

```

### Step 8: Migrate placeholder rows

File:

```text
007_migrate_placeholder_rows.sql
```

Purpose:

```text
Sets removed_at for the known UK-AIR SOS placeholder station.
Does not add a placeholder column.
```

Known placeholder:

```text
station_ref = 9999999999
label = GB_SamplingFeature_missingFOI
metadata: is_placeholder true, exclude_from_ui true, placeholder_source uk_air_sos
```

Completed:

- [X] Ran `007_migrate_placeholder_rows.sql`.
- [X] Confirmed placeholder station has removed_at set.

Notes/output summary:

```text

```

### Step 9: Public views and RPC placeholder

File:

```text
008_rebuild_public_views_and_rpcs.sql
```

Purpose:

```text
Currently expected to be a placeholder/TODO until dependency fixes are implemented.
Do not rely on this file to make the website or RPCs v0.2.0-compatible yet.
```

Completed:

- [ ] Reviewed `008_rebuild_public_views_and_rpcs.sql`.
- [ ] Ran it only if it is safe/no-op, or deliberately skipped it.

Notes/output summary:

```text

```

### Step 10: Phase 1 validation checks

File:

```text
009_validation_checks.sql
```

Purpose:

```text
Validates the additive v0.2.0 preparation state.
This is not final hard-cut validation.
```

Completed:

- [X] Ran `009_validation_checks.sql`.
- [X] Reviewed all outputs.
- [X] Recorded remaining warnings/TODOs.

Notes/output summary:

```text
[
  {
    "column_name": "connector_id",
    "data_type": "integer",
    "is_nullable": "NO"
  },
  {
    "column_name": "timeseries_id",
    "data_type": "integer",
    "is_nullable": "NO"
  },
  {
    "column_name": "observed_at",
    "data_type": "timestamp with time zone",
    "is_nullable": "NO"
  },
  {
    "column_name": "metadata",
    "data_type": "jsonb",
    "is_nullable": "NO"
  }
]
```

### Step 11: SQL dependency report checks

File:

```text
010_dependency_report_sql_checks.sql
```

Purpose:

```text
Checks database objects that still depend on old tables/columns.
Especially observations.connector_id, timeseries.phenomenon_id, station_metadata, phenomena, station_network_memberships and uk_aq_networks.
```

Completed:

- [X] Ran `010_dependency_report_sql_checks.sql`.
- [X] Saved output.
- [ ] Identified blockers for hard cut.

Notes/output summary:

```text
[
  {
    "report_section": "observations_grants",
    "grantor": "postgres",
    "grantee": "authenticated",
    "privilege_type": "SELECT",
    "is_grantable": "NO"
  },
  {
    "report_section": "observations_grants",
    "grantor": "postgres",
    "grantee": "postgres",
    "privilege_type": "DELETE",
    "is_grantable": "YES"
  },
  {
    "report_section": "observations_grants",
    "grantor": "postgres",
    "grantee": "postgres",
    "privilege_type": "INSERT",
    "is_grantable": "YES"
  },
  {
    "report_section": "observations_grants",
    "grantor": "postgres",
    "grantee": "postgres",
    "privilege_type": "REFERENCES",
    "is_grantable": "YES"
  },
  {
    "report_section": "observations_grants",
    "grantor": "postgres",
    "grantee": "postgres",
    "privilege_type": "SELECT",
    "is_grantable": "YES"
  },
  {
    "report_section": "observations_grants",
    "grantor": "postgres",
    "grantee": "postgres",
    "privilege_type": "TRIGGER",
    "is_grantable": "YES"
  },
  {
    "report_section": "observations_grants",
    "grantor": "postgres",
    "grantee": "postgres",
    "privilege_type": "TRUNCATE",
    "is_grantable": "YES"
  },
  {
    "report_section": "observations_grants",
    "grantor": "postgres",
    "grantee": "postgres",
    "privilege_type": "UPDATE",
    "is_grantable": "YES"
  },
  {
    "report_section": "observations_grants",
    "grantor": "postgres",
    "grantee": "service_role",
    "privilege_type": "DELETE",
    "is_grantable": "NO"
  },
  {
    "report_section": "observations_grants",
    "grantor": "postgres",
    "grantee": "service_role",
    "privilege_type": "INSERT",
    "is_grantable": "NO"
  },
  {
    "report_section": "observations_grants",
    "grantor": "postgres",
    "grantee": "service_role",
    "privilege_type": "REFERENCES",
    "is_grantable": "NO"
  },
  {
    "report_section": "observations_grants",
    "grantor": "postgres",
    "grantee": "service_role",
    "privilege_type": "SELECT",
    "is_grantable": "NO"
  },
  {
    "report_section": "observations_grants",
    "grantor": "postgres",
    "grantee": "service_role",
    "privilege_type": "TRIGGER",
    "is_grantable": "NO"
  },
  {
    "report_section": "observations_grants",
    "grantor": "postgres",
    "grantee": "service_role",
    "privilege_type": "TRUNCATE",
    "is_grantable": "NO"
  },
  {
    "report_section": "observations_grants",
    "grantor": "postgres",
    "grantee": "service_role",
    "privilege_type": "UPDATE",
    "is_grantable": "NO"
  }
]
```

## 6. Do not run in Phase 1

These files are deferred:

```text
900_hard_cut_observations_after_dependencies.sql
901_validate_hard_cut_observations.sql
905_enforce_timeseries_observed_property_not_null.sql
```

Reason:

```text
They require dependency fixes first.
They may require writes to be paused.
They must handle RLS, policies, grants and permissions.
They must not silently lose rows.
```

Completed:

- [X] Confirmed 900/901/905 were not run during Phase 1.

Notes:

```text
observations still has connector_id
observations metadata is now NOT NULL
current observations grants were captured
dependency report confirms hard cut is still blocked
```

## 7. After Phase 1 completes

Next work after a successful additive test migration:

```text
1. Use dependency report output to update affected SQL views/RPCs.
2. Update ingest scripts to write/read v0.2.0 fields.
3. Update AQI jobs and views.
4. Update website/public API queries.
5. Re-test ingests and website on test.
6. Only then prepare the deferred hard cut.
```

Completed:

- [ ] Dependency report reviewed.
- [ ] Blocker list created.
- [ ] Code update tasks created.

Notes:

```text

```

## 8. Hard cut readiness checklist, later

Do not start hard cut until all items are complete:

```text
- RPCs no longer insert observations.connector_id
- RPCs use final observations key (timeseries_id, observed_at)
- views no longer require old observations connector_id
- AQI jobs work with final observation structure
- ingests work with timeseries.observed_property_id
- public views/RPCs use stations.network_id -> networks.id
- station_metadata dependencies replaced with station_initial_metadata where needed
- timeseries.phenomenon_id dependencies removed
- RLS/grants/policies plan written
- writes can be paused for the hard cut
- collision/null/orphan preconditions pass
- backup exists
```

Completed:

- [ ] Hard cut readiness not started yet.

Notes:

```text

```

## 9. Can the SQL be run in the Supabase UI?

Yes, for the Phase 1 additive scripts you can usually copy and paste each SQL file into the Supabase SQL Editor and run it one file at a time.

However, `psql` is preferred for migrations because:

```text
- ON_ERROR_STOP makes failures clearer
- output can be saved more easily
- large scripts are easier to rerun/debug
- it avoids browser/session interruptions
- exact file order is easier to control
```

If using the Supabase UI:

```text
- run one migration file at a time
- check the output before running the next file
- stop at the first error
- do not run 900/901/905
- save/copy the output from preflight, validation and dependency checks
- confirm you are in the test project before every run
```

Do not paste all migration files into one huge editor window. Run them individually in order.

Completed:

- [ ] Decided whether to use Supabase UI or psql.

Notes:

```text

```

## 10. Rollback notes

Since this is the test database and a backup exists, rollback options are:

```text
1. Restore the test database from backup.
2. If only an early additive step failed, inspect the failed migration and either fix/re-run or manually undo the added object.
```

Because this is an additive Phase 1 run, the expected rollback need is low, but stop immediately on errors.

Notes:

```text

```

## 11. Final status

Phase 1 additive migration status:

```text
Not started / In progress / Completed / Blocked
```

Overall notes:

```text

```
