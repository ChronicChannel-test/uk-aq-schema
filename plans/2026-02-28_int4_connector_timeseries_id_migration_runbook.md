# Int4 Migration Runbook: connector/timeseries IDs

Date: 2026-02-28

Scope:
- Convert `uk_aq_core.connectors.id` and `uk_aq_core.timeseries.id` to `integer` (`int4`)
- Convert every table column named `connector_id` or `timeseries_id` to `integer`
- Keep behavior unchanged (type-width change only)

Files:
- Preflight SQL: `schemas/migrations/2026-02-28_connector_timeseries_ids_int4_preflight.sql`
- Forward SQL: `schemas/migrations/2026-02-28_connector_timeseries_ids_int4_forward.sql`
- Rollback SQL: `schemas/migrations/2026-02-28_connector_timeseries_ids_int4_rollback.sql`

## 1) Preflight checks

Run on each target DB (MAIN and HISTORY):

```sql
\i schemas/migrations/2026-02-28_connector_timeseries_ids_int4_preflight.sql
```

Expected:
- Notices for max values per target column
- No exception

Fail condition:
- Any max value > `2147483647` (migration must stop)

## 2) Apply order

Apply in this order.

1. MAIN DB:
```sql
\i schemas/migrations/2026-02-28_connector_timeseries_ids_int4_forward.sql
```

2. MAIN DB schema refresh (to align function signatures + grants):
```sql
\i schemas/main_db/uk_aq_core_schema.sql
\i schemas/main_db/uk_aq_raw_schema.sql
\i schemas/main_db/uk_aq_rpc.sql
\i schemas/main_db/main_db_dualwrite_bootstrap.sql
```

3. HISTORY DB:
```sql
\i schemas/migrations/2026-02-28_connector_timeseries_ids_int4_forward.sql
```

4. HISTORY DB schema refresh:
```sql
\i schemas/history_db/uk_aq_history_schema.sql
\i schemas/history_db/history_db_dualwrite_bootstrap.sql
```

5. Ops RPC refresh:
- In ingest DB, apply `CIC-test-uk-aq-ops/sql/ingest_db_ops_rpcs.sql`
- In history DB, apply `CIC-test-uk-aq-ops/sql/history_db_ops_rpcs.sql`

6. Ingest helper refresh:
- Apply `CIC-test-uk-aq-ingest/supabase/uk_aq_station_snapshot.sql`
- Apply `CIC-test-uk-aq-ingest/supabase/uk_aq_polling_helpers.sql`

## 3) Post-migration validation

Run on each DB.

### A. Type checks
```sql
select table_schema, table_name, column_name, data_type
from information_schema.columns
where table_schema in ('uk_aq_core','uk_aq_raw','uk_aq_history')
  and (
    (table_schema = 'uk_aq_core' and table_name in ('connectors','timeseries') and column_name = 'id')
    or column_name in ('connector_id','timeseries_id')
  )
order by table_schema, table_name, column_name;
```

Expected:
- `integer` for all returned rows

### B. FK datatype parity checks
```sql
select
  con.conname,
  n1.nspname as child_schema,
  c1.relname as child_table,
  a1.attname as child_column,
  format_type(a1.atttypid, a1.atttypmod) as child_type,
  n2.nspname as parent_schema,
  c2.relname as parent_table,
  a2.attname as parent_column,
  format_type(a2.atttypid, a2.atttypmod) as parent_type
from pg_constraint con
join pg_class c1 on c1.oid = con.conrelid
join pg_namespace n1 on n1.oid = c1.relnamespace
join pg_class c2 on c2.oid = con.confrelid
join pg_namespace n2 on n2.oid = c2.relnamespace
join unnest(con.conkey) with ordinality as ck(attnum, ord) on true
join unnest(con.confkey) with ordinality as fk(attnum, ord) on fk.ord = ck.ord
join pg_attribute a1 on a1.attrelid = c1.oid and a1.attnum = ck.attnum
join pg_attribute a2 on a2.attrelid = c2.oid and a2.attnum = fk.attnum
where con.contype = 'f'
  and (
    a1.attname in ('connector_id', 'timeseries_id')
    or (n2.nspname = 'uk_aq_core' and c2.relname in ('connectors','timeseries') and a2.attname = 'id')
  )
order by con.conname, child_schema, child_table;
```

Expected:
- Child and parent types match (`integer`)

### C. Function signature spot checks
```sql
select n.nspname, p.proname, pg_get_function_identity_arguments(p.oid) as args
from pg_proc p
join pg_namespace n on n.oid = p.pronamespace
where n.nspname in ('uk_aq_public','uk_aq_core')
  and p.proname in (
    'uk_aq_latest_rpc',
    'uk_aq_timeseries_rpc',
    'uk_aq_stations_rpc',
    'uk_aq_rpc_station_names',
    'uk_aq_rpc_timeseries_ids',
    'uk_aq_rpc_observations_delete_hour_bucket',
    'uk_aq_rpc_history_outbox_enqueue_hour_bucket'
  )
order by n.nspname, p.proname;
```

Expected:
- Connector/timeseries ID arguments show `integer`.

## 4) Rollback

If validation fails and rollback is required:

1. MAIN DB:
```sql
\i schemas/migrations/2026-02-28_connector_timeseries_ids_int4_rollback.sql
```
2. HISTORY DB:
```sql
\i schemas/migrations/2026-02-28_connector_timeseries_ids_int4_rollback.sql
```
3. Re-apply previous schema snapshot/files if needed.

## 5) Notes / risk controls

- This migration drops and recreates constraints that touch these ID columns.
- Run in a maintenance window for non-live DB only.
- Re-apply schema SQL immediately after migration so function signatures and grants stay aligned.
