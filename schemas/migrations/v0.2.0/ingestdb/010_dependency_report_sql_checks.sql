-- UK AQ v0.2.0 phase 1 dependency report.
-- Read-only. This file reports dependencies and performs no DDL or DML.
--
-- Save and review all result sets before designing the 900-series hard cut.

set search_path = uk_aq_core, uk_aq_public, public, pg_catalog;

-- Confirm which transitional and legacy columns/tables are present.
with target_columns(table_schema, table_name, column_name) as (
  values
    ('uk_aq_core', 'observations', 'connector_id'),
    ('uk_aq_core', 'timeseries', 'phenomenon_id'),
    ('uk_aq_core', 'timeseries', 'offering_id'),
    ('uk_aq_core', 'timeseries', 'feature_id'),
    ('uk_aq_core', 'timeseries', 'procedure_id'),
    ('uk_aq_core', 'timeseries', 'category_id'),
    ('uk_aq_core', 'stations', 'category_id')
)
select
  'target_column_inventory' as report_section,
  t.table_schema,
  t.table_name,
  t.column_name,
  (c.column_name is not null) as exists_now,
  c.data_type,
  c.is_nullable
from target_columns t
left join information_schema.columns c
  on c.table_schema = t.table_schema
 and c.table_name = t.table_name
 and c.column_name = t.column_name
order by t.table_name, t.column_name;

with target_tables(table_schema, table_name) as (
  values
    ('uk_aq_core', 'station_metadata'),
    ('uk_aq_core', 'phenomena')
)
select
  'target_table_inventory' as report_section,
  t.table_schema,
  t.table_name,
  (i.table_name is not null) as exists_now,
  i.table_type
from target_tables t
left join information_schema.tables i
  on i.table_schema = t.table_schema
 and i.table_name = t.table_name
order by t.table_name;

-- Catalog-tracked dependencies on specific legacy columns.
with target_columns as (
  select
    c.oid as relation_oid,
    n.nspname as table_schema,
    c.relname as table_name,
    a.attnum,
    a.attname as column_name
  from pg_class c
  join pg_namespace n on n.oid = c.relnamespace
  join pg_attribute a on a.attrelid = c.oid
  where n.nspname = 'uk_aq_core'
    and (
      (c.relname = 'observations' and a.attname = 'connector_id')
      or
      (c.relname = 'timeseries' and a.attname in (
        'phenomenon_id',
        'offering_id',
        'feature_id',
        'procedure_id',
        'category_id'
      ))
      or
      (c.relname = 'stations' and a.attname = 'category_id')
    )
)
select
  'catalog_column_dependencies' as report_section,
  tc.table_schema,
  tc.table_name,
  tc.column_name,
  pg_describe_object(d.classid, d.objid, d.objsubid) as dependent_object,
  d.deptype
from target_columns tc
join pg_depend d
  on d.refobjid = tc.relation_oid
 and d.refobjsubid = tc.attnum
order by tc.table_name, tc.column_name, dependent_object;

-- Catalog-tracked dependencies on legacy relations.
with target_relations as (
  select c.oid, n.nspname as table_schema, c.relname as table_name
  from pg_class c
  join pg_namespace n on n.oid = c.relnamespace
  where n.nspname = 'uk_aq_core'
    and c.relname in (
      'station_metadata',
      'phenomena'
    )
)
select
  'catalog_relation_dependencies' as report_section,
  tr.table_schema,
  tr.table_name,
  pg_describe_object(d.classid, d.objid, d.objsubid) as dependent_object,
  d.deptype
from target_relations tr
join pg_depend d
  on d.refobjid = tr.oid
where not (
  d.classid = 'pg_class'::regclass
  and d.objid = tr.oid
)
order by tr.table_name, dependent_object;

-- View definitions may expose text-level dependencies more clearly than
-- pg_depend, especially for joins and projected legacy columns.
select
  'view_definition_matches' as report_section,
  v.schemaname as object_schema,
  v.viewname as object_name,
  'view' as object_type,
  v.definition
from pg_views v
where v.schemaname not in ('pg_catalog', 'information_schema')
  and v.definition ~* (
    'station_metadata|'
    'phenomena|phenomenon_id|offering_id|feature_id|procedure_id|category_id|'
    'observations[^;]*connector_id'
  )
union all
select
  'view_definition_matches',
  m.schemaname,
  m.matviewname,
  'materialized view',
  m.definition
from pg_matviews m
where m.schemaname not in ('pg_catalog', 'information_schema')
  and m.definition ~* (
    'station_metadata|'
    'phenomena|phenomenon_id|offering_id|feature_id|procedure_id|category_id|'
    'observations[^;]*connector_id'
  )
order by object_schema, object_type, object_name;

-- Function source scan catches dynamic SQL and PL/pgSQL references that may
-- not have useful catalog dependencies until runtime.
with function_definitions as materialized (
  select
    p.oid,
    p.pronamespace,
    p.prolang,
    p.proname,
    pg_get_function_identity_arguments(p.oid) as identity_arguments,
    pg_get_functiondef(p.oid) as function_definition
  from pg_proc p
  where p.prokind in ('f', 'p')
)
select
  'function_definition_matches' as report_section,
  n.nspname as function_schema,
  f.proname as function_name,
  f.identity_arguments,
  l.lanname as language_name,
  f.function_definition
from function_definitions f
join pg_namespace n on n.oid = f.pronamespace
join pg_language l on l.oid = f.prolang
where n.nspname not in ('pg_catalog', 'information_schema')
  and f.function_definition ~* (
    'station_metadata|'
    'phenomena|phenomenon_id|offering_id|feature_id|procedure_id|category_id|'
    'connector_id[^[:alnum:]_]+timeseries_id[^[:alnum:]_]+observed_at'
  )
order by n.nspname, f.proname, identity_arguments;

-- Inventory current observation keys, indexes and conflict-key assumptions.
select
  'observations_constraints' as report_section,
  con.conname,
  con.contype,
  con.convalidated,
  pg_get_constraintdef(con.oid, true) as definition
from pg_constraint con
where con.conrelid = 'uk_aq_core.observations'::regclass
order by con.contype, con.conname;

select
  'observations_indexes' as report_section,
  schemaname,
  indexname,
  indexdef
from pg_indexes
where schemaname = 'uk_aq_core'
  and tablename = 'observations'
order by indexname;

with function_definitions as materialized (
  select
    p.oid,
    p.pronamespace,
    p.proname,
    pg_get_function_identity_arguments(p.oid) as identity_arguments,
    pg_get_functiondef(p.oid) as function_definition
  from pg_proc p
  where p.prokind in ('f', 'p')
)
select
  'old_observation_key_function_matches' as report_section,
  n.nspname as function_schema,
  f.proname as function_name,
  f.identity_arguments,
  f.function_definition
from function_definitions f
join pg_namespace n on n.oid = f.pronamespace
where n.nspname not in ('pg_catalog', 'information_schema')
  and f.function_definition ~* (
    'on[[:space:]]+conflict[[:space:]]*[(][[:space:]]*connector_id[[:space:]]*,'
    '[[:space:]]*timeseries_id[[:space:]]*,[[:space:]]*observed_at[[:space:]]*[)]'
  )
order by n.nspname, f.proname, identity_arguments;

-- Capture security and ancillary objects that a replacement table must
-- reproduce explicitly.
select
  'observations_security' as report_section,
  n.nspname as table_schema,
  c.relname as table_name,
  pg_get_userbyid(c.relowner) as owner_name,
  c.relrowsecurity as rls_enabled,
  c.relforcerowsecurity as rls_forced,
  obj_description(c.oid, 'pg_class') as table_comment
from pg_class c
join pg_namespace n on n.oid = c.relnamespace
where n.nspname = 'uk_aq_core'
  and c.relname = 'observations';

select
  'observations_policies' as report_section,
  schemaname,
  tablename,
  policyname,
  permissive,
  roles,
  cmd,
  qual,
  with_check
from pg_policies
where schemaname = 'uk_aq_core'
  and tablename = 'observations'
order by policyname;

select
  'observations_grants' as report_section,
  grantor,
  grantee,
  privilege_type,
  is_grantable
from information_schema.role_table_grants
where table_schema = 'uk_aq_core'
  and table_name = 'observations'
order by grantee, privilege_type;

select
  'observations_triggers' as report_section,
  trigger_name,
  event_manipulation,
  action_timing,
  action_statement
from information_schema.triggers
where event_object_schema = 'uk_aq_core'
  and event_object_table = 'observations'
order by trigger_name, event_manipulation;
