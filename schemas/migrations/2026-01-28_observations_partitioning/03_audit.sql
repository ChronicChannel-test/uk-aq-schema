-- 03_audit.sql
-- Observations partition migration audit (uk_aq_core.observations).
-- Read-only checks; no data changes.

set search_path = uk_aq_core, public;

-- 1) Parent table partitioning + key
select
  n.nspname as schema_name,
  c.relname as table_name,
  p.partstrat as partition_strategy,
  pg_get_partkeydef(c.oid) as partition_key
from pg_partitioned_table p
join pg_class c on c.oid = p.partrelid
join pg_namespace n on n.oid = c.relnamespace
where n.nspname = 'uk_aq_core'
  and c.relname = 'observations';

-- 1a) Primary key definition on parent
select
  conname,
  pg_get_constraintdef(oid) as constraint_def
from pg_constraint
where conrelid = 'uk_aq_core.observations'::regclass
  and contype = 'p';

-- 1b) Partition list (bounds) + default partition
-- Note: pg_partition_tree().bound is not available in older PostgreSQL/Supabase.
select
  pt.relid::regclass as partition_name,
  pt.isleaf as is_leaf,
  pt.level as level,
  pg_get_expr(c.relpartbound, c.oid) as partition_bound
from pg_partition_tree('uk_aq_core.observations'::regclass) pt
join pg_class c on c.oid = pt.relid
order by pt.level, pt.relid::regclass::text;

-- 1c) Connectors that do NOT have a dedicated partition
-- (Uses name heuristic: observations_new_c_<id> based on current migration naming.)
select
  c.id as connector_id
from uk_aq_core.connectors c
left join pg_class child on child.relname = format('observations_new_c_%s', replace(c.id::text, '-', '_'))
left join pg_namespace nsp on nsp.oid = child.relnamespace and nsp.nspname = 'uk_aq_core'
where child.oid is null;

-- 2) Foreign keys referencing uk_aq_core.observations
select
  conrelid::regclass as referencing_table,
  conname,
  pg_get_constraintdef(oid) as constraint_def
from pg_constraint
where confrelid = 'uk_aq_core.observations'::regclass
  and contype = 'f'
order by conrelid::regclass::text, conname;

-- 3) SQL functions/views that reference observations
-- Functions
select
  n.nspname as schema_name,
  p.proname as function_name,
  p.prosrc as function_body
from pg_proc p
join pg_namespace n on n.oid = p.pronamespace
where p.prosrc ilike '%observations%'
  and n.nspname in ('uk_aq_core','uk_aq_public','public')
order by n.nspname, p.proname;

-- Views (public + core)
select
  schemaname as schema_name,
  viewname as view_name,
  definition as view_def
from pg_views
where schemaname in ('uk_aq_core','uk_aq_public','public')
  and definition ilike '%observations%'
order by schemaname, viewname;

-- Materialized views (if any)
select
  schemaname as schema_name,
  matviewname as view_name,
  definition as view_def
from pg_matviews
where schemaname in ('uk_aq_core','uk_aq_public','public')
  and definition ilike '%observations%'
order by schemaname, matviewname;

-- Dependency list for views/materialized views that depend on observations
select
  n.nspname as schema_name,
  c.relname as view_name,
  c.relkind
from pg_depend d
join pg_rewrite r on r.oid = d.objid
join pg_class c on c.oid = r.ev_class
join pg_namespace n on n.oid = c.relnamespace
where d.refobjid = 'uk_aq_core.observations'::regclass
  and c.relkind in ('v','m')
order by n.nspname, c.relname;

-- 4) Public view signature check
select
  column_name,
  data_type,
  ordinal_position
from information_schema.columns
where table_schema = 'uk_aq_public'
  and table_name = 'observations'
order by ordinal_position;

-- 4a) Latest-style view query sanity (non-destructive)
-- Returns up to 10 latest rows, ordered by observed_at.
select
  timeseries_id,
  observed_at,
  value,
  status
from uk_aq_public.observations
order by observed_at desc
limit 10;

-- 5) Index audit (partition-level): ensure (timeseries_id, observed_at desc)
with partitions as (
  select pt.relid::regclass as partition_name
  from pg_partition_tree('uk_aq_core.observations'::regclass) pt
  where pt.isleaf is true
)
select
  p.partition_name,
  i.indexrelid::regclass as index_name,
  pg_get_indexdef(i.indexrelid) as index_def
from partitions p
join pg_index i on i.indrelid = p.partition_name::regclass
order by p.partition_name::text, i.indexrelid::regclass::text;

-- 5) Index presence check per partition (timeseries_id, observed_at desc)
with partitions as (
  select pt.relid::regclass as partition_name
  from pg_partition_tree('uk_aq_core.observations'::regclass) pt
  where pt.isleaf is true
)
select
  p.partition_name,
  exists (
    select 1
    from pg_index i
    where i.indrelid = p.partition_name::regclass
      and pg_get_indexdef(i.indexrelid) ilike '%(timeseries_id, observed_at desc%'
  ) as has_ts_time_desc_idx
from partitions p
order by p.partition_name::text;

-- 5a) Redundant index check: any multiple indexes on observed_at alone
select
  p.partition_name,
  i.indexrelid::regclass as index_name,
  pg_get_indexdef(i.indexrelid) as index_def
from (
  select pt.relid::regclass as partition_name
  from pg_partition_tree('uk_aq_core.observations'::regclass) pt
  where pt.isleaf is true
) p
join pg_index i on i.indrelid = p.partition_name::regclass
where pg_get_indexdef(i.indexrelid) ilike '%(observed_at)%'
order by p.partition_name::text, i.indexrelid::regclass::text;

-- 6) Data integrity: connector_id mismatch vs timeseries
select
  count(*) as mismatch_count
from uk_aq_core.observations o
join uk_aq_core.timeseries t on t.id = o.timeseries_id
where o.connector_id <> t.connector_id;

-- 6a) Sample mismatches (if any)
select
  o.connector_id as observations_connector_id,
  t.connector_id as timeseries_connector_id,
  o.timeseries_id,
  o.observed_at
from uk_aq_core.observations o
join uk_aq_core.timeseries t on t.id = o.timeseries_id
where o.connector_id <> t.connector_id
order by o.observed_at desc
limit 50;

-- 7) Default partition row count (should be 0 if all connectors have partitions)
select
  count(*) as default_partition_rows
from uk_aq_core.observations_default;
