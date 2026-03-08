-- Forward migration: convert connectors/timeseries IDs and all connector_id/timeseries_id columns to int4.
-- Safe for MAIN/HISTORY DBs; only touches columns that exist and are currently bigint.

begin;

-- 1) Preflight guardrail inside migration.
do $$
declare
  v_row record;
  v_max numeric;
  v_limit constant numeric := 2147483647;
begin
  for v_row in
    with target_columns as (
      select
        n.nspname as schema_name,
        c.relname as table_name,
        a.attname as column_name
      from pg_class c
      join pg_namespace n on n.oid = c.relnamespace
      join pg_attribute a on a.attrelid = c.oid
      where c.relkind in ('r', 'p')
        and a.attnum > 0
        and not a.attisdropped
        and n.nspname in ('uk_aq_core', 'uk_aq_raw', 'uk_aq_history')
        and (
          (n.nspname = 'uk_aq_core' and c.relname = 'connectors' and a.attname = 'id')
          or (n.nspname = 'uk_aq_core' and c.relname = 'timeseries' and a.attname = 'id')
          or a.attname in ('connector_id', 'timeseries_id')
        )
    )
    select distinct schema_name, table_name, column_name
    from target_columns
  loop
    execute format(
      'select max(%I)::numeric from %I.%I',
      v_row.column_name,
      v_row.schema_name,
      v_row.table_name
    )
    into v_max;

    if v_max is not null and v_max > v_limit then
      raise exception
        'int4 migration blocked: %.%.% has max % (> %)',
        v_row.schema_name,
        v_row.table_name,
        v_row.column_name,
        v_max,
        v_limit;
    end if;
  end loop;
end
$$;

-- 2) Capture/drop dependent views and materialized views.
create temporary table _int4_dependent_views (
  view_oid oid primary key,
  schema_name text not null,
  view_name text not null,
  relkind "char" not null,
  max_depth int not null,
  view_def text not null,
  reloptions text[]
) on commit drop;

insert into _int4_dependent_views (
  view_oid,
  schema_name,
  view_name,
  relkind,
  max_depth,
  view_def,
  reloptions
)
with recursive seed_relations as (
  select distinct c.oid as rel_oid
  from pg_class c
  join pg_namespace n on n.oid = c.relnamespace
  join pg_attribute a on a.attrelid = c.oid
  where c.relkind in ('r', 'p')
    and a.attnum > 0
    and not a.attisdropped
    and n.nspname in ('uk_aq_core', 'uk_aq_raw', 'uk_aq_history')
    and (
      (n.nspname = 'uk_aq_core' and c.relname = 'connectors' and a.attname = 'id')
      or (n.nspname = 'uk_aq_core' and c.relname = 'timeseries' and a.attname = 'id')
      or a.attname in ('connector_id', 'timeseries_id')
    )
),
view_graph as (
  select
    rel_oid,
    0 as depth,
    array[rel_oid]::oid[] as path
  from seed_relations
  union all
  select
    v.oid as rel_oid,
    vg.depth + 1 as depth,
    vg.path || v.oid
  from view_graph vg
  join pg_depend d
    on d.refclassid = 'pg_class'::regclass
   and d.refobjid = vg.rel_oid
  join pg_rewrite rw
    on rw.oid = d.objid
   and d.classid = 'pg_rewrite'::regclass
  join pg_class v
    on v.oid = rw.ev_class
   and v.relkind in ('v', 'm')
  where not v.oid = any(vg.path)
),
dependent_views as (
  select
    vg.rel_oid as view_oid,
    max(vg.depth) as max_depth
  from view_graph vg
  join pg_class v on v.oid = vg.rel_oid
  join pg_namespace vn on vn.oid = v.relnamespace
  where v.relkind in ('v', 'm')
    and vn.nspname not in ('pg_catalog', 'information_schema')
    and vn.nspname not like 'pg_toast%'
  group by vg.rel_oid
)
select
  v.oid as view_oid,
  vn.nspname as schema_name,
  v.relname as view_name,
  v.relkind,
  dv.max_depth,
  pg_get_viewdef(v.oid, true) as view_def,
  v.reloptions
from dependent_views dv
join pg_class v on v.oid = dv.view_oid
join pg_namespace vn on vn.oid = v.relnamespace
order by dv.max_depth desc, vn.nspname, v.relname;

do $$
declare
  v_row record;
begin
  for v_row in
    select schema_name, view_name, relkind, max_depth
    from _int4_dependent_views
    order by max_depth desc, schema_name, view_name
  loop
    if v_row.relkind = 'm' then
      execute format(
        'drop materialized view if exists %I.%I',
        v_row.schema_name,
        v_row.view_name
      );
    else
      execute format(
        'drop view if exists %I.%I',
        v_row.schema_name,
        v_row.view_name
      );
    end if;
  end loop;
end
$$;

-- 3) Capture and drop impacted constraints.
create temporary table _int4_id_constraints (
  table_name text not null,
  constraint_name text not null,
  constraint_type "char" not null,
  constraint_def text not null,
  primary key (table_name, constraint_name)
) on commit drop;

insert into _int4_id_constraints (table_name, constraint_name, constraint_type, constraint_def)
select distinct
  format('%I.%I', n.nspname, rel.relname) as table_name,
  c.conname,
  c.contype,
  pg_get_constraintdef(c.oid) as constraint_def
from pg_constraint c
join pg_class rel on rel.oid = c.conrelid
join pg_namespace n on n.oid = rel.relnamespace
where n.nspname in ('uk_aq_core', 'uk_aq_raw', 'uk_aq_history')
  and c.conparentid = 0
  and not rel.relispartition
  and c.contype in ('p', 'u', 'f', 'c')
  and (
    exists (
      select 1
      from unnest(c.conkey) as key_attnum(attnum)
      join pg_attribute a
        on a.attrelid = c.conrelid
       and a.attnum = key_attnum.attnum
      where a.attname in ('connector_id', 'timeseries_id')
         or (
           a.attname = 'id'
           and n.nspname = 'uk_aq_core'
           and rel.relname in ('connectors', 'timeseries')
         )
    )
    or (
      c.contype = 'f'
      and exists (
        select 1
        from unnest(c.confkey) as parent_attnum(attnum)
        join pg_attribute parent_a
          on parent_a.attrelid = c.confrelid
         and parent_a.attnum = parent_attnum.attnum
        join pg_class parent_rel on parent_rel.oid = c.confrelid
        join pg_namespace parent_n on parent_n.oid = parent_rel.relnamespace
        where parent_n.nspname = 'uk_aq_core'
          and parent_rel.relname in ('connectors', 'timeseries')
          and parent_a.attname = 'id'
      )
    )
  );

do $$
declare
  v_row record;
begin
  for v_row in
    select table_name, constraint_name
    from _int4_id_constraints
    order by
      case constraint_type
        when 'f' then 1
        when 'c' then 2
        when 'u' then 3
        when 'p' then 4
        else 5
      end,
      table_name,
      constraint_name
  loop
    execute format(
      'alter table %s drop constraint if exists %I',
      v_row.table_name,
      v_row.constraint_name
    );
  end loop;
end
$$;

create temporary table _int4_observations_partitions (
  ord int generated by default as identity primary key,
  schema_name text not null,
  old_partition_name text not null,
  new_partition_name text not null,
  bound_expr text not null
) on commit drop;

do $$
declare
  v_part record;
begin
  if exists (
    select 1
    from pg_attribute a
    join pg_class c on c.oid = a.attrelid
    join pg_namespace n on n.oid = c.relnamespace
    where n.nspname = 'uk_aq_core'
      and c.relname = 'observations'
      and a.attname = 'connector_id'
      and a.atttypid = 'int8'::regtype
      and not a.attisdropped
  ) then
    delete from _int4_observations_partitions;

    insert into _int4_observations_partitions (
      schema_name,
      old_partition_name,
      new_partition_name,
      bound_expr
    )
    select
      child_ns.nspname as schema_name,
      child.relname as old_partition_name,
      format('%s__i4m_%s', left(child.relname, 40), child.oid::text) as new_partition_name,
      pg_get_expr(child.relpartbound, child.oid) as bound_expr
    from pg_class parent
    join pg_namespace parent_ns on parent_ns.oid = parent.relnamespace
    join pg_inherits inh on inh.inhparent = parent.oid
    join pg_class child on child.oid = inh.inhrelid
    join pg_namespace child_ns on child_ns.oid = child.relnamespace
    where parent_ns.nspname = 'uk_aq_core'
      and parent.relname = 'observations'
    order by child.relname;

    execute $q$
      create table uk_aq_core.observations__int4_mig (
        connector_id integer not null,
        timeseries_id integer,
        observed_at timestamptz not null,
        value double precision,
        status text,
        created_at timestamptz default now()
      ) partition by list (connector_id)
    $q$;

    for v_part in
      select schema_name, new_partition_name, bound_expr
      from _int4_observations_partitions
      order by ord
    loop
      execute format(
        'create table %I.%I partition of uk_aq_core.observations__int4_mig %s',
        v_part.schema_name,
        v_part.new_partition_name,
        v_part.bound_expr
      );
    end loop;

    execute $q$
      insert into uk_aq_core.observations__int4_mig (
        connector_id,
        timeseries_id,
        observed_at,
        value,
        status,
        created_at
      )
      select
        connector_id::integer,
        timeseries_id::integer,
        observed_at,
        value,
        status,
        created_at
      from uk_aq_core.observations
    $q$;

    execute 'drop table uk_aq_core.observations';
    execute 'alter table uk_aq_core.observations__int4_mig rename to observations';

    for v_part in
      select schema_name, new_partition_name, old_partition_name
      from _int4_observations_partitions
      order by ord
    loop
      execute format(
        'alter table %I.%I rename to %I',
        v_part.schema_name,
        v_part.new_partition_name,
        v_part.old_partition_name
      );
    end loop;

    execute 'create index if not exists observations_time_idx on uk_aq_core.observations(observed_at)';
  end if;
end
$$;

-- 4) Alter target columns.
do $$
declare
  v_col record;
begin
  for v_col in
    select
      n.nspname as schema_name,
      c.relname as table_name,
      a.attname as column_name
    from pg_class c
    join pg_namespace n on n.oid = c.relnamespace
    join pg_attribute a on a.attrelid = c.oid
    join pg_type t on t.oid = a.atttypid
    where c.relkind in ('r', 'p')
      and not c.relispartition
      and a.attnum > 0
      and not a.attisdropped
      and n.nspname in ('uk_aq_core', 'uk_aq_raw', 'uk_aq_history')
      and t.typname = 'int8'
      and (
        (n.nspname = 'uk_aq_core' and c.relname = 'connectors' and a.attname = 'id')
        or (n.nspname = 'uk_aq_core' and c.relname = 'timeseries' and a.attname = 'id')
        or a.attname in ('connector_id', 'timeseries_id')
      )
    order by
      case
        when n.nspname = 'uk_aq_core' and c.relname = 'connectors' and a.attname = 'id' then 1
        when n.nspname = 'uk_aq_core' and c.relname = 'timeseries' and a.attname = 'id' then 2
        else 3
      end,
      n.nspname,
      c.relname,
      a.attname
  loop
    execute format(
      'alter table %I.%I alter column %I type integer using %I::integer',
      v_col.schema_name,
      v_col.table_name,
      v_col.column_name,
      v_col.column_name
    );
  end loop;
end
$$;

-- 5) Recreate constraints (PK/UNIQUE/CHECK first, FK last).
do $$
declare
  v_row record;
begin
  for v_row in
    select table_name, constraint_name, constraint_def
    from _int4_id_constraints
    order by
      case constraint_type
        when 'p' then 1
        when 'u' then 2
        when 'c' then 3
        when 'f' then 4
        else 5
      end,
      table_name,
      constraint_name
  loop
    execute format(
      'alter table %s add constraint %I %s',
      v_row.table_name,
      v_row.constraint_name,
      v_row.constraint_def
    );
  end loop;
end
$$;

-- 6) Recreate dropped views/materialized views.
do $$
declare
  v_row record;
  v_opt text;
begin
  for v_row in
    select schema_name, view_name, relkind, view_def, reloptions, max_depth
    from _int4_dependent_views
    order by max_depth asc, schema_name, view_name
  loop
    if v_row.relkind = 'm' then
      execute format(
        'create materialized view %I.%I as %s with no data',
        v_row.schema_name,
        v_row.view_name,
        v_row.view_def
      );
    else
      execute format(
        'create or replace view %I.%I as %s',
        v_row.schema_name,
        v_row.view_name,
        v_row.view_def
      );
    end if;

    if v_row.reloptions is not null then
      foreach v_opt in array v_row.reloptions
      loop
        if v_row.relkind = 'm' then
          execute format(
            'alter materialized view %I.%I set (%s)',
            v_row.schema_name,
            v_row.view_name,
            v_opt
          );
        else
          execute format(
            'alter view %I.%I set (%s)',
            v_row.schema_name,
            v_row.view_name,
            v_opt
          );
        end if;
      end loop;
    end if;
  end loop;
end
$$;

commit;
