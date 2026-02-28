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

-- 2) Capture and drop impacted constraints.
create temporary table _int4_id_constraints (
  table_name regclass not null,
  constraint_name text not null,
  constraint_type "char" not null,
  constraint_def text not null,
  primary key (table_name, constraint_name)
) on commit drop;

insert into _int4_id_constraints (table_name, constraint_name, constraint_type, constraint_def)
select distinct
  c.conrelid::regclass,
  c.conname,
  c.contype,
  pg_get_constraintdef(c.oid) as constraint_def
from pg_constraint c
join pg_class rel on rel.oid = c.conrelid
join pg_namespace n on n.oid = rel.relnamespace
where n.nspname in ('uk_aq_core', 'uk_aq_raw', 'uk_aq_history')
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
      table_name::text,
      constraint_name
  loop
    execute format(
      'alter table %s drop constraint %I',
      v_row.table_name,
      v_row.constraint_name
    );
  end loop;
end
$$;

-- 3) Alter target columns.
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

-- 4) Recreate constraints (PK/UNIQUE/CHECK first, FK last).
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
      table_name::text,
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

commit;
