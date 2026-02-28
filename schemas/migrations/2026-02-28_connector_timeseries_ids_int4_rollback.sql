-- Rollback migration: revert connectors/timeseries IDs and connector_id/timeseries_id columns to bigint.
-- Safe for MAIN/HISTORY DBs; only touches columns that exist and are currently integer.

begin;

-- 1) Capture and drop impacted constraints.
create temporary table _int8_id_constraints (
  table_name regclass not null,
  constraint_name text not null,
  constraint_type "char" not null,
  constraint_def text not null,
  primary key (table_name, constraint_name)
) on commit drop;

insert into _int8_id_constraints (table_name, constraint_name, constraint_type, constraint_def)
select distinct
  c.conrelid::regclass,
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
    from _int8_id_constraints
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
      'alter table %s drop constraint if exists %I',
      v_row.table_name,
      v_row.constraint_name
    );
  end loop;
end
$$;

-- 2) Alter target columns back to bigint.
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
      and t.typname = 'int4'
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
      'alter table %I.%I alter column %I type bigint using %I::bigint',
      v_col.schema_name,
      v_col.table_name,
      v_col.column_name,
      v_col.column_name
    );
  end loop;
end
$$;

-- 3) Recreate constraints (PK/UNIQUE/CHECK first, FK last).
do $$
declare
  v_row record;
begin
  for v_row in
    select table_name, constraint_name, constraint_def
    from _int8_id_constraints
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
