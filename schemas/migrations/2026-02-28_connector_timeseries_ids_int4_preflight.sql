-- Preflight safety checks for connector/timeseries int4 migration.
-- Blocks migration if any target value exceeds int4 max.

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
    order by schema_name, table_name, column_name
  loop
    execute format(
      'select max(%I)::numeric from %I.%I',
      v_row.column_name,
      v_row.schema_name,
      v_row.table_name
    )
    into v_max;

    raise notice 'preflight %.%.% max=%',
      v_row.schema_name,
      v_row.table_name,
      v_row.column_name,
      coalesce(v_max::text, 'NULL');

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
