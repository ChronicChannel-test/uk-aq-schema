-- Optimize history RPC upsert path by routing hot-day writes directly to
-- partition tables with ON CONFLICT, and retaining fallback behavior for
-- partitions without hot key indexes.

create or replace function uk_aq_public.uk_aq_rpc_history_observations_upsert(rows jsonb)
returns table(observations_upserted int)
language plpgsql
security definer
set search_path = uk_aq_history, uk_aq_raw, public, pg_catalog
as $$
declare
  v_count int := 0;
  v_day_count int := 0;
  v_started_at timestamptz := clock_timestamp();
  v_input_rows int := 0;
  v_payload_bytes int := 0;
  v_duration_ms int := 0;
  v_day date;
  v_day_start timestamptz;
  v_day_end timestamptz;
  v_partition_name text;
  v_partition_reg regclass;
  v_hot_uidx_name text;
  v_has_hot_uidx boolean;
begin
  if auth.role() <> 'service_role' then
    raise exception 'service_role required';
  end if;

  if rows is null
    or jsonb_typeof(rows) <> 'array'
    or jsonb_array_length(rows) = 0
  then
    return query select 0;
    return;
  end if;

  v_input_rows := jsonb_array_length(rows);
  v_payload_bytes := pg_column_size(rows);

  create temporary table history_upsert_input_rows (
    connector_id integer not null,
    timeseries_id integer not null,
    observed_at timestamptz not null,
    observed_day date not null,
    value double precision
  ) on commit drop;

  insert into history_upsert_input_rows (
    connector_id,
    timeseries_id,
    observed_at,
    observed_day,
    value
  )
  with input_rows as (
    select distinct on (
      input.connector_id,
      input.timeseries_id,
      input.observed_at
    )
      input.connector_id,
      input.timeseries_id,
      input.observed_at,
      (input.observed_at at time zone 'UTC')::date as observed_day,
      input.value
    from jsonb_to_recordset(rows) as input(
      connector_id integer,
      timeseries_id integer,
      observed_at timestamptz,
      value double precision,
      value_float8_hex text
    )
    where input.connector_id is not null
      and input.timeseries_id is not null
      and input.observed_at is not null
    order by
      input.connector_id,
      input.timeseries_id,
      input.observed_at
  )
  select
    connector_id,
    timeseries_id,
    observed_at,
    observed_day,
    value
  from input_rows;

  for v_day in
    select distinct observed_day
    from history_upsert_input_rows
    order by observed_day
  loop
    v_day_start := format('%s 00:00:00+00', v_day)::timestamptz;
    v_day_end := format('%s 00:00:00+00', v_day + 1)::timestamptz;

    v_partition_name := format('observations_%s', to_char(v_day, 'YYYYMMDD'));
    v_partition_reg := to_regclass(format('uk_aq_history.%I', v_partition_name));
    v_hot_uidx_name := v_partition_name || '_hot_key_uidx';

    select exists (
      select 1
      from pg_class idx
      join pg_namespace ns on ns.oid = idx.relnamespace
      where ns.nspname = 'uk_aq_history'
        and idx.relkind = 'i'
        and idx.relname = v_hot_uidx_name
    )
    into v_has_hot_uidx;

    if v_partition_reg is not null and v_has_hot_uidx then
      execute format(
        $sql$
        with upserted as (
          insert into uk_aq_history.%I (
            connector_id,
            timeseries_id,
            observed_at,
            value
          )
          select
            connector_id,
            timeseries_id,
            observed_at,
            value
          from pg_temp.history_upsert_input_rows
          where observed_day = $1
          on conflict (connector_id, timeseries_id, observed_at)
          do update set
            value = excluded.value
          where uk_aq_history.%I.value is distinct from excluded.value
          returning 1
        )
        select coalesce(count(*), 0)::int from upserted
        $sql$,
        v_partition_name,
        v_partition_name
      )
      into v_day_count
      using v_day;
    else
      with day_rows as (
        select
          r.connector_id,
          r.timeseries_id,
          r.observed_at,
          r.value
        from pg_temp.history_upsert_input_rows r
        where r.observed_day = v_day
      ),
      updated as (
        update uk_aq_history.observations o
        set value = d.value
        from day_rows d
        where o.observed_at >= v_day_start
          and o.observed_at < v_day_end
          and o.connector_id = d.connector_id
          and o.timeseries_id = d.timeseries_id
          and o.observed_at = d.observed_at
          and o.value is distinct from d.value
        returning 1
      ),
      inserted as (
        insert into uk_aq_history.observations (
          connector_id,
          timeseries_id,
          observed_at,
          value
        )
        select
          d.connector_id,
          d.timeseries_id,
          d.observed_at,
          d.value
        from day_rows d
        left join uk_aq_history.observations o
          on o.observed_at >= v_day_start
         and o.observed_at < v_day_end
         and o.connector_id = d.connector_id
         and o.timeseries_id = d.timeseries_id
         and o.observed_at = d.observed_at
        where o.connector_id is null
        returning 1
      )
      select
        coalesce((select count(*) from updated), 0)
        + coalesce((select count(*) from inserted), 0)
      into v_day_count;
    end if;

    v_count := v_count + coalesce(v_day_count, 0);
  end loop;

  v_duration_ms := greatest(
    0,
    floor(extract(epoch from (clock_timestamp() - v_started_at)) * 1000)::int
  );

  insert into uk_aq_raw.history_rpc_metrics_minute (
    bucket_minute,
    endpoint,
    calls,
    rows_input,
    payload_bytes,
    rows_upserted,
    duration_ms_sum,
    duration_ms_max
  )
  values (
    date_trunc('minute', now()),
    'rpc/uk_aq_rpc_history_observations_upsert',
    1,
    v_input_rows,
    v_payload_bytes,
    coalesce(v_count, 0),
    v_duration_ms,
    v_duration_ms
  )
  on conflict (bucket_minute, endpoint)
  do update set
    calls = uk_aq_raw.history_rpc_metrics_minute.calls + 1,
    rows_input = uk_aq_raw.history_rpc_metrics_minute.rows_input + excluded.rows_input,
    payload_bytes = uk_aq_raw.history_rpc_metrics_minute.payload_bytes + excluded.payload_bytes,
    rows_upserted = uk_aq_raw.history_rpc_metrics_minute.rows_upserted + excluded.rows_upserted,
    duration_ms_sum = uk_aq_raw.history_rpc_metrics_minute.duration_ms_sum + excluded.duration_ms_sum,
    duration_ms_max = greatest(uk_aq_raw.history_rpc_metrics_minute.duration_ms_max, excluded.duration_ms_max);

  return query select coalesce(v_count, 0);
end;
$$;
