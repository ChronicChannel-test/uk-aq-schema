-- Dual-write bootstrap for HISTORY DB (uk_aq_history).
-- Safe to run multiple times.

create schema if not exists uk_aq_history;
create schema if not exists uk_aq_public;
create schema if not exists uk_aq_raw;

create table if not exists uk_aq_history.observations (
  connector_id bigint not null,
  timeseries_id bigint not null,
  observed_at timestamptz not null,
  value double precision,
  status text,
  created_at timestamptz not null default now(),
  primary key (connector_id, timeseries_id, observed_at)
);

drop index if exists uk_aq_history.uk_aq_history_observations_series_observed_idx;

create index if not exists uk_aq_history_observations_observed_at_brin
  on uk_aq_history.observations using brin (observed_at);

create table if not exists uk_aq_raw.history_rpc_metrics_minute (
  bucket_minute timestamptz not null,
  endpoint text not null,
  calls bigint not null default 0,
  rows_input bigint not null default 0,
  payload_bytes bigint not null default 0,
  rows_upserted bigint not null default 0,
  duration_ms_sum bigint not null default 0,
  duration_ms_max int not null default 0,
  primary key (bucket_minute, endpoint)
);

create index if not exists history_rpc_metrics_minute_endpoint_idx
  on uk_aq_raw.history_rpc_metrics_minute (endpoint, bucket_minute desc);

create or replace view uk_aq_public.uk_aq_history_rpc_metrics_minute as
select
  bucket_minute,
  endpoint,
  calls,
  rows_input,
  payload_bytes,
  rows_upserted,
  duration_ms_sum,
  duration_ms_max
from uk_aq_raw.history_rpc_metrics_minute;
alter view if exists uk_aq_public.uk_aq_history_rpc_metrics_minute set (security_invoker = true);

create or replace view uk_aq_public.uk_aq_observation_rpc_metrics_minute as
select
  bucket_minute,
  endpoint,
  calls,
  rows_input,
  payload_bytes,
  rows_upserted,
  duration_ms_sum,
  duration_ms_max
from uk_aq_raw.history_rpc_metrics_minute;
alter view if exists uk_aq_public.uk_aq_observation_rpc_metrics_minute set (security_invoker = true);

create or replace function uk_aq_public.uk_aq_rpc_history_observations_upsert(rows jsonb)
returns table(observations_upserted int)
language plpgsql
security definer
set search_path = uk_aq_history, uk_aq_raw, public, pg_catalog
as $$
declare
  v_count int := 0;
  v_started_at timestamptz := clock_timestamp();
  v_input_rows int := 0;
  v_payload_bytes int := 0;
  v_duration_ms int := 0;
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

  insert into uk_aq_history.observations (
    connector_id,
    timeseries_id,
    observed_at,
    value,
    status
  )
  select
    input.connector_id,
    input.timeseries_id,
    input.observed_at,
    input.value,
    input.status
  from jsonb_to_recordset(rows) as input(
    connector_id bigint,
    timeseries_id bigint,
    observed_at timestamptz,
    value double precision,
    value_float8_hex text,
    status text
  )
  where input.connector_id is not null
    and input.timeseries_id is not null
    and input.observed_at is not null
  on conflict (connector_id, timeseries_id, observed_at)
  do update set
    value = excluded.value,
    status = excluded.status
  where
    uk_aq_history.observations.value is distinct from excluded.value
    or uk_aq_history.observations.status is distinct from excluded.status;

  get diagnostics v_count = row_count;

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

revoke all on function uk_aq_public.uk_aq_rpc_history_observations_upsert(jsonb) from public;
grant execute on function uk_aq_public.uk_aq_rpc_history_observations_upsert(jsonb) to service_role;

grant usage on schema uk_aq_history to service_role;
grant usage on schema uk_aq_public to service_role;
grant usage on schema uk_aq_raw to service_role;
grant all on table uk_aq_history.observations to service_role;
grant select on uk_aq_public.uk_aq_history_rpc_metrics_minute to service_role;
grant select on uk_aq_public.uk_aq_observation_rpc_metrics_minute to service_role;
