-- Dual-write bootstrap for HISTORY DB (uk_aq_history).
-- Safe to run multiple times.

create schema if not exists uk_aq_history;
create schema if not exists uk_aq_public;
create schema if not exists uk_aq_raw;

create table if not exists uk_aq_history.status_codes (
  status_id smallint primary key,
  code text not null unique,
  description text,
  severity smallint,
  is_public boolean not null default true,
  created_at timestamptz not null default now()
);

comment on table uk_aq_history.status_codes is
  'Canonical status dictionary for history observations, intended for QA scripts and future validation, not ingest writes.';
comment on column uk_aq_history.status_codes.status_id is
  'Canonical smallint status identifier for QA scripts and future validation, not ingest writes.';
comment on column uk_aq_history.status_codes.code is
  'Stable canonical status code used for QA scripts and future validation, not ingest writes.';
comment on column uk_aq_history.status_codes.description is
  'Optional human-readable status description for QA scripts and future validation, not ingest writes.';
comment on column uk_aq_history.status_codes.severity is
  'Optional canonical severity ranking for QA scripts and future validation, not ingest writes.';
comment on column uk_aq_history.status_codes.is_public is
  'Flag for whether the canonical status is suitable for public-facing use in QA/validation outputs; not ingest writes.';
comment on column uk_aq_history.status_codes.created_at is
  'Creation timestamp for canonical status dictionary rows maintained for QA scripts and future validation, not ingest writes.';

create table if not exists uk_aq_history.observations (
  connector_id integer not null,
  timeseries_id integer not null,
  observed_at timestamptz not null,
  value double precision,
  status_id smallint,
  created_at timestamptz not null default now(),
  constraint uk_aq_history_observations_status_id_fkey
    foreign key (status_id)
    references uk_aq_history.status_codes(status_id)
    on delete set null,
  primary key (connector_id, timeseries_id, observed_at)
);

alter table if exists uk_aq_history.observations
  add column if not exists status_id smallint;

alter table if exists uk_aq_history.observations
  drop column if exists status;

do $$
begin
  if not exists (
    select 1
    from pg_constraint c
    join pg_namespace n on n.oid = c.connamespace
    where n.nspname = 'uk_aq_history'
      and c.conname = 'uk_aq_history_observations_status_id_fkey'
  ) then
    execute
      'alter table uk_aq_history.observations '
      'add constraint uk_aq_history_observations_status_id_fkey '
      'foreign key (status_id) '
      'references uk_aq_history.status_codes(status_id) '
      'on delete set null';
  end if;
end $$;

drop index if exists uk_aq_history.uk_aq_history_observations_series_observed_idx;

create index if not exists uk_aq_history_observations_observed_at_brin
  on uk_aq_history.observations using brin (observed_at);

alter table if exists uk_aq_history.status_codes enable row level security;

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
    status_id
  )
  select
    input.connector_id,
    input.timeseries_id,
    input.observed_at,
    input.value,
    input.status_id
  from jsonb_to_recordset(rows) as input(
    connector_id integer,
    timeseries_id integer,
    observed_at timestamptz,
    value double precision,
    value_float8_hex text,
    status_id smallint
  )
  where input.connector_id is not null
    and input.timeseries_id is not null
    and input.observed_at is not null
  on conflict (connector_id, timeseries_id, observed_at)
  do update set
    value = excluded.value,
    status_id = excluded.status_id
  where
    uk_aq_history.observations.value is distinct from excluded.value
    or uk_aq_history.observations.status_id is distinct from excluded.status_id;

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

create or replace function uk_aq_public.uk_aq_rpc_database_size_bytes()
returns table (
  database_name text,
  size_bytes bigint,
  sampled_at timestamptz
)
language plpgsql
security definer
set search_path = pg_catalog, public
as $$
begin
  if auth.role() <> 'service_role' then
    raise exception 'service_role required';
  end if;

  return query
  select
    current_database()::text as database_name,
    pg_database_size(current_database())::bigint as size_bytes,
    now() as sampled_at;
end;
$$;

revoke all on function uk_aq_public.uk_aq_rpc_history_observations_upsert(jsonb) from public;
grant execute on function uk_aq_public.uk_aq_rpc_history_observations_upsert(jsonb) to service_role;

revoke all on function uk_aq_public.uk_aq_rpc_database_size_bytes() from public;
grant execute on function uk_aq_public.uk_aq_rpc_database_size_bytes() to service_role;

revoke all on table uk_aq_history.status_codes from public;
revoke all on table uk_aq_history.status_codes from service_role;
do $$
declare
  v_role text;
begin
  for v_role in
    select rolname
    from pg_roles
    where rolname ilike '%ingest%'
  loop
    execute format(
      'revoke all on table uk_aq_history.status_codes from %I',
      v_role
    );
  end loop;
end $$;

grant usage on schema uk_aq_history to service_role;
grant usage on schema uk_aq_public to service_role;
grant usage on schema uk_aq_raw to service_role;
grant all on table uk_aq_history.observations to service_role;
grant select on uk_aq_public.uk_aq_history_rpc_metrics_minute to service_role;
grant select on uk_aq_public.uk_aq_observation_rpc_metrics_minute to service_role;
