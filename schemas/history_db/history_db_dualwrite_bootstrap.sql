-- Dual-write bootstrap for HISTORY DB (uk_aq_history).
-- Safe to run multiple times.

create schema if not exists uk_aq_history;
create schema if not exists uk_aq_public;

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

create or replace function uk_aq_public.uk_aq_rpc_history_observations_upsert(rows jsonb)
returns table(observations_upserted int)
language plpgsql
security definer
set search_path = uk_aq_history, public, pg_catalog
as $$
declare
  v_count int := 0;
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
  return query select coalesce(v_count, 0);
end;
$$;

revoke all on function uk_aq_public.uk_aq_rpc_history_observations_upsert(jsonb) from public;
grant execute on function uk_aq_public.uk_aq_rpc_history_observations_upsert(jsonb) to service_role;

grant usage on schema uk_aq_history to service_role;
grant usage on schema uk_aq_public to service_role;
grant all on table uk_aq_history.observations to service_role;
