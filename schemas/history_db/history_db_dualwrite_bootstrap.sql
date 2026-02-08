-- Dual-write bootstrap for HISTORY DB (uk_aq_history).
-- Safe to run multiple times.

create schema if not exists uk_aq_history;
create schema if not exists uk_aq_public;

create table if not exists uk_aq_history.observations (
  connector_code text not null,
  service_ref text not null,
  timeseries_ref text not null,
  observed_at timestamptz not null,
  value double precision,
  status text,
  moved_at timestamptz default now(),
  primary key (connector_code, service_ref, timeseries_ref, observed_at)
);

create index if not exists uk_aq_history_observations_series_observed_idx
  on uk_aq_history.observations (connector_code, service_ref, timeseries_ref, observed_at);

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
    connector_code,
    service_ref,
    timeseries_ref,
    observed_at,
    value,
    status
  )
  select
    input.connector_code,
    input.service_ref,
    input.timeseries_ref,
    input.observed_at,
    input.value,
    input.status
  from jsonb_to_recordset(rows) as input(
    connector_code text,
    service_ref text,
    timeseries_ref text,
    observed_at timestamptz,
    value double precision,
    status text
  )
  where input.connector_code is not null
    and input.service_ref is not null
    and input.timeseries_ref is not null
    and input.observed_at is not null
  on conflict (connector_code, service_ref, timeseries_ref, observed_at)
  do update set
    value = excluded.value,
    status = excluded.status,
    moved_at = now();

  get diagnostics v_count = row_count;
  return query select coalesce(v_count, 0);
end;
$$;

revoke all on function uk_aq_public.uk_aq_rpc_history_observations_upsert(jsonb) from public;
grant execute on function uk_aq_public.uk_aq_rpc_history_observations_upsert(jsonb) to service_role;

grant usage on schema uk_aq_history to service_role;
grant usage on schema uk_aq_public to service_role;
grant all on table uk_aq_history.observations to service_role;
