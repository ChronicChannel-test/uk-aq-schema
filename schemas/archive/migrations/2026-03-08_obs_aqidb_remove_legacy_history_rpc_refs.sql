begin;

-- Remove legacy history-named RPCs that still reference dropped schema uk_aq_history.
drop function if exists uk_aq_public.uk_aq_rpc_history_drop_candidates(timestamptz);
drop function if exists uk_aq_public.uk_aq_rpc_history_drop_partition(text);
drop function if exists uk_aq_public.uk_aq_rpc_history_enforce_hot_cold_indexes(date, date);
drop function if exists uk_aq_public.uk_aq_rpc_history_ensure_daily_partitions(date, date);
drop function if exists uk_aq_public.uk_aq_rpc_history_observations_default_diagnostics(integer);
drop function if exists uk_aq_public.uk_aq_rpc_history_observations_upsert(jsonb);

-- Replace legacy bridge RPC so it points at uk_aq_observs after hard cut.
drop function if exists uk_aq_public.rpc_observations_window(
  timestamptz,
  timestamptz,
  integer,
  integer
);

drop function if exists uk_aq_public.rpc_observations_window(
  timestamptz,
  timestamptz,
  integer,
  bigint
);

create or replace function uk_aq_public.rpc_observations_window(
  start_utc timestamptz,
  end_utc timestamptz,
  timeseries_id integer default null,
  station_id integer default null
)
returns setof uk_aq_observs.observations
language plpgsql
security invoker
set search_path = uk_aq_observs, uk_aq_core, public, pg_catalog
as $$
begin
  if start_utc is null or end_utc is null then
    raise exception 'start_utc and end_utc are required';
  end if;

  if end_utc <= start_utc then
    raise exception 'end_utc must be greater than start_utc';
  end if;

  if end_utc - start_utc > interval '33 days' then
    raise exception 'window must be 33 days or less';
  end if;

  if timeseries_id is not null and station_id is not null then
    return query
    select o.*
    from uk_aq_observs.observations o
    join uk_aq_core.timeseries ts
      on ts.id = o.timeseries_id
    where o.observed_at >= start_utc
      and o.observed_at < end_utc
      and o.timeseries_id = rpc_observations_window.timeseries_id
      and ts.station_id = rpc_observations_window.station_id::bigint
    order by o.observed_at asc;
    return;
  end if;

  if timeseries_id is not null then
    return query
    select o.*
    from uk_aq_observs.observations o
    where o.observed_at >= start_utc
      and o.observed_at < end_utc
      and o.timeseries_id = rpc_observations_window.timeseries_id
    order by o.observed_at asc;
    return;
  end if;

  if station_id is not null then
    return query
    select o.*
    from uk_aq_observs.observations o
    join uk_aq_core.timeseries ts
      on ts.id = o.timeseries_id
    where o.observed_at >= start_utc
      and o.observed_at < end_utc
      and ts.station_id = rpc_observations_window.station_id::bigint
    order by o.observed_at asc;
    return;
  end if;

  return query
  select o.*
  from uk_aq_observs.observations o
  where o.observed_at >= start_utc
    and o.observed_at < end_utc
  order by o.observed_at asc;
end;
$$;

grant execute on function uk_aq_public.rpc_observations_window(
  timestamptz,
  timestamptz,
  integer,
  integer
) to anon, authenticated;

grant execute on function uk_aq_public.rpc_observations_window(
  timestamptz,
  timestamptz,
  integer,
  integer
) to service_role;

commit;
