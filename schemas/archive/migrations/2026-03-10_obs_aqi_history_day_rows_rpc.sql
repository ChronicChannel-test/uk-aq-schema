-- Add paged day+connector observation source RPC for obs_aqi_to_r2 export.
-- This keeps backfill source reads on uk_aq_public/service_role when desired.

drop function if exists uk_aq_public.uk_aq_rpc_observs_history_day_rows(
  date,
  integer,
  integer,
  timestamptz,
  integer
);

create or replace function uk_aq_public.uk_aq_rpc_observs_history_day_rows(
  p_day_utc date,
  p_connector_id integer,
  p_after_timeseries_id integer default null,
  p_after_observed_at timestamptz default null,
  p_limit integer default 20000
)
returns table (
  timeseries_id integer,
  observed_at timestamptz,
  value double precision
)
language plpgsql
security definer
set search_path = uk_aq_observs, public, pg_catalog
as $$
declare
  v_start timestamptz;
  v_end timestamptz;
  v_limit integer;
begin
  if auth.role() <> 'service_role' then
    raise exception 'service_role required';
  end if;

  if p_day_utc is null then
    raise exception 'p_day_utc is required';
  end if;

  if p_connector_id is null or p_connector_id <= 0 then
    raise exception 'p_connector_id must be > 0';
  end if;

  if (p_after_timeseries_id is null) <> (p_after_observed_at is null) then
    raise exception 'p_after_timeseries_id and p_after_observed_at must both be null or both provided';
  end if;

  v_limit := greatest(1, least(coalesce(p_limit, 20000), 100000));
  v_start := (p_day_utc::text || ' 00:00:00+00')::timestamptz;
  v_end := ((p_day_utc + 1)::text || ' 00:00:00+00')::timestamptz;

  return query
  select
    o.timeseries_id::integer,
    o.observed_at,
    o.value
  from uk_aq_observs.observations o
  where o.connector_id = p_connector_id
    and o.observed_at >= v_start
    and o.observed_at < v_end
    and (
      p_after_timeseries_id is null
      or o.timeseries_id > p_after_timeseries_id
      or (o.timeseries_id = p_after_timeseries_id and o.observed_at > p_after_observed_at)
    )
  order by o.timeseries_id asc, o.observed_at asc
  limit v_limit;
end;
$$;

revoke all on function uk_aq_public.uk_aq_rpc_observs_history_day_rows(
  date,
  integer,
  integer,
  timestamptz,
  integer
) from public;
grant execute on function uk_aq_public.uk_aq_rpc_observs_history_day_rows(
  date,
  integer,
  integer,
  timestamptz,
  integer
) to service_role;
