-- Ensure SOS station selector only returns stations that actually have timeseries.

create or replace function uk_aq_core.uk_air_sos_select_station_refs(
  batch_limit integer default 100,
  stale_limit integer default 4
)
returns text[]
language plpgsql
set search_path = uk_aq_core, uk_aq_raw, public, pg_catalog
as $$
declare
  v_connector_id integer;
  station_refs text[];
begin
  select id into v_connector_id
  from connectors
  where connector_code = 'uk_air_sos'
  limit 1;

  if v_connector_id is null then
    return null;
  end if;

  with latest_obs as (
    select
      t.station_id,
      max(t.last_value_at) as last_observed_at
    from timeseries t
    where t.connector_id = v_connector_id
    group by t.station_id
  ),
  station_with_timeseries as (
    select distinct t.station_id
    from timeseries t
    where t.connector_id = v_connector_id
  ),
  candidates as (
    select
      stn.id as station_id,
      stn.station_ref,
      sc.next_due_at,
      sc.last_polled_at,
      nullif(
        greatest(
          coalesce(sc.last_observed_at, '-infinity'::timestamptz),
          coalesce(lo.last_observed_at, '-infinity'::timestamptz)
        ),
        '-infinity'::timestamptz
      ) as last_observed_at,
      coalesce(sc.next_due_at, now()) as due_at
    from stations stn
    join station_with_timeseries swt
      on swt.station_id = stn.id
    left join uk_air_sos_station_checkpoints sc
      on sc.station_id = stn.id
    left join latest_obs lo
      on lo.station_id = stn.id
    where stn.connector_id = v_connector_id
      and stn.station_ref is not null
      and stn.removed_at is null
  ),
  tier1 as (
    select
      station_id,
      station_ref,
      due_at,
      last_polled_at
    from candidates
    where due_at <= now()
      and due_at >= now() - interval '6 hours'
      and (last_polled_at is null or last_polled_at <= now() - interval '5 minutes')
    order by last_polled_at asc nulls first, due_at asc
    limit batch_limit
  ),
  tier2 as (
    select
      c.station_id,
      c.station_ref,
      c.due_at,
      c.last_polled_at
    from candidates c
    where c.due_at < now() - interval '6 hours'
      and c.due_at >= now() - interval '24 hours'
      and (c.last_polled_at is null or c.last_polled_at <= now() - interval '1 hour')
      and not exists (
        select 1 from tier1 t where t.station_id = c.station_id
      )
    order by c.last_polled_at asc nulls first, c.due_at asc
    limit greatest(0, batch_limit - (select count(*) from tier1))
  ),
  tiered_limited as (
    select station_id, station_ref, due_at, last_polled_at
    from tier1
    union all
    select station_id, station_ref, due_at, last_polled_at
    from tier2
  ),
  stale as (
    select
      c.station_id,
      c.station_ref,
      c.last_observed_at
    from candidates c
    where (c.last_observed_at is null or c.last_observed_at <= now() - interval '24 hours')
      and (c.last_polled_at is null or c.last_polled_at <= now() - interval '12 hours')
      and not exists (
        select 1 from tiered_limited t where t.station_id = c.station_id
      )
    order by c.last_observed_at nulls first
    limit least(
      stale_limit,
      greatest(0, batch_limit - (select count(*) from tiered_limited))
    )
  ),
  combined as (
    select station_ref, 1 as group_order, due_at as sort_at
    from tiered_limited
    union all
    select station_ref, 2 as group_order, null as sort_at
    from stale
  )
  select array_agg(combined.station_ref order by group_order, sort_at nulls last)
  into station_refs
  from combined;

  return station_refs;
end;
$$;
