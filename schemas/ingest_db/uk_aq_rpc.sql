-- uk_aq_la_hex RPC for read-only access (Edge function backing).
create schema if not exists uk_aq_public;

drop function if exists uk_aq_public.uk_aq_la_hex_rpc(
  text[],
  text,
  int
);

drop function if exists uk_aq_public.uk_aq_la_hex_rpc(
  text[],
  text,
  int,
  timestamptz
);

create or replace function uk_aq_public.uk_aq_la_hex_rpc(
  region text[] default null,
  la_version text default null,
  limit_rows int default 1000,
  since_ts timestamptz default null
)
returns table (
  la_code text,
  la_name text,
  la_version text,
  station_count int,
  single_site boolean,
  median_value double precision,
  mean_value double precision,
  latest_value_at timestamptz
)
language sql
security definer
set search_path = uk_aq_core, public, pg_catalog
as $$
  with params as (
    select
      region as region_codes,
      nullif(trim(la_version), '') as la_version,
      least(10000, greatest(1, coalesce(limit_rows, 1000)))::int as limit_rows,
      since_ts as since_ts
  )
  select
    l.la_code,
    l.la_name,
    l.la_version,
    l.station_count,
    l.single_site,
    l.median_value,
    l.mean_value,
    l.latest_value_at
  from uk_aq_core.la_latest_pm25 l
  cross join params
  where (params.la_version is null or l.la_version = params.la_version)
    and (params.region_codes is null or l.la_code = any(params.region_codes))
    and (params.since_ts is null or l.latest_value_at > params.since_ts)
  limit (select limit_rows from params);
$$;

grant execute on function uk_aq_public.uk_aq_la_hex_rpc(
  text[],
  text,
  int,
  timestamptz
) to anon, authenticated;

grant execute on function uk_aq_public.uk_aq_la_hex_rpc(
  text[],
  text,
  int,
  timestamptz
) to service_role;
-- uk_aq_latest RPC for read-only access (Edge function backing).

do $$
declare
  v_fn record;
begin
  for v_fn in
    select
      p.proname,
      pg_get_function_identity_arguments(p.oid) as identity_args
    from pg_proc p
    join pg_namespace n on n.oid = p.pronamespace
    where n.nspname = 'uk_aq_public'
      and p.proname = any(array[
        'uk_aq_latest_rpc',
        'uk_aq_timeseries_rpc',
        'uk_aq_stations_rpc',
        'uk_aq_surbiton_latest_rpc'
      ])
  loop
    execute format(
      'drop function if exists uk_aq_public.%I(%s)',
      v_fn.proname,
      v_fn.identity_args
    );
  end loop;
end
$$;

drop function if exists uk_aq_public.uk_aq_latest_rpc(
  text,
  text,
  text,
  integer,
  text,
  int,
  text
);

drop function if exists uk_aq_public.uk_aq_latest_rpc(
  text,
  text,
  text,
  integer,
  text,
  int,
  text,
  timestamptz,
  timestamptz,
  integer
);

drop function if exists uk_aq_public.uk_aq_latest_rpc(
  text,
  text,
  text,
  integer,
  text,
  int,
  text,
  timestamptz
);

create or replace function uk_aq_public.uk_aq_latest_rpc(
  region text default null,
  pcon_code text default null,
  station_like text default null,
  connector_id integer default null,
  pollutant text default null,
  limit_rows int default 1000,
  window_label text default null,
  since_ts timestamptz default null,
  since_updated_at timestamptz default null,
  since_updated_id integer default null
)
returns table (
  id integer,
  updated_at timestamptz,
  timeseries_ref text,
  label text,
  uom text,
  last_value double precision,
  last_value_at timestamptz,
  connector_id integer,
  connector jsonb,
  station jsonb,
  phenomenon jsonb
)
language sql
security definer
set search_path = uk_aq_core, public, pg_catalog
as $$
  with params as (
    select
      nullif(trim(region), '') as region,
      nullif(trim(pcon_code), '') as pcon_code,
      nullif(trim(station_like), '') as station_like,
      connector_id as connector_id,
      nullif(trim(pollutant), '') as pollutant,
      least(10000, greatest(1, coalesce(limit_rows, 1000)))::int as limit_rows,
      since_ts as since_ts,
      coalesce(since_updated_at, since_ts) as since_updated_at,
      case
        when since_updated_at is null then null
        else greatest(0, coalesce(since_updated_id, 0))
      end as since_updated_id,
      case
        when lower(nullif(trim(window_label), '')) in ('3h','6h','1d','7d','all')
          then lower(nullif(trim(window_label), ''))
        else 'all'
      end as window_label
  ),
  pollutant_tokens as (
    select
      case
        when pollutant is null then array[]::text[]
        when replace(replace(lower(pollutant), ' ', ''), '_', '') in ('pm25', 'pm2.5', 'pm2-5') then
          array[
            lower(pollutant),
            'pm2.5',
            'pm25',
            'pm2-5',
            'pm2_5'
          ]
        else
          array[lower(pollutant)]
      end as tokens,
      pollutant
    from params
  ),
  base as (
    select
      ts.id,
      ts.updated_at,
      ts.timeseries_ref,
      ts.label,
      ts.uom,
      ts.last_value,
      ts.last_value_at,
      ts.connector_id,
      case
        when c.id is null then null
        else jsonb_build_object(
          'id', c.id,
          'connector_code', c.connector_code,
          'label', c.label,
          'display_name', c.display_name,
          'station_display_name_template', c.station_display_name_template
        )
      end as connector,
      case
        when s.id is null then null
        else jsonb_build_object(
          'id', s.id,
          'station_ref', s.station_ref,
          'label', s.label,
          'station_name', s.station_name,
          'region', s.region,
          'la_code', s.la_code,
          'la_version', s.la_version,
          'pcon_code', s.pcon_code,
          'pcon_version', s.pcon_version,
          'connector_id', s.connector_id,
          'station_network_memberships', coalesce(memberships.memberships, '[]'::jsonb)
        )
      end as station,
      case
        when p.id is null then null
        else jsonb_build_object(
          'id', p.id,
          'label', p.label,
          'source_label', p.source_label,
          'notation', p.notation,
          'eionet_uri', p.source_label,
          'pollutant_label', p.pollutant_label,
          'observed_property_id', op.id,
          'observed_property_code', op.code,
          'observed_property_display_name', op.display_name,
          'observed_property_domain', op.domain,
          'canonical_uom', op.canonical_uom
        )
      end as phenomenon,
      s.label as station_label,
      s.station_name as station_name
    from uk_aq_core.timeseries ts
    left join uk_aq_core.connectors c on c.id = ts.connector_id
    left join uk_aq_core.stations s on s.id = ts.station_id
    left join uk_aq_core.phenomena p on p.id = ts.phenomenon_id
    left join uk_aq_core.observed_properties op on op.id = p.observed_property_id
    left join lateral (
      select coalesce(
        jsonb_agg(
          jsonb_build_object(
            'network_code', snm.network_code,
            'network_label', snm.network_label,
            'is_primary', snm.is_primary
          )
          order by snm.network_code
        ),
        '[]'::jsonb
      ) as memberships
      from uk_aq_core.station_network_memberships snm
      where snm.station_id = s.id
    ) memberships on true
    cross join params
    cross join pollutant_tokens pt
    where ts.last_value >= 0
      and ts.last_value_at is not null
      and (params.connector_id is null or ts.connector_id = params.connector_id)
      and (params.region is null or s.region ilike '%' || params.region || '%')
      and (params.pcon_code is null or s.pcon_code = params.pcon_code)
      and (
        params.since_updated_at is null
        or ts.updated_at > params.since_updated_at
        or (
          params.since_updated_id is not null
          and ts.updated_at = params.since_updated_at
          and ts.id > params.since_updated_id
        )
      )
      and (
        params.window_label = 'all'
        or (params.window_label = '3h' and ts.last_value_at >= now() - interval '3 hours')
        or (params.window_label = '6h' and ts.last_value_at >= now() - interval '6 hours')
        or (params.window_label = '1d' and ts.last_value_at >= now() - interval '1 day')
        or (params.window_label = '7d' and ts.last_value_at >= now() - interval '7 days')
      )
      and (
        params.pollutant is null
        or exists (
          select 1
          from unnest(pt.tokens) token
          where op.code = uk_aq_core.uk_aq_observed_property_code(null, token, token, token)
             or lower(coalesce(op.display_name, '')) = token
             or lower(coalesce(p.notation, '')) = token
             or lower(coalesce(p.pollutant_label, '')) = token
             or lower(coalesce(p.label, '')) = token
             or lower(coalesce(p.source_label, '')) = token
        )
      )
  ),
  series_matches as (
    select base.*
    from base
    where (select station_like from params) is null
       or base.label ilike '%' || (select station_like from params) || '%'
       or coalesce(base.station_name, '') ilike '%' || (select station_like from params) || '%'
    order by base.updated_at asc, base.id
    limit (select limit_rows from params)
  ),
  station_matches as (
    select base.*
    from base
    where (select station_like from params) is not null
      and (
        base.station_label ilike '%' || (select station_like from params) || '%'
        or coalesce(base.station_name, '') ilike '%' || (select station_like from params) || '%'
      )
    order by base.updated_at asc, base.id
    limit (select limit_rows from params)
  ),
  combined as (
    select 1 as src_rank, series_matches.*
    from series_matches
    union all
    select 2 as src_rank, station_matches.*
    from station_matches
  ),
  deduped as (
    select distinct on (id) *
    from combined
    order by id, src_rank
  )
  select
    id,
    updated_at,
    timeseries_ref,
    label,
    uom,
    last_value,
    last_value_at,
    connector_id,
    connector,
    station,
    phenomenon
  from deduped
  order by src_rank, updated_at, id
  limit (select limit_rows from params);
$$;

grant execute on function uk_aq_public.uk_aq_latest_rpc(
  text,
  text,
  text,
  integer,
  text,
  int,
  text,
  timestamptz,
  timestamptz,
  integer
) to anon, authenticated;

grant execute on function uk_aq_public.uk_aq_latest_rpc(
  text,
  text,
  text,
  integer,
  text,
  int,
  text,
  timestamptz,
  timestamptz,
  integer
) to service_role;

-- uk_aq_timeseries RPC for read-only access (Edge function backing).

drop function if exists uk_aq_public.uk_aq_timeseries_rpc(
  integer,
  text,
  int
);

drop function if exists uk_aq_public.uk_aq_timeseries_rpc(
  integer,
  text,
  int,
  timestamptz
);

drop function if exists uk_aq_public.uk_aq_timeseries_rpc(
  integer,
  text,
  int,
  timestamptz,
  boolean
);

create or replace function uk_aq_public.uk_aq_timeseries_rpc(
  timeseries_id integer,
  window_label text default '24h',
  limit_rows int default null,
  since_ts timestamptz default null,
  include_status boolean default true
)
returns table (
  timeseries_id integer,
  "window" text,
  start timestamptz,
  "end" timestamptz,
  count int,
  guideline jsonb,
  data jsonb
)
language sql
security definer
set search_path = uk_aq_core, public, pg_catalog
as $$
  with params as (
    select
      $1::integer as timeseries_id,
      case
        when lower(nullif(trim(window_label), '')) in ('12h','24h','7d','30d')
          then lower(nullif(trim(window_label), ''))
        else '24h'
      end as window_label,
      case
        when limit_rows is null then null
        else greatest(1, limit_rows)::int
      end as limit_rows,
      since_ts as since_ts,
      coalesce(include_status, true) as include_status
  ),
  windowed as (
    select
      timeseries_id,
      window_label,
      now() as end_ts,
      case window_label
        when '12h' then now() - interval '12 hours'
        when '24h' then now() - interval '24 hours'
        when '7d' then now() - interval '7 days'
        when '30d' then now() - interval '30 days'
        else now() - interval '24 hours'
      end as start_ts,
      limit_rows,
      since_ts,
      include_status
    from params
  ),
  phen as (
    select
      op.code as observed_property_code,
      op.display_name as observed_property_display_name,
      p.pollutant_label,
      p.notation,
      p.label
    from uk_aq_core.timeseries ts
    left join uk_aq_core.phenomena p on p.id = ts.phenomenon_id
    left join uk_aq_core.observed_properties op on op.id = p.observed_property_id
    join windowed w on w.timeseries_id = ts.id
    limit 1
  ),
  pollutant as (
    select
      case
        when observed_property_code = 'pm25'
          then 'PM2.5'
        when observed_property_code = 'pm10'
          then 'PM10'
        when observed_property_code = 'no2'
          then 'NO2'
        when observed_property_code = 'o3'
          then 'O3'
        when observed_property_code = 'so2'
          then 'SO2'
        when lower(coalesce(observed_property_display_name, pollutant_label, notation, label, '')) like '%pm2.5%'
          or lower(coalesce(observed_property_display_name, pollutant_label, notation, label, '')) like '%pm2_5%'
          or lower(coalesce(observed_property_display_name, pollutant_label, notation, label, '')) like '%pm25%'
          then 'PM2.5'
        when lower(coalesce(observed_property_display_name, pollutant_label, notation, label, '')) like '%pm10%'
          then 'PM10'
        when lower(coalesce(observed_property_display_name, pollutant_label, notation, label, '')) like '%no2%'
          or lower(coalesce(observed_property_display_name, pollutant_label, notation, label, '')) like '%nitrogen dioxide%'
          then 'NO2'
        when lower(coalesce(observed_property_display_name, pollutant_label, notation, label, '')) like '%o3%'
          or lower(coalesce(observed_property_display_name, pollutant_label, notation, label, '')) like '%ozone%'
          then 'O3'
        when lower(coalesce(observed_property_display_name, pollutant_label, notation, label, '')) like '%so2%'
          or lower(coalesce(observed_property_display_name, pollutant_label, notation, label, '')) like '%sulphur dioxide%'
          or lower(coalesce(observed_property_display_name, pollutant_label, notation, label, '')) like '%sulfur dioxide%'
          then 'SO2'
        else nullif(
          upper(regexp_replace(
            coalesce(observed_property_display_name, pollutant_label, notation, label, ''),
            '\s+',
            '',
            'g'
          )),
          ''
        )
      end as pollutant_key
    from phen
  ),
  guideline as (
    select jsonb_build_object(
      'pollutant', g.pollutant,
      'averaging_period_label', g.averaging_period_label,
      'level_label', g.level_label,
      'limit_value', g.limit_value,
      'uom', g.uom,
      'source', g.source,
      'notes', g.notes
    ) as guideline
    from uk_aq_core.uk_aq_guidelines g
    cross join pollutant p
    where p.pollutant_key is not null
      and g.pollutant = p.pollutant_key
      and g.averaging_period_label = '24-hour'
      and g.level_label = 'AQG_2021'
    limit 1
  ),
  obs as (
    select o.observed_at, o.value, o.status
    from uk_aq_core.observations o
    join windowed w on w.timeseries_id = o.timeseries_id
    where o.observed_at >= w.start_ts
      and (w.since_ts is null or o.observed_at > w.since_ts)
    order by o.observed_at asc
    limit coalesce((select limit_rows from windowed), 2147483647)
  )
  select
    w.timeseries_id,
    w.window_label as window,
    w.start_ts as start,
    w.end_ts as "end",
    (select count(*) from obs)::int as count,
    (select guideline from guideline) as guideline,
    coalesce(
      (select jsonb_agg(
        case
          when (select include_status from windowed limit 1)
            then jsonb_build_object(
              'observed_at', observed_at,
              'value', value,
              'status', status
            )
          else jsonb_build_object(
            'observed_at', observed_at,
            'value', value
          )
        end
        order by observed_at
      ) from obs),
      '[]'::jsonb
    ) as data
  from windowed w;
$$;

grant execute on function uk_aq_public.uk_aq_timeseries_rpc(
  integer,
  text,
  int,
  timestamptz,
  boolean
) to anon, authenticated;

grant execute on function uk_aq_public.uk_aq_timeseries_rpc(
  integer,
  text,
  int,
  timestamptz,
  boolean
) to service_role;

-- uk_aq_pcon_hex RPC for read-only access (Edge function backing).

drop function if exists uk_aq_public.uk_aq_pcon_hex_rpc(
  text,
  int
);

drop function if exists uk_aq_public.uk_aq_pcon_hex_rpc(
  text,
  int,
  timestamptz
);

create or replace function uk_aq_public.uk_aq_pcon_hex_rpc(
  pcon_version text default null,
  limit_rows int default 1000,
  since_ts timestamptz default null
)
returns table (
  pcon_code text,
  pcon_name text,
  pcon_version text,
  station_count int,
  single_site boolean,
  median_value double precision,
  mean_value double precision,
  latest_value_at timestamptz
)
language sql
security definer
set search_path = uk_aq_core, public, pg_catalog
as $$
  with params as (
    select
      nullif(trim(pcon_version), '') as pcon_version,
      least(10000, greatest(1, coalesce(limit_rows, 1000)))::int as limit_rows,
      since_ts as since_ts
  )
  select
    p.pcon_code,
    p.pcon_name,
    p.pcon_version,
    p.station_count,
    p.single_site,
    p.median_value,
    p.mean_value,
    p.latest_value_at
  from uk_aq_core.pcon_latest_pm25 p
  cross join params
  where params.pcon_version is null or p.pcon_version = params.pcon_version
    and (params.since_ts is null or p.latest_value_at > params.since_ts)
  limit (select limit_rows from params);
$$;

grant execute on function uk_aq_public.uk_aq_pcon_hex_rpc(
  text,
  int,
  timestamptz
) to anon, authenticated;

grant execute on function uk_aq_public.uk_aq_pcon_hex_rpc(
  text,
  int,
  timestamptz
) to service_role;

-- uk_aq_stations RPC for read-only access (Edge function backing).

create or replace function uk_aq_public.uk_aq_stations_rpc(
  connector_id integer default null,
  region text default null,
  station_like text default null,
  limit_rows int default null,
  page_size int default null
)
returns table (
  id bigint,
  station_ref text,
  label text,
  geometry jsonb,
  network_memberships jsonb
)
language sql
security definer
set search_path = uk_aq_core, public, pg_catalog
as $$
  with params as (
    select
      connector_id as connector_id,
      nullif(trim(region), '') as region,
      nullif(trim(station_like), '') as station_like,
      case
        when limit_rows is null then null
        else least(20000, greatest(1, limit_rows))::int
      end as limit_rows
  )
  select
    s.id,
    s.station_ref,
    s.label,
    case
      when s.geometry is null then null
      else st_asgeojson(s.geometry)::jsonb
    end as geometry,
    coalesce(memberships.memberships, '[]'::jsonb) as network_memberships
  from uk_aq_core.stations s
  left join lateral (
    select jsonb_agg(
      jsonb_build_object(
        'network_code', snm.network_code,
        'network_label', snm.network_label,
        'is_primary', snm.is_primary
      )
      order by snm.network_code
    ) as memberships
    from uk_aq_core.station_network_memberships snm
    where snm.station_id = s.id
  ) memberships on true
  cross join params
  where s.geometry is not null
    and (params.connector_id is null or s.connector_id = params.connector_id)
    and (params.region is null or s.region ilike '%' || params.region || '%')
    and (params.station_like is null or s.label ilike '%' || params.station_like || '%')
  limit (select limit_rows from params);
$$;

grant execute on function uk_aq_public.uk_aq_stations_rpc(
  integer,
  text,
  text,
  int,
  int
) to anon, authenticated;

grant execute on function uk_aq_public.uk_aq_stations_rpc(
  integer,
  text,
  text,
  int,
  int
) to service_role;

-- uk_aq_surbiton_latest RPC for read-only access (Edge function backing).

drop function if exists uk_aq_public.uk_aq_surbiton_latest_rpc(
  text,
  text,
  text,
  text,
  int
);

create or replace function uk_aq_public.uk_aq_surbiton_latest_rpc(
  region text default null,
  station_like text default null,
  connector_id text default null,
  pollutant text default null,
  limit_rows int default 1000
)
returns table (
  id integer,
  timeseries_ref text,
  label text,
  uom text,
  last_value double precision,
  last_value_at timestamptz,
  connector_id integer,
  connector jsonb,
  station jsonb,
  phenomenon jsonb
)
language sql
security definer
set search_path = uk_aq_core, public, pg_catalog
as $$
  with params as (
    select
      nullif(trim(region), '') as region,
      nullif(trim(station_like), '') as station_like,
      nullif(trim(connector_id), '')::integer as connector_id,
      nullif(trim(pollutant), '') as pollutant,
      least(10000, greatest(1, coalesce(limit_rows, 1000)))::int as limit_rows
  ),
  pollutant_tokens as (
    select
      case
        when pollutant is null then array[]::text[]
        when replace(replace(lower(pollutant), ' ', ''), '_', '') in ('pm25', 'pm2.5', 'pm2-5') then
          array[
            lower(pollutant),
            'pm2.5',
            'pm25',
            'pm2-5',
            'pm2_5'
          ]
        else
          array[lower(pollutant)]
      end as tokens,
      pollutant
    from params
  ),
  base as (
    select
      ts.id,
      ts.timeseries_ref,
      ts.label,
      ts.uom,
      ts.last_value,
      ts.last_value_at,
      ts.connector_id,
      case
        when c.id is null then null
        else jsonb_build_object(
          'id', c.id,
          'connector_code', c.connector_code,
          'label', c.label,
          'display_name', c.display_name,
          'station_display_name_template', c.station_display_name_template
        )
      end as connector,
      case
        when s.id is null then null
        else jsonb_build_object(
          'id', s.id,
          'station_ref', s.station_ref,
          'label', s.label,
          'station_name', s.station_name,
          'region', s.region,
          'connector_id', s.connector_id
        )
      end as station,
      case
        when p.id is null then null
        else jsonb_build_object(
          'id', p.id,
          'label', p.label,
          'source_label', p.source_label,
          'notation', p.notation,
          'eionet_uri', p.source_label,
          'pollutant_label', p.pollutant_label,
          'observed_property_id', op.id,
          'observed_property_code', op.code,
          'observed_property_display_name', op.display_name,
          'observed_property_domain', op.domain,
          'canonical_uom', op.canonical_uom
        )
      end as phenomenon,
      s.label as station_label
    from uk_aq_core.timeseries ts
    left join uk_aq_core.connectors c on c.id = ts.connector_id
    left join uk_aq_core.stations s on s.id = ts.station_id
    left join uk_aq_core.phenomena p on p.id = ts.phenomenon_id
    left join uk_aq_core.observed_properties op on op.id = p.observed_property_id
    cross join params
    cross join pollutant_tokens pt
    where ts.last_value >= 0
      and ts.last_value_at is not null
      and (params.connector_id is null or ts.connector_id = params.connector_id)
      and (params.region is null or s.region ilike '%' || params.region || '%')
      and (
        params.pollutant is null
        or exists (
          select 1
          from unnest(pt.tokens) token
          where op.code = uk_aq_core.uk_aq_observed_property_code(null, token, token, token)
             or lower(coalesce(op.display_name, '')) = token
             or lower(coalesce(p.notation, '')) = token
             or lower(coalesce(p.pollutant_label, '')) = token
             or lower(coalesce(p.label, '')) = token
             or lower(coalesce(p.source_label, '')) = token
        )
      )
  ),
  series_matches as (
    select base.*
    from base
    where (select station_like from params) is null
       or base.label ilike '%' || (select station_like from params) || '%'
    order by base.id
    limit (select limit_rows from params)
  ),
  station_matches as (
    select base.*
    from base
    where (select station_like from params) is not null
      and base.station_label ilike '%' || (select station_like from params) || '%'
    order by base.id
    limit (select limit_rows from params)
  ),
  combined as (
    select 1 as src_rank, series_matches.*
    from series_matches
    union all
    select 2 as src_rank, station_matches.*
    from station_matches
  ),
  deduped as (
    select distinct on (id) *
    from combined
    order by id, src_rank
  )
  select
    id,
    timeseries_ref,
    label,
    uom,
    last_value,
    last_value_at,
    connector_id,
    connector,
    station,
    phenomenon
  from deduped
  order by src_rank, id
  limit (select limit_rows from params);
$$;

grant execute on function uk_aq_public.uk_aq_surbiton_latest_rpc(
  text,
  text,
  text,
  text,
  int
) to anon, authenticated;

grant execute on function uk_aq_public.uk_aq_surbiton_latest_rpc(
  text,
  text,
  text,
  text,
  int
) to service_role;

-- rpc_observations_window for explicit UTC observs windows (website/cache use).

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

-- station AQI source RPC (service_role): v1 simplified source output with
-- hourly means + sample_count only.

drop function if exists uk_aq_public.uk_aq_rpc_station_aqi_hourly_source(
  timestamptz,
  timestamptz,
  bigint[]
);

create or replace function uk_aq_public.uk_aq_rpc_station_aqi_hourly_source(
  p_window_start timestamptz,
  p_window_end timestamptz,
  p_station_ids bigint[] default null
)
returns table (
  station_id bigint,
  timestamp_hour_utc timestamptz,
  pollutant_code text,
  hourly_mean_ugm3 double precision,
  sample_count integer
)
language plpgsql
security definer
set search_path = uk_aq_core, public, pg_catalog
as $$
declare
  v_window_start timestamptz;
  v_window_end timestamptz;
begin
  if auth.role() <> 'service_role' then
    raise exception 'service_role required';
  end if;

  if p_window_start is null or p_window_end is null then
    raise exception 'p_window_start and p_window_end are required';
  end if;

  v_window_start := date_trunc('hour', p_window_start);
  v_window_end := date_trunc('hour', p_window_end);

  if v_window_end <= v_window_start then
    raise exception 'p_window_end must be greater than p_window_start';
  end if;

  return query
  with raw as (
    select
      ts.id as timeseries_id,
      ts.station_id,
      op.code as pollutant_code,
      o.observed_at,
      o.value
    from uk_aq_core.observations o
    join uk_aq_core.timeseries ts
      on ts.id = o.timeseries_id
     and ts.connector_id = o.connector_id
    join uk_aq_core.phenomena p
      on p.id = ts.phenomenon_id
    join uk_aq_core.observed_properties op
      on op.id = p.observed_property_id
    where o.observed_at >= v_window_start
      and o.observed_at < v_window_end
      and ts.station_id is not null
      and op.code in ('pm25', 'pm10', 'no2')
      and o.value is not null
      and o.value >= 0
      and (
        p_station_ids is null
        or ts.station_id = any(p_station_ids)
      )
  ),
  hourly_by_timeseries as (
    select
      r.station_id,
      r.timeseries_id,
      r.pollutant_code,
      date_trunc('hour', r.observed_at at time zone 'UTC') at time zone 'UTC' as timestamp_hour_utc,
      avg(r.value)::double precision as hourly_mean_ugm3,
      count(*)::int as sample_count
    from raw r
    group by
      r.station_id,
      r.timeseries_id,
      r.pollutant_code,
      date_trunc('hour', r.observed_at at time zone 'UTC') at time zone 'UTC'
  ),
  ranked as (
    select
      h.station_id,
      h.timestamp_hour_utc,
      h.pollutant_code,
      h.hourly_mean_ugm3,
      h.sample_count,
      row_number() over (
        partition by h.station_id, h.timestamp_hour_utc, h.pollutant_code
        order by
          h.sample_count desc,
          h.timeseries_id asc
      ) as rn
    from hourly_by_timeseries h
  )
  select
    r.station_id,
    r.timestamp_hour_utc,
    r.pollutant_code,
    r.hourly_mean_ugm3,
    r.sample_count
  from ranked r
  where r.rn = 1
  order by
    r.timestamp_hour_utc,
    r.station_id,
    r.pollutant_code;
end;
$$;

revoke all on function uk_aq_public.uk_aq_rpc_station_aqi_hourly_source(
  timestamptz,
  timestamptz,
  bigint[]
) from public;

revoke all on function uk_aq_public.uk_aq_rpc_station_aqi_hourly_source(
  timestamptz,
  timestamptz,
  bigint[]
) from anon, authenticated;

grant execute on function uk_aq_public.uk_aq_rpc_station_aqi_hourly_source(
  timestamptz,
  timestamptz,
  bigint[]
) to service_role;

-- Public RPCs for non-exposed schemas (service_role only)
do $$
declare
  v_fn record;
begin
  for v_fn in
    select
      p.proname,
      pg_get_function_identity_arguments(p.oid) as identity_args
    from pg_proc p
    join pg_namespace n on n.oid = p.pronamespace
    where n.nspname = 'uk_aq_public'
      and p.proname = any(array[
        'uk_aq_rpc_connector_select',
        'uk_aq_rpc_station_names',
        'uk_aq_rpc_station_ids',
        'uk_aq_rpc_openaq_station_checkpoints_select',
        'uk_aq_rpc_openaq_station_checkpoints_upsert',
        'uk_aq_rpc_openaq_timeseries_checkpoints_select',
        'uk_aq_rpc_openaq_timeseries_checkpoints_upsert',
        'uk_aq_rpc_openaq_select_station_refs',
        'uk_aq_rpc_dispatch_claim',
        'uk_aq_rpc_latest_ingest_runs',
        'uk_aq_rpc_stations_upsert',
        'uk_aq_rpc_station_metadata_upsert',
        'uk_aq_rpc_phenomena_upsert',
        'uk_aq_rpc_phenomena_ids',
        'uk_aq_rpc_timeseries_upsert',
        'uk_aq_rpc_timeseries_ids',
        'uk_aq_rpc_timeseries_refs_by_station_ids',
        'uk_aq_rpc_observations_upsert',
        'uk_aq_rpc_timeseries_last_values_update',
        'uk_aq_rpc_error_log_insert'
      ])
  loop
    execute format(
      'drop function if exists uk_aq_public.%I(%s)',
      v_fn.proname,
      v_fn.identity_args
    );
  end loop;
end
$$;

create or replace function uk_aq_public.uk_aq_rpc_connector_select(connector_code text)
returns table (
  id integer,
  connector_code text,
  label text,
  service_url text,
  overwrite_station_name boolean
)
language sql
security definer
set search_path = uk_aq_core, uk_aq_raw, public, pg_catalog
as $$
  select c.id, c.connector_code, c.label, c.service_url, c.overwrite_station_name
  from uk_aq_core.connectors c
  where c.connector_code = $1
  limit 1;
$$;

create or replace function uk_aq_public.uk_aq_rpc_station_names(
  connector_id integer,
  service_ref text,
  station_refs text[]
)
returns table (
  station_ref text,
  station_name text
)
language sql
security definer
set search_path = uk_aq_core, uk_aq_raw, public, pg_catalog
as $$
  select s.station_ref, s.station_name
  from uk_aq_core.stations s
  where s.connector_id = $1
    and s.service_ref = $2
    and s.station_ref = any($3);
$$;

create or replace function uk_aq_public.uk_aq_rpc_station_ids(
  connector_id integer,
  service_ref text,
  station_refs text[]
)
returns table (
  station_ref text,
  id bigint
)
language sql
security definer
set search_path = uk_aq_core, uk_aq_raw, public, pg_catalog
as $$
  select s.station_ref, s.id
  from uk_aq_core.stations s
  where s.connector_id = $1
    and s.service_ref = $2
    and s.station_ref = any($3);
$$;

create or replace function uk_aq_public.uk_aq_rpc_openaq_station_checkpoints_select(
  station_ids bigint[]
)
returns table (
  station_id bigint,
  next_due_at timestamptz,
  last_observed_at timestamptz,
  observ_interval_samples int[],
  ingest_lag_samples int[],
  last_polled_at timestamptz
)
language sql
security definer
set search_path = uk_aq_raw, public, pg_catalog
as $$
  select
    station_id,
    next_due_at,
    last_observed_at,
    observ_interval_samples,
    ingest_lag_samples,
    last_polled_at
  from uk_aq_raw.openaq_station_checkpoints
  where station_id = any($1);
$$;

create or replace function uk_aq_public.uk_aq_rpc_openaq_station_checkpoints_upsert(rows jsonb)
returns table (rows_upserted int)
language plpgsql
security definer
set search_path = uk_aq_raw, public, pg_catalog
as $$
declare
  count_rows int := 0;
begin
  if rows is null or jsonb_typeof(rows) <> 'array' or jsonb_array_length(rows) = 0 then
    return query select 0;
    return;
  end if;
  insert into uk_aq_raw.openaq_station_checkpoints (
    station_id,
    next_due_at,
    last_observed_at,
    observ_interval_samples,
    ingest_lag_samples,
    last_polled_at
  )
  select
    r.station_id,
    r.next_due_at,
    r.last_observed_at,
    r.observ_interval_samples,
    r.ingest_lag_samples,
    r.last_polled_at
  from jsonb_to_recordset(rows) as r(
    station_id bigint,
    next_due_at timestamptz,
    last_observed_at timestamptz,
    observ_interval_samples int[],
    ingest_lag_samples int[],
    last_polled_at timestamptz
  )
  on conflict (station_id) do update set
    next_due_at = excluded.next_due_at,
    last_observed_at = excluded.last_observed_at,
    observ_interval_samples = excluded.observ_interval_samples,
    ingest_lag_samples = excluded.ingest_lag_samples,
    last_polled_at = excluded.last_polled_at,
    updated_at = now();
  get diagnostics count_rows = row_count;
  return query select count_rows;
end;
$$;

create or replace function uk_aq_public.uk_aq_rpc_openaq_timeseries_checkpoints_select(
  station_ids bigint[]
)
returns table (
  station_id bigint,
  timeseries_id integer,
  next_due_at timestamptz,
  last_observed_at timestamptz,
  ingest_lag_samples int[],
  last_polled_at timestamptz
)
language sql
security definer
set search_path = uk_aq_raw, public, pg_catalog
as $$
  select
    station_id,
    timeseries_id,
    next_due_at,
    last_observed_at,
    ingest_lag_samples,
    last_polled_at
  from uk_aq_raw.openaq_timeseries_checkpoints
  where station_id = any($1);
$$;

create or replace function uk_aq_public.uk_aq_rpc_openaq_timeseries_checkpoints_upsert(rows jsonb)
returns table (rows_upserted int)
language plpgsql
security definer
set search_path = uk_aq_raw, public, pg_catalog
as $$
declare
  count_rows int := 0;
begin
  if rows is null or jsonb_typeof(rows) <> 'array' or jsonb_array_length(rows) = 0 then
    return query select 0;
    return;
  end if;
  insert into uk_aq_raw.openaq_timeseries_checkpoints (
    station_id,
    timeseries_id,
    next_due_at,
    last_observed_at,
    ingest_lag_samples,
    last_polled_at
  )
  select
    r.station_id,
    r.timeseries_id,
    r.next_due_at,
    r.last_observed_at,
    r.ingest_lag_samples,
    r.last_polled_at
  from jsonb_to_recordset(rows) as r(
    station_id bigint,
    timeseries_id integer,
    next_due_at timestamptz,
    last_observed_at timestamptz,
    ingest_lag_samples int[],
    last_polled_at timestamptz
  )
  on conflict (station_id, timeseries_id) do update set
    next_due_at = excluded.next_due_at,
    last_observed_at = excluded.last_observed_at,
    ingest_lag_samples = excluded.ingest_lag_samples,
    last_polled_at = excluded.last_polled_at,
    updated_at = now();
  get diagnostics count_rows = row_count;
  return query select count_rows;
end;
$$;

drop function if exists uk_aq_public.uk_aq_rpc_openaq_select_station_refs(integer, integer);

create or replace function uk_aq_public.uk_aq_rpc_openaq_select_station_refs(
  batch_limit integer default 50,
  stale_limit integer default 10,
  tier1_retry_seconds integer default 300
)
returns table (
  station_ref text,
  station_id bigint
)
language plpgsql
security definer
set search_path = uk_aq_core, uk_aq_raw, public, pg_catalog
as $$
declare
  v_connector_id integer;
begin
  select id into v_connector_id
  from uk_aq_core.connectors
  where connector_code = 'openaq'
  limit 1;

  if v_connector_id is null then
    return;
  end if;

  return query
  with latest_obs as (
    select
      t.station_id,
      max(t.last_value_at) as last_observed_at
    from uk_aq_core.timeseries t
    where t.connector_id = v_connector_id
      and t.service_ref = 'openaq'
    group by t.station_id
  ),
  candidates as (
    select
      stn.id as station_id,
      stn.station_ref as station_ref,
      osc.next_due_at,
      osc.last_polled_at,
      coalesce(osc.last_observed_at, lo.last_observed_at) as last_observed_at,
      coalesce(osc.next_due_at, now()) as due_at
    from uk_aq_core.stations stn
    left join uk_aq_raw.openaq_station_checkpoints osc
      on osc.station_id = stn.id
    left join latest_obs lo
      on lo.station_id = stn.id
    where stn.connector_id = v_connector_id
      and stn.service_ref = 'openaq'
      and stn.station_ref is not null
      and stn.removed_at is null
  ),
  tiered as (
    select
      c.station_id,
      c.station_ref,
      c.due_at,
      c.last_polled_at
    from candidates c
    where c.due_at <= now()
      and c.due_at >= now() - interval '6 hours'
      and (
        c.last_polled_at is null or
        c.last_polled_at <= now() - make_interval(secs => greatest(0, tier1_retry_seconds))
      )
    union all
    select
      c.station_id,
      c.station_ref,
      c.due_at,
      c.last_polled_at
    from candidates c
    where c.due_at < now() - interval '6 hours'
      and c.due_at >= now() - interval '24 hours'
      and (c.last_polled_at is null or c.last_polled_at <= now() - interval '1 hour')
  ),
  tiered_limited as (
    select *
    from tiered
    order by last_polled_at asc nulls first, due_at asc
    limit batch_limit
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
    limit stale_limit
  ),
  combined as (
    select tl.station_ref, tl.station_id, 1 as group_order, tl.due_at as sort_at
    from tiered_limited tl
    union all
    select s.station_ref, s.station_id, 2 as group_order, null as sort_at
    from stale s
  )
  select combined.station_ref, combined.station_id
  from combined
  order by combined.group_order, combined.sort_at nulls last;
end;
$$;

create or replace function uk_aq_public.uk_aq_rpc_dispatch_claim(
  p_connector_code text,
  p_run_started_at timestamptz,
  p_timeout_minutes integer default 10
)
returns table (
  claimed boolean,
  connector_id integer,
  last_run_start timestamptz,
  last_run_end timestamptz
)
language plpgsql
security definer
set search_path = uk_aq_core, uk_aq_raw, public, pg_catalog
as $$
declare
  v_timeout interval;
begin
  if auth.role() <> 'service_role' then
    raise exception 'service_role required';
  end if;

  v_timeout := make_interval(mins => greatest(1, coalesce(p_timeout_minutes, 10)));

  return query
  with updated as (
    update uk_aq_core.connectors c
    set
      last_run_start = p_run_started_at,
      last_run_end = null,
      last_run_status = 'running',
      last_run_message = 'dispatching'
    where c.connector_code = p_connector_code
      and (
        c.last_run_end is not null
        or c.last_run_start is null
        or c.last_run_start <= now() - v_timeout
      )
    returning c.id, c.last_run_start, c.last_run_end
  )
  select
    (count(*) > 0) as claimed,
    max(updated.id) as connector_id,
    max(updated.last_run_start) as last_run_start,
    max(updated.last_run_end) as last_run_end
  from updated;
end;
$$;

create or replace function uk_aq_public.uk_aq_rpc_latest_ingest_runs(
  p_connector_codes text[] default null,
  p_since timestamptz default null
)
returns table (
  connector_id integer,
  connector_code text,
  run_started_at timestamptz,
  run_ended_at timestamptz,
  run_status text
)
language sql
security definer
set search_path = uk_aq_core, uk_aq_raw, public, pg_catalog
as $$
  with filtered as (
    select
      r.id,
      r.connector_id,
      r.connector_code,
      r.run_started_at,
      r.run_ended_at,
      r.run_status
    from uk_aq_core.uk_aq_ingest_runs r
    where (
      p_connector_codes is null
      or array_length(p_connector_codes, 1) is null
      or r.connector_code = any(p_connector_codes)
    )
      and (
        p_since is null
        or r.run_started_at >= p_since
      )
  )
  select distinct on (f.connector_code)
    f.connector_id,
    f.connector_code,
    f.run_started_at,
    f.run_ended_at,
    f.run_status
  from filtered f
  where f.connector_code is not null
    and btrim(f.connector_code) <> ''
  order by
    f.connector_code,
    f.run_started_at desc nulls last,
    f.id desc;
$$;

create or replace function uk_aq_public.uk_aq_rpc_stations_upsert(rows jsonb)
returns table (stations_upserted int)
language plpgsql
security definer
set search_path = uk_aq_core, uk_aq_raw, public, pg_catalog
as $$
declare
  count_rows int := 0;
begin
  if rows is null or jsonb_typeof(rows) <> 'array' or jsonb_array_length(rows) = 0 then
    return query select 0;
    return;
  end if;
  insert into uk_aq_core.stations (
    station_ref,
    service_ref,
    label,
    station_name,
    station_type,
    region,
    geometry,
    connector_id,
    last_seen_at,
    removed_at
  )
  select
    r.station_ref,
    r.service_ref,
    r.label,
    r.station_name,
    r.station_type,
    r.region,
    case
      when r.geometry is null or r.geometry = '' then null
      else ST_GeogFromText(r.geometry)
    end,
    r.connector_id,
    r.last_seen_at,
    r.removed_at
  from jsonb_to_recordset(rows) as r(
    station_ref text,
    service_ref text,
    label text,
    station_name text,
    station_type text,
    region text,
    geometry text,
    connector_id integer,
    last_seen_at timestamptz,
    removed_at timestamptz
  )
  on conflict (connector_id, service_ref, station_ref) do update set
    label = excluded.label,
    station_name = excluded.station_name,
    station_type = excluded.station_type,
    region = excluded.region,
    geometry = excluded.geometry,
    last_seen_at = excluded.last_seen_at,
    removed_at = excluded.removed_at;
  get diagnostics count_rows = row_count;
  return query select count_rows;
end;
$$;

create or replace function uk_aq_public.uk_aq_rpc_station_metadata_upsert(rows jsonb)
returns table (station_metadata_upserted int)
language plpgsql
security definer
set search_path = uk_aq_core, uk_aq_raw, public, pg_catalog
as $$
declare
  count_rows int := 0;
begin
  if rows is null or jsonb_typeof(rows) <> 'array' or jsonb_array_length(rows) = 0 then
    return query select 0;
    return;
  end if;
  insert into uk_aq_core.station_metadata (
    station_id,
    attributes,
    updated_at
  )
  select
    r.station_id,
    coalesce(r.attributes, '{}'::jsonb),
    coalesce(r.updated_at, now())
  from jsonb_to_recordset(rows) as r(
    station_id bigint,
    attributes jsonb,
    updated_at timestamptz
  )
  on conflict (station_id) do update set
    attributes = uk_aq_core.station_metadata.attributes || excluded.attributes,
    updated_at = excluded.updated_at;
  get diagnostics count_rows = row_count;
  return query select count_rows;
end;
$$;

create or replace function uk_aq_public.uk_aq_rpc_phenomena_upsert(rows jsonb)
returns table (phenomena_upserted int)
language plpgsql
security definer
set search_path = uk_aq_core, uk_aq_raw, public, pg_catalog
as $$
declare
  count_rows int := 0;
begin
  if rows is null or jsonb_typeof(rows) <> 'array' or jsonb_array_length(rows) = 0 then
    return query select 0;
    return;
  end if;

  with parsed as (
    select
      nullif(item->>'connector_id', '')::integer as connector_id,
      nullif(trim(coalesce(item->>'source_label', item->>'eionet_uri')), '') as source_label,
      nullif(trim(item->>'label'), '') as label,
      nullif(trim(item->>'notation'), '') as notation,
      nullif(trim(item->>'pollutant_label'), '') as pollutant_label,
      nullif(trim(item->>'observed_property_code'), '') as observed_property_code,
      nullif(trim(item->>'observed_property_display_name'), '') as observed_property_display_name,
      nullif(trim(item->>'observed_property_domain'), '') as observed_property_domain,
      nullif(trim(coalesce(item->>'canonical_uom', item->>'observed_property_canonical_uom')), '') as canonical_uom
    from jsonb_array_elements(rows) item
  ),
  normalized as (
    select
      p.connector_id,
      p.source_label,
      coalesce(
        p.label,
        p.notation,
        p.pollutant_label,
        p.source_label,
        'unknown'
      ) as label,
      p.notation,
      p.pollutant_label,
      coalesce(
        nullif(lower(regexp_replace(p.observed_property_code, '[^a-z0-9]+', '', 'g')), ''),
        uk_aq_core.uk_aq_observed_property_code(
          p.source_label,
          p.notation,
          p.pollutant_label,
          p.label
        )
      ) as observed_property_code,
      p.observed_property_display_name,
      p.observed_property_domain,
      p.canonical_uom
    from parsed p
    where p.connector_id is not null
  )
  insert into uk_aq_core.observed_properties (
    code,
    display_name,
    domain,
    canonical_uom
  )
  select
    per_code.code,
    per_code.display_name,
    per_code.domain,
    per_code.canonical_uom
  from (
    select
      n.observed_property_code as code,
      coalesce(
        max(n.observed_property_display_name),
        max(n.notation),
        max(n.pollutant_label),
        max(n.label),
        initcap(replace(n.observed_property_code, '_', ' '))
      ) as display_name,
      coalesce(
        max(n.observed_property_domain),
        uk_aq_core.uk_aq_observed_property_domain(n.observed_property_code)
      ) as domain,
      coalesce(
        max(n.canonical_uom),
        uk_aq_core.uk_aq_observed_property_default_uom(n.observed_property_code)
      ) as canonical_uom
    from normalized n
    where n.observed_property_code is not null
    group by n.observed_property_code
  ) per_code
  on conflict (code) do update
  set
    domain = excluded.domain,
    canonical_uom = coalesce(uk_aq_core.observed_properties.canonical_uom, excluded.canonical_uom),
    updated_at = now();

  with parsed as (
    select
      nullif(item->>'connector_id', '')::integer as connector_id,
      nullif(trim(coalesce(item->>'source_label', item->>'eionet_uri')), '') as source_label,
      nullif(trim(item->>'label'), '') as label,
      nullif(trim(item->>'notation'), '') as notation,
      nullif(trim(item->>'pollutant_label'), '') as pollutant_label,
      nullif(trim(item->>'observed_property_code'), '') as observed_property_code
    from jsonb_array_elements(rows) item
  ),
  normalized as (
    select
      p.connector_id,
      p.source_label,
      coalesce(
        p.label,
        p.notation,
        p.pollutant_label,
        p.source_label,
        'unknown'
      ) as label,
      p.notation,
      p.pollutant_label,
      coalesce(
        nullif(lower(regexp_replace(p.observed_property_code, '[^a-z0-9]+', '', 'g')), ''),
        uk_aq_core.uk_aq_observed_property_code(
          p.source_label,
          p.notation,
          p.pollutant_label,
          p.label
        )
      ) as observed_property_code
    from parsed p
    where p.connector_id is not null
  )
  insert into uk_aq_core.phenomena (
    connector_id,
    source_label,
    label,
    notation,
    pollutant_label,
    observed_property_id
  )
  select
    n.connector_id,
    n.source_label,
    n.label,
    n.notation,
    n.pollutant_label,
    op.id as observed_property_id
  from normalized n
  left join uk_aq_core.observed_properties op
    on op.code = n.observed_property_code
  on conflict (connector_id, source_label) do update set
    label = excluded.label,
    notation = excluded.notation,
    pollutant_label = excluded.pollutant_label,
    observed_property_id = coalesce(excluded.observed_property_id, uk_aq_core.phenomena.observed_property_id);
  get diagnostics count_rows = row_count;
  return query select count_rows;
end;
$$;

create or replace function uk_aq_public.uk_aq_rpc_phenomena_ids(
  connector_id integer,
  eionet_uris text[]
)
returns table (
  eionet_uri text,
  id bigint
)
language sql
security definer
set search_path = uk_aq_core, uk_aq_raw, public, pg_catalog
as $$
  select p.source_label as eionet_uri, p.id
  from uk_aq_core.phenomena p
  where p.connector_id = $1
    and p.source_label = any($2);
$$;

create or replace function uk_aq_public.uk_aq_rpc_timeseries_upsert(rows jsonb)
returns table (timeseries_upserted int)
language plpgsql
security definer
set search_path = uk_aq_core, uk_aq_raw, public, pg_catalog
as $$
declare
  count_rows int := 0;
begin
  if rows is null or jsonb_typeof(rows) <> 'array' or jsonb_array_length(rows) = 0 then
    return query select 0;
    return;
  end if;
  insert into uk_aq_core.timeseries (
    timeseries_ref,
    label,
    uom,
    station_id,
    connector_id,
    service_ref,
    phenomenon_id
  )
  select
    r.timeseries_ref,
    r.label,
    r.uom,
    r.station_id,
    r.connector_id,
    r.service_ref,
    r.phenomenon_id
  from jsonb_to_recordset(rows) as r(
    timeseries_ref text,
    label text,
    uom text,
    station_id bigint,
    connector_id integer,
    service_ref text,
    phenomenon_id bigint
  )
  on conflict (connector_id, service_ref, timeseries_ref) do update set
    label = excluded.label,
    uom = excluded.uom,
    station_id = excluded.station_id,
    phenomenon_id = excluded.phenomenon_id;
  get diagnostics count_rows = row_count;
  return query select count_rows;
end;
$$;

create or replace function uk_aq_public.uk_aq_rpc_timeseries_ids(
  connector_id integer,
  service_ref text,
  timeseries_refs text[]
)
returns table (
  timeseries_ref text,
  id integer
)
language sql
security definer
set search_path = uk_aq_core, uk_aq_raw, public, pg_catalog
as $$
  select t.timeseries_ref, t.id
  from uk_aq_core.timeseries t
  where t.connector_id = $1
    and t.service_ref = $2
    and t.timeseries_ref = any($3);
$$;

create or replace function uk_aq_public.uk_aq_rpc_timeseries_refs_by_station_ids(
  connector_id integer,
  service_ref text,
  station_ids bigint[]
)
returns table (
  station_id bigint,
  timeseries_id integer,
  timeseries_ref text
)
language sql
security definer
set search_path = uk_aq_core, uk_aq_raw, public, pg_catalog
as $$
  select t.station_id, t.id as timeseries_id, t.timeseries_ref
  from uk_aq_core.timeseries t
  where t.connector_id = $1
    and t.service_ref = $2
    and t.station_id = any($3);
$$;

create or replace function uk_aq_public.uk_aq_rpc_observations_upsert(rows jsonb)
returns table (observations_upserted int)
language plpgsql
security definer
set search_path = uk_aq_core, uk_aq_raw, public, pg_catalog
as $$
declare
  count_rows int := 0;
  v_started_at timestamptz := clock_timestamp();
  v_input_rows int := 0;
  v_payload_bytes int := 0;
  v_duration_ms int := 0;
begin
  if rows is null or jsonb_typeof(rows) <> 'array' or jsonb_array_length(rows) = 0 then
    return query select 0;
    return;
  end if;

  v_input_rows := jsonb_array_length(rows);
  v_payload_bytes := pg_column_size(rows);

  insert into uk_aq_core.observations (
    connector_id,
    timeseries_id,
    observed_at,
    value,
    status
  )
  select
    r.connector_id,
    r.timeseries_id,
    r.observed_at,
    r.value,
    r.status
  from jsonb_to_recordset(rows) as r(
    connector_id integer,
    timeseries_id integer,
    observed_at timestamptz,
    value double precision,
    status text
  )
  on conflict (connector_id, timeseries_id, observed_at) do update set
    value = excluded.value,
    status = excluded.status
  where
    uk_aq_core.observations.value is distinct from excluded.value
    or uk_aq_core.observations.status is distinct from excluded.status;
  get diagnostics count_rows = row_count;

  v_duration_ms := greatest(
    0,
    floor(extract(epoch from (clock_timestamp() - v_started_at)) * 1000)::int
  );

  insert into uk_aq_raw.observation_rpc_metrics_minute (
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
    'rpc/uk_aq_rpc_observations_upsert',
    1,
    v_input_rows,
    v_payload_bytes,
    coalesce(count_rows, 0),
    v_duration_ms,
    v_duration_ms
  )
  on conflict (bucket_minute, endpoint)
  do update set
    calls = uk_aq_raw.observation_rpc_metrics_minute.calls + 1,
    rows_input = uk_aq_raw.observation_rpc_metrics_minute.rows_input + excluded.rows_input,
    payload_bytes = uk_aq_raw.observation_rpc_metrics_minute.payload_bytes + excluded.payload_bytes,
    rows_upserted = uk_aq_raw.observation_rpc_metrics_minute.rows_upserted + excluded.rows_upserted,
    duration_ms_sum = uk_aq_raw.observation_rpc_metrics_minute.duration_ms_sum + excluded.duration_ms_sum,
    duration_ms_max = greatest(
      uk_aq_raw.observation_rpc_metrics_minute.duration_ms_max,
      excluded.duration_ms_max
    );

  return query select count_rows;
end;
$$;

create or replace function uk_aq_public.uk_aq_rpc_timeseries_last_values_update(rows jsonb)
returns table (timeseries_updated int)
language plpgsql
security definer
set search_path = uk_aq_core, uk_aq_raw, public, pg_catalog
as $$
declare
  count_rows int := 0;
begin
  if rows is null or jsonb_typeof(rows) <> 'array' or jsonb_array_length(rows) = 0 then
    return query select 0;
    return;
  end if;
  with updates as (
    select * from jsonb_to_recordset(rows) as r(
      id bigint,
      last_value double precision,
      last_value_at timestamptz
    )
  )
  update uk_aq_core.timeseries t
  set last_value = u.last_value,
      last_value_at = u.last_value_at
  from updates u
  where t.id = u.id
    and (
      t.last_value is distinct from u.last_value
      or t.last_value_at is distinct from u.last_value_at
    );
  get diagnostics count_rows = row_count;
  return query select count_rows;
end;
$$;

create or replace function uk_aq_public.uk_aq_rpc_error_log_insert(entry jsonb)
returns table (id uuid)
language plpgsql
security definer
set search_path = uk_aq_raw, uk_aq_core, public, pg_catalog
as $$
declare
  new_id uuid;
begin
  insert into uk_aq_raw.error_logs (
    source,
    severity,
    message,
    stack,
    context,
    connector_id,
    station_id,
    timeseries_id,
    dropbox_path
  )
  values (
    coalesce(entry->>'source', 'unknown'),
    coalesce(entry->>'severity', 'error'),
    coalesce(entry->>'message', 'unknown'),
    entry->>'stack',
    entry->'context',
    nullif(entry->>'connector_id', '')::integer,
    nullif(entry->>'station_id', '')::bigint,
    nullif(entry->>'timeseries_id', '')::integer,
    entry->>'dropbox_path'
  )
  returning uk_aq_raw.error_logs.id into new_id;
  return query select new_id;
end;
$$;

revoke all on function uk_aq_public.uk_aq_rpc_connector_select(text) from public;
grant execute on function uk_aq_public.uk_aq_rpc_connector_select(text) to service_role;

revoke all on function uk_aq_public.uk_aq_rpc_station_names(integer, text, text[]) from public;
grant execute on function uk_aq_public.uk_aq_rpc_station_names(integer, text, text[]) to service_role;

revoke all on function uk_aq_public.uk_aq_rpc_station_ids(integer, text, text[]) from public;
grant execute on function uk_aq_public.uk_aq_rpc_station_ids(integer, text, text[]) to service_role;

revoke all on function uk_aq_public.uk_aq_rpc_openaq_station_checkpoints_select(bigint[]) from public;
grant execute on function uk_aq_public.uk_aq_rpc_openaq_station_checkpoints_select(bigint[]) to service_role;

revoke all on function uk_aq_public.uk_aq_rpc_openaq_station_checkpoints_upsert(jsonb) from public;
grant execute on function uk_aq_public.uk_aq_rpc_openaq_station_checkpoints_upsert(jsonb) to service_role;

revoke all on function uk_aq_public.uk_aq_rpc_openaq_timeseries_checkpoints_select(bigint[]) from public;
grant execute on function uk_aq_public.uk_aq_rpc_openaq_timeseries_checkpoints_select(bigint[]) to service_role;

revoke all on function uk_aq_public.uk_aq_rpc_openaq_timeseries_checkpoints_upsert(jsonb) from public;
grant execute on function uk_aq_public.uk_aq_rpc_openaq_timeseries_checkpoints_upsert(jsonb) to service_role;

revoke all on function uk_aq_public.uk_aq_rpc_openaq_select_station_refs(integer, integer, integer) from public;
grant execute on function uk_aq_public.uk_aq_rpc_openaq_select_station_refs(integer, integer, integer) to service_role;

revoke all on function uk_aq_public.uk_aq_rpc_dispatch_claim(text, timestamptz, integer) from public;
grant execute on function uk_aq_public.uk_aq_rpc_dispatch_claim(text, timestamptz, integer) to service_role;

revoke all on function uk_aq_public.uk_aq_rpc_latest_ingest_runs(text[], timestamptz) from public;
grant execute on function uk_aq_public.uk_aq_rpc_latest_ingest_runs(text[], timestamptz) to service_role;

revoke all on function uk_aq_public.uk_aq_rpc_stations_upsert(jsonb) from public;
grant execute on function uk_aq_public.uk_aq_rpc_stations_upsert(jsonb) to service_role;

revoke all on function uk_aq_public.uk_aq_rpc_station_metadata_upsert(jsonb) from public;
grant execute on function uk_aq_public.uk_aq_rpc_station_metadata_upsert(jsonb) to service_role;

revoke all on function uk_aq_public.uk_aq_rpc_phenomena_upsert(jsonb) from public;
grant execute on function uk_aq_public.uk_aq_rpc_phenomena_upsert(jsonb) to service_role;

revoke all on function uk_aq_public.uk_aq_rpc_phenomena_ids(integer, text[]) from public;
grant execute on function uk_aq_public.uk_aq_rpc_phenomena_ids(integer, text[]) to service_role;

revoke all on function uk_aq_public.uk_aq_rpc_timeseries_upsert(jsonb) from public;
grant execute on function uk_aq_public.uk_aq_rpc_timeseries_upsert(jsonb) to service_role;

revoke all on function uk_aq_public.uk_aq_rpc_timeseries_ids(integer, text, text[]) from public;
grant execute on function uk_aq_public.uk_aq_rpc_timeseries_ids(integer, text, text[]) to service_role;

revoke all on function uk_aq_public.uk_aq_rpc_timeseries_refs_by_station_ids(
  integer,
  text,
  bigint[]
) from public;
grant execute on function uk_aq_public.uk_aq_rpc_timeseries_refs_by_station_ids(
  integer,
  text,
  bigint[]
) to service_role;

revoke all on function uk_aq_public.uk_aq_rpc_observations_upsert(jsonb) from public;
grant execute on function uk_aq_public.uk_aq_rpc_observations_upsert(jsonb) to service_role;

revoke all on function uk_aq_public.uk_aq_rpc_timeseries_last_values_update(jsonb) from public;
grant execute on function uk_aq_public.uk_aq_rpc_timeseries_last_values_update(jsonb) to service_role;

revoke all on function uk_aq_public.uk_aq_rpc_error_log_insert(jsonb) from public;
grant execute on function uk_aq_public.uk_aq_rpc_error_log_insert(jsonb) to service_role;


drop function if exists uk_aq_public.uk_aq_rpc_station_aqi_hourly_helper_upsert(
  timestamptz,
  timestamptz,
  bigint[],
  timestamptz
);

create or replace function uk_aq_public.uk_aq_rpc_station_aqi_hourly_helper_upsert(
  p_hour_end_start_exclusive timestamptz,
  p_hour_end_end_inclusive timestamptz,
  p_station_ids bigint[] default null,
  p_reference_hour_end_utc timestamptz default null
)
returns table (
  source_rows integer,
  rows_upserted integer,
  station_hours_changed integer,
  max_changed_lag_hours numeric
)
language plpgsql
security definer
set search_path = uk_aq_aqilevels, uk_aq_core, uk_aq_public, public, pg_catalog
as $$
declare
  v_start_exclusive timestamptz;
  v_end_inclusive timestamptz;
  v_source_start timestamptz;
  v_source_end timestamptz;
  v_reference_end timestamptz;
  v_source_rows integer := 0;
  v_rows_upserted integer := 0;
  v_station_hours_changed integer := 0;
  v_max_changed_lag_hours numeric := null;
begin
  if auth.role() <> 'service_role' then
    raise exception 'service_role required';
  end if;

  if p_hour_end_start_exclusive is null or p_hour_end_end_inclusive is null then
    raise exception 'p_hour_end_start_exclusive and p_hour_end_end_inclusive are required';
  end if;

  v_start_exclusive := date_trunc('hour', p_hour_end_start_exclusive);
  v_end_inclusive := date_trunc('hour', p_hour_end_end_inclusive);
  if v_end_inclusive <= v_start_exclusive then
    raise exception 'p_hour_end_end_inclusive must be greater than p_hour_end_start_exclusive';
  end if;

  v_source_start := v_start_exclusive - interval '23 hours';
  v_source_end := v_end_inclusive;
  v_reference_end := date_trunc('hour', coalesce(p_reference_hour_end_utc, v_end_inclusive));

  with source_rows as (
    with raw as (
      select
        ts.id as timeseries_id,
        ts.station_id,
        op.code as pollutant_code,
        o.observed_at,
        o.value
      from uk_aq_core.observations o
      join uk_aq_core.timeseries ts
        on ts.id = o.timeseries_id
       and ts.connector_id = o.connector_id
      join uk_aq_core.phenomena p
        on p.id = ts.phenomenon_id
      join uk_aq_core.observed_properties op
        on op.id = p.observed_property_id
      where o.observed_at >= v_source_start
        and o.observed_at < v_source_end
        and ts.station_id is not null
        and op.code in ('pm25', 'pm10', 'no2')
        and o.value is not null
        and o.value >= 0
        and (
          p_station_ids is null
          or ts.station_id = any(p_station_ids)
        )
    ),
    hourly_by_timeseries as (
      select
        r.station_id,
        r.timeseries_id,
        r.pollutant_code,
        date_trunc('hour', r.observed_at at time zone 'UTC') at time zone 'UTC' as timestamp_hour_utc,
        avg(r.value)::double precision as hourly_mean_ugm3,
        count(*)::int as sample_count
      from raw r
      group by
        r.station_id,
        r.timeseries_id,
        r.pollutant_code,
        date_trunc('hour', r.observed_at at time zone 'UTC') at time zone 'UTC'
    ),
    ranked as (
      select
        h.station_id,
        h.timestamp_hour_utc,
        h.pollutant_code,
        h.hourly_mean_ugm3,
        h.sample_count,
        row_number() over (
          partition by h.station_id, h.timestamp_hour_utc, h.pollutant_code
          order by
            h.sample_count desc,
            h.timeseries_id asc
        ) as rn
      from hourly_by_timeseries h
    )
    select
      r.station_id,
      r.timestamp_hour_utc,
      r.pollutant_code,
      r.hourly_mean_ugm3,
      r.sample_count
    from ranked r
    where r.rn = 1
  ),
  source_count as (
    select count(*)::integer as source_rows
    from source_rows
  ),
  hourly_pivot as (
    select
      s.station_id,
      s.timestamp_hour_utc,
      max(s.hourly_mean_ugm3) filter (where s.pollutant_code = 'no2') as no2_hourly_mean_ugm3,
      max(s.hourly_mean_ugm3) filter (where s.pollutant_code = 'pm25') as pm25_hourly_mean_ugm3,
      max(s.hourly_mean_ugm3) filter (where s.pollutant_code = 'pm10') as pm10_hourly_mean_ugm3,
      max(s.sample_count) filter (where s.pollutant_code = 'no2') as no2_hourly_sample_count_raw,
      max(s.sample_count) filter (where s.pollutant_code = 'pm25') as pm25_hourly_sample_count_raw,
      max(s.sample_count) filter (where s.pollutant_code = 'pm10') as pm10_hourly_sample_count_raw
    from source_rows s
    group by
      s.station_id,
      s.timestamp_hour_utc
  ),
  stations as (
    select distinct
      h.station_id
    from hourly_pivot h
  ),
  hour_grid as (
    select
      s.station_id,
      gs.timestamp_hour_utc
    from stations s
    cross join lateral generate_series(
      v_source_start,
      v_end_inclusive - interval '1 hour',
      interval '1 hour'
    ) as gs(timestamp_hour_utc)
  ),
  hourly as (
    select
      g.station_id,
      g.timestamp_hour_utc,
      h.no2_hourly_mean_ugm3,
      h.pm25_hourly_mean_ugm3,
      h.pm10_hourly_mean_ugm3,
      h.no2_hourly_sample_count_raw,
      h.pm25_hourly_sample_count_raw,
      h.pm10_hourly_sample_count_raw
    from hour_grid g
    left join hourly_pivot h
      on h.station_id = g.station_id
     and h.timestamp_hour_utc = g.timestamp_hour_utc
  ),
  with_rolling as (
    select
      h.station_id,
      h.timestamp_hour_utc,
      h.no2_hourly_mean_ugm3,
      h.pm25_hourly_mean_ugm3,
      h.pm10_hourly_mean_ugm3,
      h.no2_hourly_sample_count_raw,
      h.pm25_hourly_sample_count_raw,
      h.pm10_hourly_sample_count_raw,
      avg(h.pm25_hourly_mean_ugm3) over w as pm25_rolling24h_mean_raw,
      count(h.pm25_hourly_mean_ugm3) over w as pm25_rolling24h_valid_hours_raw,
      avg(h.pm10_hourly_mean_ugm3) over w as pm10_rolling24h_mean_raw,
      count(h.pm10_hourly_mean_ugm3) over w as pm10_rolling24h_valid_hours_raw
    from hourly h
    window w as (
      partition by h.station_id
      order by h.timestamp_hour_utc
      rows between 23 preceding and current row
    )
  ),
  target_hours as (
    select
      wr.station_id,
      wr.timestamp_hour_utc,
      wr.no2_hourly_mean_ugm3,
      wr.pm25_hourly_mean_ugm3,
      wr.pm10_hourly_mean_ugm3,
      wr.no2_hourly_sample_count_raw,
      wr.pm25_hourly_sample_count_raw,
      wr.pm10_hourly_sample_count_raw,
      case
        when wr.pm25_rolling24h_valid_hours_raw >= 18 then wr.pm25_rolling24h_mean_raw
        else null
      end as pm25_rolling24h_mean_ugm3,
      case
        when wr.pm10_rolling24h_valid_hours_raw >= 18 then wr.pm10_rolling24h_mean_raw
        else null
      end as pm10_rolling24h_mean_ugm3
    from with_rolling wr
    where wr.timestamp_hour_utc > (v_start_exclusive - interval '1 hour')
      and wr.timestamp_hour_utc <= (v_end_inclusive - interval '1 hour')
  ),
  computed as (
    select
      t.station_id,
      t.timestamp_hour_utc,
      t.no2_hourly_mean_ugm3,
      t.pm25_hourly_mean_ugm3,
      t.pm10_hourly_mean_ugm3,
      t.pm25_rolling24h_mean_ugm3,
      t.pm10_rolling24h_mean_ugm3,
      case
        when t.no2_hourly_sample_count_raw is null then null
        else least(32767, greatest(0, t.no2_hourly_sample_count_raw))::smallint
      end as no2_hourly_sample_count,
      case
        when t.pm25_hourly_sample_count_raw is null then null
        else least(32767, greatest(0, t.pm25_hourly_sample_count_raw))::smallint
      end as pm25_hourly_sample_count,
      case
        when t.pm10_hourly_sample_count_raw is null then null
        else least(32767, greatest(0, t.pm10_hourly_sample_count_raw))::smallint
      end as pm10_hourly_sample_count
    from target_hours t
    where
      t.no2_hourly_mean_ugm3 is not null
      or t.pm25_hourly_mean_ugm3 is not null
      or t.pm10_hourly_mean_ugm3 is not null
      or t.pm25_rolling24h_mean_ugm3 is not null
      or t.pm10_rolling24h_mean_ugm3 is not null
  ),
  changed as (
    select
      c.*
    from computed c
    left join uk_aq_aqilevels.station_aqi_hourly_helper e
      on e.station_id = c.station_id
     and e.timestamp_hour_utc = c.timestamp_hour_utc
    where
      e.station_id is null
      or (
        (
          e.no2_hourly_mean_ugm3,
          e.pm25_hourly_mean_ugm3,
          e.pm10_hourly_mean_ugm3,
          e.pm25_rolling24h_mean_ugm3,
          e.pm10_rolling24h_mean_ugm3,
          e.no2_hourly_sample_count,
          e.pm25_hourly_sample_count,
          e.pm10_hourly_sample_count
        )
        is distinct from
        (
          c.no2_hourly_mean_ugm3,
          c.pm25_hourly_mean_ugm3,
          c.pm10_hourly_mean_ugm3,
          c.pm25_rolling24h_mean_ugm3,
          c.pm10_rolling24h_mean_ugm3,
          c.no2_hourly_sample_count,
          c.pm25_hourly_sample_count,
          c.pm10_hourly_sample_count
        )
      )
  ),
  upserted as (
    insert into uk_aq_aqilevels.station_aqi_hourly_helper (
      station_id,
      timestamp_hour_utc,
      no2_hourly_mean_ugm3,
      pm25_hourly_mean_ugm3,
      pm10_hourly_mean_ugm3,
      pm25_rolling24h_mean_ugm3,
      pm10_rolling24h_mean_ugm3,
      no2_hourly_sample_count,
      pm25_hourly_sample_count,
      pm10_hourly_sample_count,
      updated_at
    )
    select
      c.station_id,
      c.timestamp_hour_utc,
      c.no2_hourly_mean_ugm3,
      c.pm25_hourly_mean_ugm3,
      c.pm10_hourly_mean_ugm3,
      c.pm25_rolling24h_mean_ugm3,
      c.pm10_rolling24h_mean_ugm3,
      c.no2_hourly_sample_count,
      c.pm25_hourly_sample_count,
      c.pm10_hourly_sample_count,
      now()
    from changed c
    on conflict (station_id, timestamp_hour_utc) do update
    set
      no2_hourly_mean_ugm3 = excluded.no2_hourly_mean_ugm3,
      pm25_hourly_mean_ugm3 = excluded.pm25_hourly_mean_ugm3,
      pm10_hourly_mean_ugm3 = excluded.pm10_hourly_mean_ugm3,
      pm25_rolling24h_mean_ugm3 = excluded.pm25_rolling24h_mean_ugm3,
      pm10_rolling24h_mean_ugm3 = excluded.pm10_rolling24h_mean_ugm3,
      no2_hourly_sample_count = excluded.no2_hourly_sample_count,
      pm25_hourly_sample_count = excluded.pm25_hourly_sample_count,
      pm10_hourly_sample_count = excluded.pm10_hourly_sample_count,
      updated_at = now()
    where
      (
        uk_aq_aqilevels.station_aqi_hourly_helper.no2_hourly_mean_ugm3,
        uk_aq_aqilevels.station_aqi_hourly_helper.pm25_hourly_mean_ugm3,
        uk_aq_aqilevels.station_aqi_hourly_helper.pm10_hourly_mean_ugm3,
        uk_aq_aqilevels.station_aqi_hourly_helper.pm25_rolling24h_mean_ugm3,
        uk_aq_aqilevels.station_aqi_hourly_helper.pm10_rolling24h_mean_ugm3,
        uk_aq_aqilevels.station_aqi_hourly_helper.no2_hourly_sample_count,
        uk_aq_aqilevels.station_aqi_hourly_helper.pm25_hourly_sample_count,
        uk_aq_aqilevels.station_aqi_hourly_helper.pm10_hourly_sample_count
      )
      is distinct from
      (
        excluded.no2_hourly_mean_ugm3,
        excluded.pm25_hourly_mean_ugm3,
        excluded.pm10_hourly_mean_ugm3,
        excluded.pm25_rolling24h_mean_ugm3,
        excluded.pm10_rolling24h_mean_ugm3,
        excluded.no2_hourly_sample_count,
        excluded.pm25_hourly_sample_count,
        excluded.pm10_hourly_sample_count
      )
    returning
      station_id,
      timestamp_hour_utc
  )
  select
    coalesce((select sc.source_rows from source_count sc), 0),
    coalesce((select count(*)::integer from upserted), 0),
    coalesce((select count(*)::integer from upserted), 0),
    (
      select max(
        greatest(
          0,
          extract(epoch from (v_reference_end - (u.timestamp_hour_utc + interval '1 hour'))) / 3600.0
        )
      )::numeric
      from upserted u
    )
  into
    v_source_rows,
    v_rows_upserted,
    v_station_hours_changed,
    v_max_changed_lag_hours;

  return query
  select
    coalesce(v_source_rows, 0),
    coalesce(v_rows_upserted, 0),
    coalesce(v_station_hours_changed, 0),
    v_max_changed_lag_hours;
end;
$$;

drop function if exists uk_aq_public.uk_aq_rpc_station_aqi_hourly_helper_window(
  timestamptz,
  timestamptz,
  bigint[]
);

create or replace function uk_aq_public.uk_aq_rpc_station_aqi_hourly_helper_window(
  p_hour_end_start_exclusive timestamptz,
  p_hour_end_end_inclusive timestamptz,
  p_station_ids bigint[] default null
)
returns table (
  station_id bigint,
  timestamp_hour_utc timestamptz,
  no2_hourly_mean_ugm3 double precision,
  pm25_hourly_mean_ugm3 double precision,
  pm10_hourly_mean_ugm3 double precision,
  pm25_rolling24h_mean_ugm3 double precision,
  pm10_rolling24h_mean_ugm3 double precision,
  no2_hourly_sample_count smallint,
  pm25_hourly_sample_count smallint,
  pm10_hourly_sample_count smallint
)
language plpgsql
security definer
set search_path = uk_aq_aqilevels, public, pg_catalog
as $$
declare
  v_start_exclusive timestamptz;
  v_end_inclusive timestamptz;
begin
  if auth.role() <> 'service_role' then
    raise exception 'service_role required';
  end if;

  if p_hour_end_start_exclusive is null or p_hour_end_end_inclusive is null then
    raise exception 'p_hour_end_start_exclusive and p_hour_end_end_inclusive are required';
  end if;

  v_start_exclusive := date_trunc('hour', p_hour_end_start_exclusive);
  v_end_inclusive := date_trunc('hour', p_hour_end_end_inclusive);
  if v_end_inclusive <= v_start_exclusive then
    raise exception 'p_hour_end_end_inclusive must be greater than p_hour_end_start_exclusive';
  end if;

  return query
  select
    h.station_id,
    h.timestamp_hour_utc,
    h.no2_hourly_mean_ugm3,
    h.pm25_hourly_mean_ugm3,
    h.pm10_hourly_mean_ugm3,
    h.pm25_rolling24h_mean_ugm3,
    h.pm10_rolling24h_mean_ugm3,
    h.no2_hourly_sample_count,
    h.pm25_hourly_sample_count,
    h.pm10_hourly_sample_count
  from uk_aq_aqilevels.station_aqi_hourly_helper h
  where h.timestamp_hour_utc > (v_start_exclusive - interval '1 hour')
    and h.timestamp_hour_utc <= (v_end_inclusive - interval '1 hour')
    and (p_station_ids is null or h.station_id = any(p_station_ids))
  order by
    h.timestamp_hour_utc,
    h.station_id;
end;
$$;

drop function if exists uk_aq_public.uk_aq_rpc_station_aqi_hourly_helper_cleanup(integer);

create or replace function uk_aq_public.uk_aq_rpc_station_aqi_hourly_helper_cleanup(
  p_retention_days integer default 45
)
returns table (
  rows_deleted bigint
)
language plpgsql
security definer
set search_path = uk_aq_aqilevels, public, pg_catalog
as $$
declare
  v_days integer;
  v_rows bigint := 0;
begin
  if auth.role() is not null and auth.role() <> 'service_role' then
    raise exception 'service_role required';
  end if;

  v_days := greatest(1, least(coalesce(p_retention_days, 45), 3650));

  delete from uk_aq_aqilevels.station_aqi_hourly_helper
  where timestamp_hour_utc < date_trunc('hour', now()) - make_interval(days => v_days);

  get diagnostics v_rows = row_count;

  return query select coalesce(v_rows, 0);
end;
$$;

revoke all on function uk_aq_public.uk_aq_rpc_station_aqi_hourly_helper_upsert(
  timestamptz,
  timestamptz,
  bigint[],
  timestamptz
) from public;

revoke all on function uk_aq_public.uk_aq_rpc_station_aqi_hourly_helper_upsert(
  timestamptz,
  timestamptz,
  bigint[],
  timestamptz
) from anon, authenticated;

grant execute on function uk_aq_public.uk_aq_rpc_station_aqi_hourly_helper_upsert(
  timestamptz,
  timestamptz,
  bigint[],
  timestamptz
) to service_role;

revoke all on function uk_aq_public.uk_aq_rpc_station_aqi_hourly_helper_window(
  timestamptz,
  timestamptz,
  bigint[]
) from public;

revoke all on function uk_aq_public.uk_aq_rpc_station_aqi_hourly_helper_window(
  timestamptz,
  timestamptz,
  bigint[]
) from anon, authenticated;

grant execute on function uk_aq_public.uk_aq_rpc_station_aqi_hourly_helper_window(
  timestamptz,
  timestamptz,
  bigint[]
) to service_role;

revoke all on function uk_aq_public.uk_aq_rpc_station_aqi_hourly_helper_cleanup(integer) from public;

revoke all on function uk_aq_public.uk_aq_rpc_station_aqi_hourly_helper_cleanup(integer) from anon, authenticated;

grant execute on function uk_aq_public.uk_aq_rpc_station_aqi_hourly_helper_cleanup(integer) to service_role;

do $$
declare
  v_fn record;
begin
  for v_fn in
    select
      p.proname,
      pg_get_function_identity_arguments(p.oid) as identity_args
    from pg_proc p
    join pg_namespace n on n.oid = p.pronamespace
    where n.nspname = 'uk_aq_public'
      and p.proname = any(array[
        'uk_aq_rpc_database_size_bytes',
        'uk_aq_rpc_db_size_metric_upsert',
        'uk_aq_rpc_db_size_metric_cleanup',
        'uk_aq_rpc_schema_size_metric_upsert',
        'uk_aq_rpc_schema_size_metric_cleanup',
        'uk_aq_rpc_r2_domain_size_metric_upsert',
        'uk_aq_rpc_r2_domain_size_metric_cleanup',
        'uk_aq_rpc_r2_backup_window'
      ])
  loop
    execute format(
      'drop function if exists uk_aq_public.%I(%s)',
      v_fn.proname,
      v_fn.identity_args
    );
  end loop;
end
$$;

create or replace function uk_aq_public.uk_aq_rpc_database_size_bytes()
returns table (
  database_name text,
  size_bytes bigint,
  oldest_observed_at timestamptz,
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
    (
      select coalesce(sum(pg_database_size(pg_database.datname)), 0)::bigint
      from pg_database
    ) as size_bytes,
    (select min(o.observed_at) from uk_aq_core.observations o) as oldest_observed_at,
    now() as sampled_at;
end;
$$;

create or replace function uk_aq_public.uk_aq_rpc_db_size_metric_upsert(
  p_database_label text,
  p_database_name text,
  p_size_bytes bigint,
  p_oldest_observed_at timestamptz default null,
  p_recorded_at timestamptz default now(),
  p_source text default null
)
returns table (rows_upserted int)
language plpgsql
security definer
set search_path = uk_aq_ops, public, pg_catalog
as $$
declare
  v_bucket_hour timestamptz;
  v_rows int := 0;
  v_source text;
begin
  if auth.role() <> 'service_role' then
    raise exception 'service_role required';
  end if;

  if p_database_label not in ('ingestdb', 'obs_aqidb') then
    raise exception 'invalid database_label: %', p_database_label;
  end if;

  if p_database_name is null or btrim(p_database_name) = '' then
    raise exception 'database_name is required';
  end if;

  if p_size_bytes is null or p_size_bytes < 0 then
    raise exception 'size_bytes must be >= 0';
  end if;

  v_bucket_hour := date_trunc('hour', coalesce(p_recorded_at, now()));
  v_source := coalesce(nullif(btrim(p_source), ''), 'uk_aq_db_size_logger_cloud_run');

  insert into uk_aq_ops.db_size_metrics_hourly (
    bucket_hour,
    database_label,
    database_name,
    size_bytes,
    oldest_observed_at,
    source,
    recorded_at,
    updated_at
  )
  values (
    v_bucket_hour,
    p_database_label,
    p_database_name,
    p_size_bytes,
    p_oldest_observed_at,
    v_source,
    coalesce(p_recorded_at, now()),
    now()
  )
  on conflict (bucket_hour, database_label) do update set
    database_name = excluded.database_name,
    size_bytes = excluded.size_bytes,
    oldest_observed_at = excluded.oldest_observed_at,
    source = excluded.source,
    recorded_at = excluded.recorded_at,
    updated_at = now();

  get diagnostics v_rows = row_count;
  return query select v_rows;
end;
$$;

create or replace function uk_aq_public.uk_aq_rpc_db_size_metric_cleanup(
  p_retention_days integer default 120
)
returns table (rows_deleted bigint)
language plpgsql
security definer
set search_path = uk_aq_ops, public, pg_catalog
as $$
declare
  v_days integer;
  v_rows bigint := 0;
begin
  if auth.role() <> 'service_role' then
    raise exception 'service_role required';
  end if;

  v_days := greatest(1, least(coalesce(p_retention_days, 120), 3650));

  delete from uk_aq_ops.db_size_metrics_hourly
  where bucket_hour < now() - make_interval(days => v_days);

  get diagnostics v_rows = row_count;
  return query select v_rows;
end;
$$;

create or replace function uk_aq_public.uk_aq_rpc_r2_domain_size_metric_upsert(
  p_domain_name text,
  p_size_bytes bigint,
  p_recorded_at timestamptz default now(),
  p_source text default null
)
returns table (rows_upserted int)
language plpgsql
security definer
set search_path = uk_aq_ops, public, pg_catalog
as $$
declare
  v_bucket_hour timestamptz;
  v_rows int := 0;
  v_source text;
begin
  if auth.role() <> 'service_role' then
    raise exception 'service_role required';
  end if;

  if p_domain_name not in ('observations', 'aqilevels') then
    raise exception 'invalid domain_name: %', p_domain_name;
  end if;

  if p_size_bytes is null or p_size_bytes < 0 then
    raise exception 'size_bytes must be >= 0';
  end if;

  v_bucket_hour := date_trunc('hour', coalesce(p_recorded_at, now()));
  v_source := coalesce(nullif(btrim(p_source), ''), 'uk_aq_db_size_logger_cloud_run');

  insert into uk_aq_ops.r2_domain_size_metrics_hourly (
    bucket_hour,
    domain_name,
    size_bytes,
    source,
    recorded_at,
    updated_at
  )
  values (
    v_bucket_hour,
    p_domain_name,
    p_size_bytes,
    v_source,
    coalesce(p_recorded_at, now()),
    now()
  )
  on conflict (bucket_hour, domain_name) do update set
    size_bytes = excluded.size_bytes,
    source = excluded.source,
    recorded_at = excluded.recorded_at,
    updated_at = now();

  get diagnostics v_rows = row_count;
  return query select v_rows;
end;
$$;

create or replace function uk_aq_public.uk_aq_rpc_r2_domain_size_metric_cleanup(
  p_retention_days integer default 120
)
returns table (rows_deleted bigint)
language plpgsql
security definer
set search_path = uk_aq_ops, public, pg_catalog
as $$
declare
  v_days integer;
  v_rows bigint := 0;
begin
  if auth.role() <> 'service_role' then
    raise exception 'service_role required';
  end if;

  v_days := greatest(1, least(coalesce(p_retention_days, 120), 3650));

  delete from uk_aq_ops.r2_domain_size_metrics_hourly
  where bucket_hour < now() - make_interval(days => v_days);

  get diagnostics v_rows = row_count;
  return query select v_rows;
end;
$$;

create or replace function uk_aq_public.uk_aq_rpc_r2_backup_window()
returns table (
  min_day_utc date,
  max_day_utc date
)
language plpgsql
security definer
set search_path = uk_aq_ops, public, pg_catalog
as $$
begin
  if auth.role() <> 'service_role' then
    raise exception 'service_role required';
  end if;

  if to_regclass('uk_aq_ops.prune_day_gates') is null then
    return query select null::date, null::date;
    return;
  end if;

  return query
  select
    min(day_utc)::date as min_day_utc,
    max(day_utc)::date as max_day_utc
  from uk_aq_ops.prune_day_gates
  where backup_done is true
    and nullif(btrim(backup_manifest_key), '') is not null
    and backup_completed_at is not null;
end;
$$;

revoke all on function uk_aq_public.uk_aq_rpc_database_size_bytes() from public;
grant execute on function uk_aq_public.uk_aq_rpc_database_size_bytes() to service_role;

revoke all on function uk_aq_public.uk_aq_rpc_db_size_metric_upsert(
  text,
  text,
  bigint,
  timestamptz,
  timestamptz,
  text
) from public;
grant execute on function uk_aq_public.uk_aq_rpc_db_size_metric_upsert(
  text,
  text,
  bigint,
  timestamptz,
  timestamptz,
  text
) to service_role;

revoke all on function uk_aq_public.uk_aq_rpc_db_size_metric_cleanup(integer) from public;
grant execute on function uk_aq_public.uk_aq_rpc_db_size_metric_cleanup(integer) to service_role;

revoke all on function uk_aq_public.uk_aq_rpc_r2_domain_size_metric_upsert(
  text,
  bigint,
  timestamptz,
  text
) from public;
grant execute on function uk_aq_public.uk_aq_rpc_r2_domain_size_metric_upsert(
  text,
  bigint,
  timestamptz,
  text
) to service_role;

revoke all on function uk_aq_public.uk_aq_rpc_r2_domain_size_metric_cleanup(integer) from public;
grant execute on function uk_aq_public.uk_aq_rpc_r2_domain_size_metric_cleanup(integer) to service_role;

revoke all on function uk_aq_public.uk_aq_rpc_r2_backup_window() from public;
grant execute on function uk_aq_public.uk_aq_rpc_r2_backup_window() to service_role;
