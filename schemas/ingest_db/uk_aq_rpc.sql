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

-- rpc_observations_window for explicit UTC history windows (website/cache use).

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
returns setof uk_aq_history.observations
language plpgsql
security invoker
set search_path = uk_aq_history, uk_aq_core, public, pg_catalog
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
    from uk_aq_history.observations o
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
    from uk_aq_history.observations o
    where o.observed_at >= start_utc
      and o.observed_at < end_utc
      and o.timeseries_id = rpc_observations_window.timeseries_id
    order by o.observed_at asc;
    return;
  end if;

  if station_id is not null then
    return query
    select o.*
    from uk_aq_history.observations o
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
  from uk_aq_history.observations o
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

drop function if exists uk_aq_aggdaily.uk_aq_aqi_index_lookup(
  text,
  text,
  text,
  double precision,
  date
);

create or replace function uk_aq_aggdaily.uk_aq_aqi_index_lookup(
  p_standard_code text,
  p_pollutant_code text,
  p_averaging_code text,
  p_value double precision,
  p_effective_date date default ((now() at time zone 'UTC')::date)
)
returns table (
  index_level smallint,
  index_band text
)
language sql
stable
set search_path = uk_aq_aggdaily, public, pg_catalog
as $$
  select
    b.index_level,
    b.index_band
  from uk_aq_aggdaily.aqi_breakpoints b
  join uk_aq_aggdaily.aqi_standard_versions v
    on v.standard_code = b.standard_code
   and v.version_code = b.version_code
  where p_value is not null
    and b.standard_code = p_standard_code
    and b.pollutant_code = p_pollutant_code
    and b.averaging_code = p_averaging_code
    and (v.valid_from is null or v.valid_from <= p_effective_date)
    and (v.valid_to is null or v.valid_to >= p_effective_date)
    and (b.valid_from is null or b.valid_from <= p_effective_date)
    and (b.valid_to is null or b.valid_to >= p_effective_date)
    and p_value >= b.range_low
    and (b.range_high is null or p_value <= b.range_high)
  order by b.index_level
  limit 1;
$$;

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
set search_path = uk_aq_aggdaily, uk_aq_core, uk_aq_public, public, pg_catalog
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
    left join uk_aq_aggdaily.station_aqi_hourly_helper e
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
    insert into uk_aq_aggdaily.station_aqi_hourly_helper (
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
        uk_aq_aggdaily.station_aqi_hourly_helper.no2_hourly_mean_ugm3,
        uk_aq_aggdaily.station_aqi_hourly_helper.pm25_hourly_mean_ugm3,
        uk_aq_aggdaily.station_aqi_hourly_helper.pm10_hourly_mean_ugm3,
        uk_aq_aggdaily.station_aqi_hourly_helper.pm25_rolling24h_mean_ugm3,
        uk_aq_aggdaily.station_aqi_hourly_helper.pm10_rolling24h_mean_ugm3,
        uk_aq_aggdaily.station_aqi_hourly_helper.no2_hourly_sample_count,
        uk_aq_aggdaily.station_aqi_hourly_helper.pm25_hourly_sample_count,
        uk_aq_aggdaily.station_aqi_hourly_helper.pm10_hourly_sample_count
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
set search_path = uk_aq_aggdaily, public, pg_catalog
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
  from uk_aq_aggdaily.station_aqi_hourly_helper h
  where h.timestamp_hour_utc > (v_start_exclusive - interval '1 hour')
    and h.timestamp_hour_utc <= (v_end_inclusive - interval '1 hour')
    and (p_station_ids is null or h.station_id = any(p_station_ids))
  order by
    h.timestamp_hour_utc,
    h.station_id;
end;
$$;

drop function if exists uk_aq_ops.uk_aq_station_aqi_hourly_ingest_tick(
  timestamptz,
  bigint[],
  integer
);

drop function if exists uk_aq_public.uk_aq_rpc_station_aqi_hourly_helper_cleanup(integer);

create or replace function uk_aq_public.uk_aq_rpc_station_aqi_hourly_helper_cleanup(
  p_retention_days integer default 45
)
returns table (
  rows_deleted bigint
)
language plpgsql
security definer
set search_path = uk_aq_aggdaily, public, pg_catalog
as $$
declare
  v_days integer;
  v_rows bigint := 0;
begin
  if auth.role() is not null and auth.role() <> 'service_role' then
    raise exception 'service_role required';
  end if;

  v_days := greatest(1, least(coalesce(p_retention_days, 45), 3650));

  delete from uk_aq_aggdaily.station_aqi_hourly_helper
  where timestamp_hour_utc < date_trunc('hour', now()) - make_interval(days => v_days);

  get diagnostics v_rows = row_count;

  return query select coalesce(v_rows, 0);
end;
$$;

drop function if exists uk_aq_ops.uk_aq_station_aqi_hourly_ingest_tick(
  timestamptz,
  bigint[]
);

create or replace function uk_aq_ops.uk_aq_station_aqi_hourly_ingest_tick(
  p_now_utc timestamptz default now(),
  p_station_ids bigint[] default null,
  p_helper_retention_days integer default 45
)
returns table (
  target_hour_end_utc timestamptz,
  source_rows integer,
  rows_upserted integer,
  station_hours_changed integer,
  max_changed_lag_hours numeric,
  helper_rows_deleted bigint
)
language plpgsql
security definer
set search_path = uk_aq_ops, uk_aq_public, public, pg_catalog
as $$
declare
  v_target_hour_end_utc timestamptz;
  v_source_rows integer := 0;
  v_rows_upserted integer := 0;
  v_station_hours_changed integer := 0;
  v_max_changed_lag_hours numeric := null;
  v_helper_rows_deleted bigint := 0;
begin
  v_target_hour_end_utc := date_trunc(
    'hour',
    coalesce(p_now_utc, now()) - interval '3 hours 10 minutes'
  );

  select
    r.source_rows,
    r.rows_upserted,
    r.station_hours_changed,
    r.max_changed_lag_hours
  into
    v_source_rows,
    v_rows_upserted,
    v_station_hours_changed,
    v_max_changed_lag_hours
  from uk_aq_public.uk_aq_rpc_station_aqi_hourly_helper_upsert(
    v_target_hour_end_utc - interval '1 hour',
    v_target_hour_end_utc,
    p_station_ids,
    v_target_hour_end_utc
  ) r;

  select
    c.rows_deleted
  into v_helper_rows_deleted
  from uk_aq_public.uk_aq_rpc_station_aqi_hourly_helper_cleanup(
    p_helper_retention_days
  ) c;

  return query
  select
    v_target_hour_end_utc,
    coalesce(v_source_rows, 0),
    coalesce(v_rows_upserted, 0),
    coalesce(v_station_hours_changed, 0),
    v_max_changed_lag_hours,
    coalesce(v_helper_rows_deleted, 0);
end;
$$;

create extension if not exists pg_cron with schema extensions;

select cron.unschedule(jobid)
from cron.job
where jobname = 'uk_aq_ingest_station_aqi_hourly_helper_tick';

select cron.schedule(
  'uk_aq_ingest_station_aqi_hourly_helper_tick',
  '10 * * * *',
  $$select * from uk_aq_ops.uk_aq_station_aqi_hourly_ingest_tick();$$
);

revoke all on function uk_aq_aggdaily.uk_aq_aqi_index_lookup(
  text,
  text,
  text,
  double precision,
  date
) from public;

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

revoke all on function uk_aq_ops.uk_aq_station_aqi_hourly_ingest_tick(
  timestamptz,
  bigint[],
  integer
) from public;

grant execute on function uk_aq_ops.uk_aq_station_aqi_hourly_ingest_tick(
  timestamptz,
  bigint[],
  integer
) to service_role;
