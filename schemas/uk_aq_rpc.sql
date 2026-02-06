-- uk_aq_la_hex RPC for read-only access (Edge function backing).

create or replace function uk_aq_public.uk_aq_la_hex_rpc(
  region text[] default null,
  la_version text default null,
  limit_rows int default 1000
)
returns table (
  la_code text,
  la_name text,
  la_version text,
  station_count int,
  single_site boolean,
  median_value numeric,
  mean_value numeric,
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
      least(10000, greatest(1, coalesce(limit_rows, 1000)))::int as limit_rows
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
  limit (select limit_rows from params);
$$;

grant execute on function uk_aq_public.uk_aq_la_hex_rpc(
  text[],
  text,
  int
) to anon, authenticated;

grant execute on function uk_aq_public.uk_aq_la_hex_rpc(
  text[],
  text,
  int
) to service_role;
-- uk_aq_latest RPC for read-only access (Edge function backing).

create or replace function uk_aq_public.uk_aq_latest_rpc(
  region text default null,
  pcon_code text default null,
  station_like text default null,
  connector_id bigint default null,
  pollutant text default null,
  limit_rows int default 1000,
  window_label text default null
)
returns table (
  id bigint,
  timeseries_ref text,
  label text,
  uom text,
  last_value numeric,
  last_value_at timestamptz,
  connector_id bigint,
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
          'notation', p.notation,
          'eionet_uri', p.eionet_uri,
          'pollutant_label', p.pollutant_label
        )
      end as phenomenon,
      s.label as station_label
    from uk_aq_core.timeseries ts
    left join uk_aq_core.connectors c on c.id = ts.connector_id
    left join uk_aq_core.stations s on s.id = ts.station_id
    left join uk_aq_core.phenomena p on p.id = ts.phenomenon_id
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
          where p.notation ilike token
             or p.pollutant_label ilike token
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

grant execute on function uk_aq_public.uk_aq_latest_rpc(
  text,
  text,
  text,
  bigint,
  text,
  int,
  text
) to anon, authenticated;

grant execute on function uk_aq_public.uk_aq_latest_rpc(
  text,
  text,
  text,
  bigint,
  text,
  int,
  text
) to service_role;

-- uk_aq_timeseries RPC for read-only access (Edge function backing).

create or replace function uk_aq_public.uk_aq_timeseries_rpc(
  timeseries_id bigint,
  window_label text default '24h',
  limit_rows int default null
)
returns table (
  timeseries_id bigint,
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
      $1::bigint as timeseries_id,
      case
        when lower(nullif(trim(window_label), '')) in ('12h','24h','7d','30d')
          then lower(nullif(trim(window_label), ''))
        else '24h'
      end as window_label,
      case
        when limit_rows is null then null
        else greatest(1, limit_rows)::int
      end as limit_rows
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
      limit_rows
    from params
  ),
  phen as (
    select
      p.pollutant_label,
      p.notation,
      p.label
    from uk_aq_core.timeseries ts
    left join uk_aq_core.phenomena p on p.id = ts.phenomenon_id
    join windowed w on w.timeseries_id = ts.id
    limit 1
  ),
  pollutant as (
    select
      case
        when lower(coalesce(pollutant_label, notation, label, '')) like '%pm2.5%'
          or lower(coalesce(pollutant_label, notation, label, '')) like '%pm2_5%'
          or lower(coalesce(pollutant_label, notation, label, '')) like '%pm25%'
          then 'PM2.5'
        when lower(coalesce(pollutant_label, notation, label, '')) like '%pm10%'
          then 'PM10'
        when lower(coalesce(pollutant_label, notation, label, '')) like '%no2%'
          or lower(coalesce(pollutant_label, notation, label, '')) like '%nitrogen dioxide%'
          then 'NO2'
        when lower(coalesce(pollutant_label, notation, label, '')) like '%o3%'
          or lower(coalesce(pollutant_label, notation, label, '')) like '%ozone%'
          then 'O3'
        when lower(coalesce(pollutant_label, notation, label, '')) like '%so2%'
          or lower(coalesce(pollutant_label, notation, label, '')) like '%sulphur dioxide%'
          or lower(coalesce(pollutant_label, notation, label, '')) like '%sulfur dioxide%'
          then 'SO2'
        else nullif(
          upper(regexp_replace(coalesce(pollutant_label, notation, label, ''), '\s+', '', 'g')),
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
        jsonb_build_object(
          'observed_at', observed_at,
          'value', value,
          'status', status
        )
        order by observed_at
      ) from obs),
      '[]'::jsonb
    ) as data
  from windowed w;
$$;

grant execute on function uk_aq_public.uk_aq_timeseries_rpc(
  bigint,
  text,
  int
) to anon, authenticated;

grant execute on function uk_aq_public.uk_aq_timeseries_rpc(
  bigint,
  text,
  int
) to service_role;

-- uk_aq_pcon_hex RPC for read-only access (Edge function backing).

create or replace function uk_aq_public.uk_aq_pcon_hex_rpc(
  pcon_version text default null,
  limit_rows int default 1000
)
returns table (
  pcon_code text,
  pcon_name text,
  pcon_version text,
  station_count int,
  single_site boolean,
  median_value numeric,
  mean_value numeric,
  latest_value_at timestamptz
)
language sql
security definer
set search_path = uk_aq_core, public, pg_catalog
as $$
  with params as (
    select
      nullif(trim(pcon_version), '') as pcon_version,
      least(10000, greatest(1, coalesce(limit_rows, 1000)))::int as limit_rows
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
  limit (select limit_rows from params);
$$;

grant execute on function uk_aq_public.uk_aq_pcon_hex_rpc(
  text,
  int
) to anon, authenticated;

grant execute on function uk_aq_public.uk_aq_pcon_hex_rpc(
  text,
  int
) to service_role;

-- uk_aq_stations RPC for read-only access (Edge function backing).

create or replace function uk_aq_public.uk_aq_stations_rpc(
  connector_id bigint default null,
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
  bigint,
  text,
  text,
  int,
  int
) to anon, authenticated;

grant execute on function uk_aq_public.uk_aq_stations_rpc(
  bigint,
  text,
  text,
  int,
  int
) to service_role;

-- uk_aq_surbiton_latest RPC for read-only access (Edge function backing).

create or replace function uk_aq_public.uk_aq_surbiton_latest_rpc(
  region text default null,
  station_like text default null,
  connector_id text default null,
  pollutant text default null,
  limit_rows int default 1000
)
returns table (
  id bigint,
  timeseries_ref text,
  label text,
  uom text,
  last_value numeric,
  last_value_at timestamptz,
  connector_id bigint,
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
      nullif(trim(connector_id), '')::bigint as connector_id,
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
          'notation', p.notation,
          'eionet_uri', p.eionet_uri,
          'pollutant_label', p.pollutant_label
        )
      end as phenomenon,
      s.label as station_label
    from uk_aq_core.timeseries ts
    left join uk_aq_core.connectors c on c.id = ts.connector_id
    left join uk_aq_core.stations s on s.id = ts.station_id
    left join uk_aq_core.phenomena p on p.id = ts.phenomenon_id
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
          where p.notation ilike token
             or p.pollutant_label ilike token
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
