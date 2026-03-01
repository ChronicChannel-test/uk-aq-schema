-- History observations partition migration + status artifact removal.
--
-- Run during a maintenance window with history writes paused.
-- This migration keeps the old table as uk_aq_history.observations_old for rollback.

begin;

set local timezone = 'UTC';
set local lock_timeout = '30s';
set local statement_timeout = '0';

-- Build a detached replacement table first.
-- Note: indexes are created on the detached table before swap, so CONCURRENTLY is not required.
do $$
declare
  v_today_utc date := (now() at time zone 'UTC')::date;
  v_min_day date;
  v_max_day date;
  v_start_day date;
  v_end_day date;
  v_day date;
  v_partition_name text;
begin
  if to_regclass('uk_aq_history.observations') is null then
    raise exception 'uk_aq_history.observations not found';
  end if;

  if to_regclass('uk_aq_history.observations_old') is not null then
    raise exception 'uk_aq_history.observations_old already exists; clear previous cutover artifacts first';
  end if;

  if to_regclass('uk_aq_history.observations_new') is not null then
    raise exception 'uk_aq_history.observations_new already exists; clear previous cutover artifacts first';
  end if;

  select
    min((o.observed_at at time zone 'UTC')::date),
    max((o.observed_at at time zone 'UTC')::date)
  into v_min_day, v_max_day
  from uk_aq_history.observations o;

  v_start_day := coalesce(v_min_day, v_today_utc);
  v_end_day := greatest(coalesce(v_max_day, v_today_utc), v_today_utc + 7);

  execute $ddl$
    create table uk_aq_history.observations_new (
      connector_id integer not null,
      timeseries_id integer not null,
      observed_at timestamptz not null,
      value double precision,
      created_at timestamptz not null default now()
    ) partition by range (observed_at)
  $ddl$;

  execute $ddl$
    create table uk_aq_history.observations_new_default
      partition of uk_aq_history.observations_new default
  $ddl$;

  execute $ddl$
    create index observations_new_default_observed_at_brin_idx
      on uk_aq_history.observations_new_default using brin (observed_at)
  $ddl$;

  for v_day in
    select generate_series(v_start_day, v_end_day, interval '1 day')::date
  loop
    v_partition_name := format('observations_new_%s', to_char(v_day, 'YYYYMMDD'));

    execute format(
      'create table uk_aq_history.%I '
      'partition of uk_aq_history.observations_new '
      'for values from (%L) to (%L)',
      v_partition_name,
      format('%s 00:00:00+00', v_day),
      format('%s 00:00:00+00', v_day + 1)
    );

    execute format(
      'create index %I on uk_aq_history.%I using brin (observed_at)',
      v_partition_name || '_observed_at_brin_idx',
      v_partition_name
    );

    if v_day between (v_today_utc - 2) and v_today_utc then
      execute format(
        'create unique index %I on uk_aq_history.%I (connector_id, timeseries_id, observed_at)',
        v_partition_name || '_hot_key_uidx',
        v_partition_name
      );
    end if;
  end loop;
end $$;

insert into uk_aq_history.observations_new (
  connector_id,
  timeseries_id,
  observed_at,
  value,
  created_at
)
select
  o.connector_id::integer,
  o.timeseries_id::integer,
  o.observed_at,
  o.value,
  coalesce(o.created_at, now())
from uk_aq_history.observations o;

create temporary table _history_day_counts_old on commit drop as
select
  (o.observed_at at time zone 'UTC')::date as day_utc,
  count(*)::bigint as row_count
from uk_aq_history.observations o
group by 1;

create temporary table _history_day_counts_new on commit drop as
select
  (o.observed_at at time zone 'UTC')::date as day_utc,
  count(*)::bigint as row_count
from uk_aq_history.observations_new o
group by 1;

do $$
declare
  v_mismatch record;
begin
  select
    q.day_utc,
    q.old_count,
    q.new_count
  into v_mismatch
  from (
    select
      coalesce(o.day_utc, n.day_utc) as day_utc,
      coalesce(o.row_count, 0) as old_count,
      coalesce(n.row_count, 0) as new_count
    from _history_day_counts_old o
    full outer join _history_day_counts_new n
      on n.day_utc = o.day_utc
  ) q
  where q.old_count <> q.new_count
  order by q.day_utc
  limit 1;

  if found then
    raise exception
      'Per-day count mismatch during history cutover (day %, old %, new %)',
      v_mismatch.day_utc,
      v_mismatch.old_count,
      v_mismatch.new_count;
  end if;
end $$;

lock table uk_aq_history.observations in access exclusive mode;

alter table uk_aq_history.observations rename to observations_old;
alter table uk_aq_history.observations_new rename to observations;

-- Keep rollback table but rename its partitions so new table can use canonical names.
do $$
declare
  v_part record;
  v_new_name text;
begin
  for v_part in
    select c.relname as child_name
    from pg_inherits i
    join pg_class c on c.oid = i.inhrelid
    join pg_class p on p.oid = i.inhparent
    join pg_namespace n on n.oid = c.relnamespace
    where n.nspname = 'uk_aq_history'
      and p.relname = 'observations_old'
  loop
    v_new_name := left('observations_old_' || v_part.child_name, 63);
    execute format(
      'alter table uk_aq_history.%I rename to %I',
      v_part.child_name,
      v_new_name
    );
  end loop;

  for v_part in
    select c.relname as child_name
    from pg_inherits i
    join pg_class c on c.oid = i.inhrelid
    join pg_class p on p.oid = i.inhparent
    join pg_namespace n on n.oid = c.relnamespace
    where n.nspname = 'uk_aq_history'
      and p.relname = 'observations'
      and c.relname like 'observations_new_%'
  loop
    execute format(
      'alter table uk_aq_history.%I rename to %I',
      v_part.child_name,
      regexp_replace(v_part.child_name, '^observations_new_', 'observations_')
    );
  end loop;
end $$;

alter table uk_aq_history.observations enable row level security;

do $$
begin
  if not exists (
    select 1
    from pg_policies p
    where p.schemaname = 'uk_aq_history'
      and p.tablename = 'observations'
      and p.policyname = 'uk_aq_history_observations_service_role'
  ) then
    execute
      'create policy uk_aq_history_observations_service_role on uk_aq_history.observations '
      'for all using (auth.role() = ''service_role'') '
      'with check (auth.role() = ''service_role'')';
  end if;
end $$;

grant all on table uk_aq_history.observations to service_role;

create extension if not exists pgcrypto;

drop function if exists uk_aq_public.uk_aq_rpc_observations_hourly_fingerprint(timestamptz, timestamptz);
create or replace function uk_aq_public.uk_aq_rpc_observations_hourly_fingerprint(
  window_start timestamptz,
  window_end timestamptz
)
returns table (
  connector_id integer,
  hour_start timestamptz,
  observation_count bigint,
  fingerprint text,
  min_observed_at timestamptz,
  max_observed_at timestamptz
)
language plpgsql
security definer
set search_path = uk_aq_history, public, pg_catalog
as $$
begin
  set local timezone = 'UTC';

  if window_start is null or window_end is null then
    raise exception 'window_start and window_end are required';
  end if;

  if window_end <= window_start then
    raise exception 'window_end must be greater than window_start';
  end if;

  return query
  with row_hashes as (
    select
      o.connector_id,
      date_trunc('hour', o.observed_at) as hour_start,
      o.timeseries_id,
      o.observed_at,
      encode(
        digest(
          concat_ws(
            '|',
            o.connector_id::text,
            o.timeseries_id::text,
            to_char(o.observed_at at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
            coalesce(to_char(o.value, 'FM9999999990.999999999'), 'NULL')
          ),
          'sha256'
        ),
        'hex'
      ) as row_hash_hex
    from uk_aq_history.observations o
    where o.observed_at >= window_start
      and o.observed_at < window_end
  )
  select
    r.connector_id,
    r.hour_start,
    count(*)::bigint as observation_count,
    encode(
      digest(
        string_agg(r.row_hash_hex, '' order by r.timeseries_id, r.observed_at),
        'sha256'
      ),
      'hex'
    ) as fingerprint,
    min(r.observed_at) as min_observed_at,
    max(r.observed_at) as max_observed_at
  from row_hashes r
  group by r.connector_id, r.hour_start
  order by r.hour_start, r.connector_id;
end;
$$;

revoke execute on function uk_aq_public.uk_aq_rpc_observations_hourly_fingerprint(timestamptz, timestamptz) from public;
revoke execute on function uk_aq_public.uk_aq_rpc_observations_hourly_fingerprint(timestamptz, timestamptz) from anon, authenticated;
grant execute on function uk_aq_public.uk_aq_rpc_observations_hourly_fingerprint(timestamptz, timestamptz) to service_role;

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

  return query
  select o.*
  from uk_aq_history.observations o
  where o.observed_at >= start_utc
    and o.observed_at < end_utc
    and (rpc_observations_window.timeseries_id is null or o.timeseries_id = rpc_observations_window.timeseries_id)
    and (
      rpc_observations_window.station_id is null
      or exists (
        select 1
        from uk_aq_core.timeseries ts
        where ts.id = o.timeseries_id
          and ts.station_id = rpc_observations_window.station_id::bigint
      )
    )
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

-- Remove all FK references to uk_aq_history.status_codes before dropping it.
do $$
declare
  v_fk record;
begin
  if to_regclass('uk_aq_history.status_codes') is null then
    return;
  end if;

  for v_fk in
    select
      n.nspname as schema_name,
      c.relname as table_name,
      con.conname as constraint_name
    from pg_constraint con
    join pg_class c on c.oid = con.conrelid
    join pg_namespace n on n.oid = c.relnamespace
    where con.contype = 'f'
      and con.confrelid = 'uk_aq_history.status_codes'::regclass
  loop
    execute format(
      'alter table %I.%I drop constraint if exists %I',
      v_fk.schema_name,
      v_fk.table_name,
      v_fk.constraint_name
    );
  end loop;
end $$;

drop table if exists uk_aq_history.status_codes;

commit;
