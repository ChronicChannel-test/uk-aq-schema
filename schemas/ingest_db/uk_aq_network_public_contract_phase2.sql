-- UK AQ website/API v2.0.0 hard cut: Phase 2 public RPC contract.
--
-- Run with psql from any directory. This driver deliberately applies the
-- complete canonical RPC file so unchanged RPCs and the four hard-cut public
-- RPCs cannot drift between clean-schema and targeted applies.
begin;

\ir uk_aq_rpc.sql

do $$
declare
  v_count integer;
begin
  select count(*)
  into v_count
  from pg_proc p
  join pg_namespace n on n.oid = p.pronamespace
  where n.nspname = 'uk_aq_public'
    and p.proname in (
      'uk_aq_latest_rpc',
      'uk_aq_stations_rpc',
      'uk_aq_pcon_hex_rpc',
      'uk_aq_la_hex_rpc'
    );

  if v_count <> 4 then
    raise exception 'Expected exactly four hard-cut public RPC signatures; found %', v_count;
  end if;

  if exists (
    select 1
    from pg_proc p
    join pg_namespace n on n.oid = p.pronamespace
    where n.nspname = 'uk_aq_public'
      and p.proname in (
        'uk_aq_latest_rpc',
        'uk_aq_stations_rpc',
        'uk_aq_pcon_hex_rpc',
        'uk_aq_la_hex_rpc'
      )
      and (
        pg_get_function_identity_arguments(p.oid) not like '%network_code text%'
        or pg_get_function_identity_arguments(p.oid) like '%connector_id%'
        or pg_get_functiondef(p.oid) not like '%public_display_enabled = true%'
        or pg_get_function_result(p.oid) like '%network_type%'
      )
  ) then
    raise exception 'A hard-cut public RPC does not satisfy the Phase 2 contract';
  end if;
end
$$;

notify pgrst, 'reload schema';

commit;
