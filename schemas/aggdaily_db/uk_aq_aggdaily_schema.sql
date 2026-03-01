-- UK-AQ aggdaily database bootstrap.
-- Safe to rerun; focused on minimum objects required for ops DB-size logging.

create schema if not exists uk_aq_aggdaily;
create schema if not exists uk_aq_public;

create or replace function uk_aq_public.uk_aq_rpc_database_size_bytes()
returns table (
  database_name text,
  size_bytes bigint,
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
    pg_database_size(current_database())::bigint as size_bytes,
    now() as sampled_at;
end;
$$;

revoke all on function uk_aq_public.uk_aq_rpc_database_size_bytes() from public;
grant execute on function uk_aq_public.uk_aq_rpc_database_size_bytes() to service_role;

grant usage on schema uk_aq_aggdaily to service_role;
grant usage on schema uk_aq_public to service_role;

-- Note: aggregate data tables are intentionally not defined yet.
-- Add them once grain/dimensions/retention decisions are finalized.
