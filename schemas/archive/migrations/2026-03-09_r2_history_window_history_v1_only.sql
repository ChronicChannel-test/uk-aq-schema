-- Restrict R2 History window to committed history/v1 manifests only.
begin;

create or replace function uk_aq_public.uk_aq_rpc_r2_history_window()
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
  where history_done is true
    and nullif(btrim(history_manifest_key), '') is not null
    and history_manifest_key ~ '^history/v1/(observations|aqilevels)/day_utc=[0-9]{4}-[0-9]{2}-[0-9]{2}/manifest\.json$'
    and history_completed_at is not null;
end;
$$;

revoke all on function uk_aq_public.uk_aq_rpc_r2_history_window() from public;
grant execute on function uk_aq_public.uk_aq_rpc_r2_history_window() to service_role;

commit;
