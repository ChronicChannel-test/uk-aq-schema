-- Expose backup-window day range for dashboard heading from prune-day gates.

drop function if exists uk_aq_public.uk_aq_rpc_r2_backup_window();

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

revoke all on function uk_aq_public.uk_aq_rpc_r2_backup_window() from public;
grant execute on function uk_aq_public.uk_aq_rpc_r2_backup_window() to service_role;
