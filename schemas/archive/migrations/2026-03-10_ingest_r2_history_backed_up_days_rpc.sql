-- Expose committed history-backed day lookup through uk_aq_public so callers
-- do not require direct uk_aq_ops schema exposure in PostgREST.

create or replace function uk_aq_public.uk_aq_rpc_r2_history_backed_up_days(
  p_from_day_utc date,
  p_to_day_utc date
)
returns table (
  day_utc date
)
language plpgsql
security definer
set search_path = uk_aq_ops, public, pg_catalog
as $$
begin
  if auth.role() <> 'service_role' then
    raise exception 'service_role required';
  end if;

  if p_from_day_utc is null or p_to_day_utc is null then
    raise exception 'p_from_day_utc and p_to_day_utc are required';
  end if;

  if p_to_day_utc < p_from_day_utc then
    raise exception 'p_to_day_utc must be >= p_from_day_utc';
  end if;

  if to_regclass('uk_aq_ops.prune_day_gates') is null then
    return;
  end if;

  return query
  select
    g.day_utc::date as day_utc
  from uk_aq_ops.prune_day_gates g
  where g.day_utc >= p_from_day_utc
    and g.day_utc <= p_to_day_utc
    and g.history_done is true
    and nullif(btrim(g.history_manifest_key), '') is not null
    and g.history_manifest_key ~ '^history/v1/(observations|aqilevels)/day_utc=[0-9]{4}-[0-9]{2}-[0-9]{2}/manifest\.json$'
    and g.history_completed_at is not null
  order by g.day_utc asc;
end;
$$;

revoke all on function uk_aq_public.uk_aq_rpc_r2_history_backed_up_days(date, date) from public;
grant execute on function uk_aq_public.uk_aq_rpc_r2_history_backed_up_days(date, date) to service_role;
