begin;

alter table uk_aq_ops.aqi_compute_runs
  drop constraint if exists aqi_compute_runs_run_mode_check;

alter table uk_aq_ops.aqi_compute_runs
  add constraint aqi_compute_runs_run_mode_check
  check (
    run_mode in (
      'sync_hourly',
      'backfill',
      'fast',
      'reconcile_short',
      'reconcile_deep',
      'reconcile_deep_rolling'
    )
  )
  not valid;

alter table uk_aq_ops.aqi_compute_runs
  validate constraint aqi_compute_runs_run_mode_check;

create or replace function uk_aq_public.uk_aq_rpc_aqi_compute_run_log(
  p_run_mode text,
  p_trigger_mode text,
  p_window_start_utc timestamptz,
  p_window_end_utc timestamptz,
  p_source_rows integer,
  p_candidate_station_hours integer,
  p_rows_upserted integer,
  p_rows_changed integer,
  p_station_hours_changed integer,
  p_station_hours_changed_gt_36h integer,
  p_max_changed_lag_hours numeric,
  p_deep_reconcile_effective boolean,
  p_daily_rows_upserted integer,
  p_monthly_rows_upserted integer,
  p_run_status text,
  p_error_message text default null,
  p_duration_ms integer default null
)
returns table (
  run_id uuid
)
language plpgsql
security definer
set search_path = uk_aq_ops, public, pg_catalog
as $$
declare
  v_run_id uuid;
begin
  if auth.role() <> 'service_role' then
    raise exception 'service_role required';
  end if;

  if coalesce(nullif(trim(p_run_mode), ''), '') not in (
    'sync_hourly',
    'backfill',
    'fast',
    'reconcile_short',
    'reconcile_deep',
    'reconcile_deep_rolling'
  ) then
    raise exception 'invalid run_mode: %', p_run_mode;
  end if;

  if coalesce(nullif(trim(p_run_status), ''), '') not in ('ok', 'error') then
    raise exception 'invalid run_status: %', p_run_status;
  end if;

  insert into uk_aq_ops.aqi_compute_runs (
    run_mode,
    trigger_mode,
    window_start_utc,
    window_end_utc,
    source_rows,
    candidate_station_hours,
    rows_upserted,
    rows_changed,
    station_hours_changed,
    station_hours_changed_gt_36h,
    max_changed_lag_hours,
    deep_reconcile_effective,
    daily_rows_upserted,
    monthly_rows_upserted,
    run_status,
    error_message,
    duration_ms
  )
  values (
    trim(p_run_mode),
    coalesce(nullif(trim(p_trigger_mode), ''), 'manual'),
    p_window_start_utc,
    p_window_end_utc,
    greatest(0, coalesce(p_source_rows, 0)),
    greatest(0, coalesce(p_candidate_station_hours, 0)),
    greatest(0, coalesce(p_rows_upserted, 0)),
    greatest(0, coalesce(p_rows_changed, 0)),
    greatest(0, coalesce(p_station_hours_changed, 0)),
    greatest(0, coalesce(p_station_hours_changed_gt_36h, 0)),
    p_max_changed_lag_hours,
    p_deep_reconcile_effective,
    greatest(0, coalesce(p_daily_rows_upserted, 0)),
    greatest(0, coalesce(p_monthly_rows_upserted, 0)),
    trim(p_run_status),
    nullif(trim(coalesce(p_error_message, '')), ''),
    case
      when p_duration_ms is null then null
      else greatest(0, p_duration_ms)
    end
  )
  returning id into v_run_id;

  return query select v_run_id;
end;
$$;

revoke all on function uk_aq_public.uk_aq_rpc_aqi_compute_run_log(
  text,
  text,
  timestamptz,
  timestamptz,
  integer,
  integer,
  integer,
  integer,
  integer,
  integer,
  numeric,
  boolean,
  integer,
  integer,
  text,
  text,
  integer
) from public;

grant execute on function uk_aq_public.uk_aq_rpc_aqi_compute_run_log(
  text,
  text,
  timestamptz,
  timestamptz,
  integer,
  integer,
  integer,
  integer,
  integer,
  integer,
  numeric,
  boolean,
  integer,
  integer,
  text,
  text,
  integer
) to service_role;

commit;
