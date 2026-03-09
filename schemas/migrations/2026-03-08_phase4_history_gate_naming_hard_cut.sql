begin;

create schema if not exists uk_aq_ops;
create schema if not exists uk_aq_public;

do $$
begin
  if to_regclass('uk_aq_ops.history_candidates') is not null
     and to_regclass('uk_aq_ops.backup_candidates') is not null then
    raise exception 'Both uk_aq_ops.history_candidates and uk_aq_ops.backup_candidates exist; reconcile manually before hard-cut rename.';
  end if;

  if to_regclass('uk_aq_ops.history_candidates') is null
     and to_regclass('uk_aq_ops.backup_candidates') is not null then
    execute 'alter table uk_aq_ops.backup_candidates rename to history_candidates';
  end if;
end
$$;

do $$
begin
  if to_regclass('uk_aq_ops.history_candidates') is null then
    return;
  end if;

  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'uk_aq_ops'
      and table_name = 'history_candidates'
      and column_name = 'backup_row_count'
  ) and not exists (
    select 1
    from information_schema.columns
    where table_schema = 'uk_aq_ops'
      and table_name = 'history_candidates'
      and column_name = 'history_row_count'
  ) then
    execute 'alter table uk_aq_ops.history_candidates rename column backup_row_count to history_row_count';
  end if;

  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'uk_aq_ops'
      and table_name = 'history_candidates'
      and column_name = 'backup_file_count'
  ) and not exists (
    select 1
    from information_schema.columns
    where table_schema = 'uk_aq_ops'
      and table_name = 'history_candidates'
      and column_name = 'history_file_count'
  ) then
    execute 'alter table uk_aq_ops.history_candidates rename column backup_file_count to history_file_count';
  end if;

  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'uk_aq_ops'
      and table_name = 'history_candidates'
      and column_name = 'backup_total_bytes'
  ) and not exists (
    select 1
    from information_schema.columns
    where table_schema = 'uk_aq_ops'
      and table_name = 'history_candidates'
      and column_name = 'history_total_bytes'
  ) then
    execute 'alter table uk_aq_ops.history_candidates rename column backup_total_bytes to history_total_bytes';
  end if;

  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'uk_aq_ops'
      and table_name = 'history_candidates'
      and column_name = 'backup_completed_at'
  ) and not exists (
    select 1
    from information_schema.columns
    where table_schema = 'uk_aq_ops'
      and table_name = 'history_candidates'
      and column_name = 'history_completed_at'
  ) then
    execute 'alter table uk_aq_ops.history_candidates rename column backup_completed_at to history_completed_at';
  end if;
end
$$;

do $$
begin
  if to_regclass('uk_aq_ops.prune_day_gates') is null then
    return;
  end if;

  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'uk_aq_ops'
      and table_name = 'prune_day_gates'
      and column_name = 'backup_done'
  ) and not exists (
    select 1
    from information_schema.columns
    where table_schema = 'uk_aq_ops'
      and table_name = 'prune_day_gates'
      and column_name = 'history_done'
  ) then
    execute 'alter table uk_aq_ops.prune_day_gates rename column backup_done to history_done';
  end if;

  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'uk_aq_ops'
      and table_name = 'prune_day_gates'
      and column_name = 'backup_run_id'
  ) and not exists (
    select 1
    from information_schema.columns
    where table_schema = 'uk_aq_ops'
      and table_name = 'prune_day_gates'
      and column_name = 'history_run_id'
  ) then
    execute 'alter table uk_aq_ops.prune_day_gates rename column backup_run_id to history_run_id';
  end if;

  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'uk_aq_ops'
      and table_name = 'prune_day_gates'
      and column_name = 'backup_manifest_key'
  ) and not exists (
    select 1
    from information_schema.columns
    where table_schema = 'uk_aq_ops'
      and table_name = 'prune_day_gates'
      and column_name = 'history_manifest_key'
  ) then
    execute 'alter table uk_aq_ops.prune_day_gates rename column backup_manifest_key to history_manifest_key';
  end if;

  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'uk_aq_ops'
      and table_name = 'prune_day_gates'
      and column_name = 'backup_row_count'
  ) and not exists (
    select 1
    from information_schema.columns
    where table_schema = 'uk_aq_ops'
      and table_name = 'prune_day_gates'
      and column_name = 'history_row_count'
  ) then
    execute 'alter table uk_aq_ops.prune_day_gates rename column backup_row_count to history_row_count';
  end if;

  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'uk_aq_ops'
      and table_name = 'prune_day_gates'
      and column_name = 'backup_file_count'
  ) and not exists (
    select 1
    from information_schema.columns
    where table_schema = 'uk_aq_ops'
      and table_name = 'prune_day_gates'
      and column_name = 'history_file_count'
  ) then
    execute 'alter table uk_aq_ops.prune_day_gates rename column backup_file_count to history_file_count';
  end if;

  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'uk_aq_ops'
      and table_name = 'prune_day_gates'
      and column_name = 'backup_total_bytes'
  ) and not exists (
    select 1
    from information_schema.columns
    where table_schema = 'uk_aq_ops'
      and table_name = 'prune_day_gates'
      and column_name = 'history_total_bytes'
  ) then
    execute 'alter table uk_aq_ops.prune_day_gates rename column backup_total_bytes to history_total_bytes';
  end if;

  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'uk_aq_ops'
      and table_name = 'prune_day_gates'
      and column_name = 'backup_completed_at'
  ) and not exists (
    select 1
    from information_schema.columns
    where table_schema = 'uk_aq_ops'
      and table_name = 'prune_day_gates'
      and column_name = 'history_completed_at'
  ) then
    execute 'alter table uk_aq_ops.prune_day_gates rename column backup_completed_at to history_completed_at';
  end if;
end
$$;

do $$
begin
  if to_regclass('uk_aq_ops.backup_candidates_status_day_idx') is not null
     and to_regclass('uk_aq_ops.history_candidates_status_day_idx') is null then
    execute 'alter index uk_aq_ops.backup_candidates_status_day_idx rename to history_candidates_status_day_idx';
  end if;

  if to_regclass('uk_aq_ops.backup_candidates_day_status_idx') is not null
     and to_regclass('uk_aq_ops.history_candidates_day_status_idx') is null then
    execute 'alter index uk_aq_ops.backup_candidates_day_status_idx rename to history_candidates_day_status_idx';
  end if;

  if to_regclass('uk_aq_ops.prune_day_gates_backup_done_idx') is not null
     and to_regclass('uk_aq_ops.prune_day_gates_history_done_idx') is null then
    execute 'alter index uk_aq_ops.prune_day_gates_backup_done_idx rename to prune_day_gates_history_done_idx';
  end if;
end
$$;

do $$
begin
  if to_regclass('uk_aq_ops.history_candidates') is not null then
    if exists (
      select 1
      from pg_constraint
      where conrelid = 'uk_aq_ops.history_candidates'::regclass
        and conname = 'backup_candidates_status_check'
    ) and not exists (
      select 1
      from pg_constraint
      where conrelid = 'uk_aq_ops.history_candidates'::regclass
        and conname = 'history_candidates_status_check'
    ) then
      execute 'alter table uk_aq_ops.history_candidates rename constraint backup_candidates_status_check to history_candidates_status_check';
    end if;

    if exists (
      select 1
      from pg_constraint
      where conrelid = 'uk_aq_ops.history_candidates'::regclass
        and conname = 'backup_candidates_resume_nonnegative_check'
    ) and not exists (
      select 1
      from pg_constraint
      where conrelid = 'uk_aq_ops.history_candidates'::regclass
        and conname = 'history_candidates_resume_nonnegative_check'
    ) then
      execute 'alter table uk_aq_ops.history_candidates rename constraint backup_candidates_resume_nonnegative_check to history_candidates_resume_nonnegative_check';
    end if;
  end if;
end
$$;

do $$
begin
  if exists (
    select 1
    from pg_trigger
    where tgname = 'backup_candidates_touch_updated_at'
      and tgrelid = 'uk_aq_ops.history_candidates'::regclass
  ) and not exists (
    select 1
    from pg_trigger
    where tgname = 'history_candidates_touch_updated_at'
      and tgrelid = 'uk_aq_ops.history_candidates'::regclass
  ) then
    execute 'alter trigger backup_candidates_touch_updated_at on uk_aq_ops.history_candidates rename to history_candidates_touch_updated_at';
  end if;
end
$$;

do $$
begin
  if to_regprocedure('uk_aq_ops.uk_aq_phase_b_history_rows(integer,timestamptz,timestamptz,integer,timestamptz)') is null
     and to_regprocedure('uk_aq_ops.uk_aq_phase_b_backup_rows(integer,timestamptz,timestamptz,integer,timestamptz)') is not null then
    execute 'alter function uk_aq_ops.uk_aq_phase_b_backup_rows(integer,timestamptz,timestamptz,integer,timestamptz) rename to uk_aq_phase_b_history_rows';
  end if;
end
$$;

do $$
begin
  if to_regprocedure('uk_aq_public.uk_aq_rpc_r2_history_window()') is null
     and to_regprocedure('uk_aq_public.uk_aq_rpc_r2_backup_window()') is not null then
    execute 'alter function uk_aq_public.uk_aq_rpc_r2_backup_window() rename to uk_aq_rpc_r2_history_window';
  end if;

  if to_regprocedure('uk_aq_public.uk_aq_rpc_r2_backup_window()') is not null
     and to_regprocedure('uk_aq_public.uk_aq_rpc_r2_history_window()') is not null then
    execute 'drop function uk_aq_public.uk_aq_rpc_r2_backup_window()';
  end if;
end
$$;

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
    and history_completed_at is not null;
end;
$$;

revoke all on function uk_aq_public.uk_aq_rpc_r2_history_window() from public;
grant execute on function uk_aq_public.uk_aq_rpc_r2_history_window() to service_role;

commit;
