-- Phase 3 hard-cut migration.
-- Apply target: ingestdb and obs_aqidb.
-- Purpose:
--   1) Rename legacy schemas to final names.
--   2) Remove legacy run-mode/database-label values used by runtime tables.
--   3) Rename legacy backfill metric columns to final names.

begin;

do $$
begin
  if to_regnamespace('uk_aq_history') is not null
     and to_regnamespace('uk_aq_observs') is null then
    execute 'alter schema uk_aq_history rename to uk_aq_observs';
  end if;

  if to_regnamespace('uk_aq_aggdaily') is not null
     and to_regnamespace('uk_aq_aqilevels') is null then
    execute 'alter schema uk_aq_aggdaily rename to uk_aq_aqilevels';
  end if;
end
$$;

do $$
declare
  v_constraint_name text;
begin
  if to_regclass('uk_aq_ops.db_size_metrics_hourly') is not null then
    execute $sql$
      with consolidated as (
        select
          m.bucket_hour,
          coalesce(
            max(m.database_name) filter (where m.database_label = 'obs_aqidb'),
            max(m.database_name) filter (where m.database_label in ('historydb', 'aggdailydb')),
            current_database()::text
          ) as database_name,
          greatest(
            coalesce(max(m.size_bytes) filter (where m.database_label = 'obs_aqidb'), 0),
            coalesce(sum(m.size_bytes) filter (where m.database_label in ('historydb', 'aggdailydb')), 0)
          )::bigint as size_bytes,
          min(m.oldest_observed_at) filter (
            where m.database_label in ('obs_aqidb', 'historydb', 'aggdailydb')
          ) as oldest_observed_at,
          coalesce(
            max(m.source) filter (where m.database_label = 'obs_aqidb'),
            max(m.source) filter (where m.database_label in ('historydb', 'aggdailydb')),
            'uk_aq_db_size_logger_cloud_run'
          ) as source,
          max(m.recorded_at) as recorded_at
        from uk_aq_ops.db_size_metrics_hourly m
        where m.database_label in ('obs_aqidb', 'historydb', 'aggdailydb')
        group by m.bucket_hour
      )
      insert into uk_aq_ops.db_size_metrics_hourly (
        bucket_hour,
        database_label,
        database_name,
        size_bytes,
        oldest_observed_at,
        source,
        recorded_at,
        updated_at
      )
      select
        c.bucket_hour,
        'obs_aqidb',
        c.database_name,
        c.size_bytes,
        c.oldest_observed_at,
        c.source,
        c.recorded_at,
        now()
      from consolidated c
      on conflict (bucket_hour, database_label) do update set
        database_name = excluded.database_name,
        size_bytes = excluded.size_bytes,
        oldest_observed_at = excluded.oldest_observed_at,
        source = excluded.source,
        recorded_at = excluded.recorded_at,
        updated_at = now()
    $sql$;

    execute $sql$
      delete from uk_aq_ops.db_size_metrics_hourly
      where database_label in ('historydb', 'aggdailydb')
    $sql$;

    for v_constraint_name in
      select con.conname
      from pg_constraint con
      join pg_class rel on rel.oid = con.conrelid
      join pg_namespace nsp on nsp.oid = rel.relnamespace
      where nsp.nspname = 'uk_aq_ops'
        and rel.relname = 'db_size_metrics_hourly'
        and con.contype = 'c'
        and pg_get_constraintdef(con.oid) ilike '%database_label%'
    loop
      execute format(
        'alter table uk_aq_ops.db_size_metrics_hourly drop constraint if exists %I',
        v_constraint_name
      );
    end loop;

    if not exists (
      select 1
      from pg_constraint con
      join pg_class rel on rel.oid = con.conrelid
      join pg_namespace nsp on nsp.oid = rel.relnamespace
      where nsp.nspname = 'uk_aq_ops'
        and rel.relname = 'db_size_metrics_hourly'
        and con.conname = 'db_size_metrics_hourly_database_label_check'
    ) then
      execute $sql$
        alter table uk_aq_ops.db_size_metrics_hourly
        add constraint db_size_metrics_hourly_database_label_check
        check (database_label in ('ingestdb', 'obs_aqidb')) not valid
      $sql$;
    end if;
  end if;
end
$$;

do $$
begin
  if to_regclass('uk_aq_ops.backfill_runs') is not null then
    if exists (
      select 1
      from information_schema.columns
      where table_schema = 'uk_aq_ops'
        and table_name = 'backfill_runs'
        and column_name = 'rows_written_aggdaily'
    ) and not exists (
      select 1
      from information_schema.columns
      where table_schema = 'uk_aq_ops'
        and table_name = 'backfill_runs'
        and column_name = 'rows_written_aqilevels'
    ) then
      execute 'alter table uk_aq_ops.backfill_runs rename column rows_written_aggdaily to rows_written_aqilevels';
    end if;

    execute $sql$
      update uk_aq_ops.backfill_runs
      set run_mode = 'local_to_aqilevels'
      where run_mode = 'local_to_aggdaily'
    $sql$;

    execute 'alter table uk_aq_ops.backfill_runs drop constraint if exists backfill_runs_run_mode_check';
    execute $sql$
      alter table uk_aq_ops.backfill_runs
      add constraint backfill_runs_run_mode_check
      check (run_mode in ('local_to_aqilevels', 'obs_aqi_to_r2', 'source_to_all')) not valid
    $sql$;
  end if;

  if to_regclass('uk_aq_ops.backfill_run_days') is not null then
    if exists (
      select 1
      from information_schema.columns
      where table_schema = 'uk_aq_ops'
        and table_name = 'backfill_run_days'
        and column_name = 'rows_written_aggdaily'
    ) and not exists (
      select 1
      from information_schema.columns
      where table_schema = 'uk_aq_ops'
        and table_name = 'backfill_run_days'
        and column_name = 'rows_written_aqilevels'
    ) then
      execute 'alter table uk_aq_ops.backfill_run_days rename column rows_written_aggdaily to rows_written_aqilevels';
    end if;

    execute $sql$
      update uk_aq_ops.backfill_run_days
      set run_mode = 'local_to_aqilevels'
      where run_mode = 'local_to_aggdaily'
    $sql$;

    execute 'alter table uk_aq_ops.backfill_run_days drop constraint if exists backfill_run_days_run_mode_check';
    execute $sql$
      alter table uk_aq_ops.backfill_run_days
      add constraint backfill_run_days_run_mode_check
      check (run_mode in ('local_to_aqilevels', 'obs_aqi_to_r2', 'source_to_all')) not valid
    $sql$;
  end if;

  if to_regclass('uk_aq_ops.backfill_checkpoints') is not null then
    if exists (
      select 1
      from information_schema.columns
      where table_schema = 'uk_aq_ops'
        and table_name = 'backfill_checkpoints'
        and column_name = 'rows_written_aggdaily'
    ) and not exists (
      select 1
      from information_schema.columns
      where table_schema = 'uk_aq_ops'
        and table_name = 'backfill_checkpoints'
        and column_name = 'rows_written_aqilevels'
    ) then
      execute 'alter table uk_aq_ops.backfill_checkpoints rename column rows_written_aggdaily to rows_written_aqilevels';
    end if;

    execute $sql$
      update uk_aq_ops.backfill_checkpoints
      set run_mode = 'local_to_aqilevels'
      where run_mode = 'local_to_aggdaily'
    $sql$;

    execute 'alter table uk_aq_ops.backfill_checkpoints drop constraint if exists backfill_checkpoints_run_mode_check';
    execute $sql$
      alter table uk_aq_ops.backfill_checkpoints
      add constraint backfill_checkpoints_run_mode_check
      check (run_mode in ('local_to_aqilevels', 'obs_aqi_to_r2', 'source_to_all')) not valid
    $sql$;
  end if;

  if to_regclass('uk_aq_ops.backfill_errors') is not null then
    execute $sql$
      update uk_aq_ops.backfill_errors
      set run_mode = 'local_to_aqilevels'
      where run_mode = 'local_to_aggdaily'
    $sql$;

    execute 'alter table uk_aq_ops.backfill_errors drop constraint if exists backfill_errors_run_mode_check';
    execute $sql$
      alter table uk_aq_ops.backfill_errors
      add constraint backfill_errors_run_mode_check
      check (run_mode in ('local_to_aqilevels', 'obs_aqi_to_r2', 'source_to_all')) not valid
    $sql$;
  end if;
end
$$;

do $$
begin
  if to_regnamespace('uk_aq_observs') is not null then
    execute 'grant usage on schema uk_aq_observs to service_role';
  end if;
  if to_regnamespace('uk_aq_aqilevels') is not null then
    execute 'grant usage on schema uk_aq_aqilevels to service_role';
  end if;
end
$$;

commit;
