-- UK AQ v0.2.0 canonical core security.
-- Apply after 001_core_schema.sql.

do $$
declare
  table_name text;
begin
  foreach table_name in array array[
    'connectors',
    'networks',
    'stations',
    'station_initial_metadata',
    'station_matches',
    'observed_properties',
    'timeseries',
    'observations',
    'uk_aq_ingest_runs'
  ]
  loop
    execute format(
      'alter table uk_aq_core.%I enable row level security',
      table_name
    );

    if not exists (
      select 1
      from pg_policies
      where schemaname = 'uk_aq_core'
        and tablename = table_name
        and policyname = table_name || '_select_authenticated'
    ) then
      execute format(
        'create policy %I on uk_aq_core.%I for select '
        'using ((select auth.role()) in (''authenticated'', ''service_role''))',
        table_name || '_select_authenticated',
        table_name
      );
    end if;

    if not exists (
      select 1
      from pg_policies
      where schemaname = 'uk_aq_core'
        and tablename = table_name
        and policyname = table_name || '_write_service_role'
    ) then
      execute format(
        'create policy %I on uk_aq_core.%I for all '
        'using ((select auth.role()) = ''service_role'') '
        'with check ((select auth.role()) = ''service_role'')',
        table_name || '_write_service_role',
        table_name
      );
    end if;
  end loop;
end
$$;

grant usage on schema uk_aq_core to anon, authenticated, service_role;
grant select on all tables in schema uk_aq_core to authenticated;
grant all on all tables in schema uk_aq_core to service_role;
grant all on all sequences in schema uk_aq_core to service_role;
grant execute on all functions in schema uk_aq_core to service_role;

alter default privileges in schema uk_aq_core
  grant select on tables to authenticated;
alter default privileges in schema uk_aq_core
  grant all on tables to service_role;
alter default privileges in schema uk_aq_core
  grant all on sequences to service_role;
alter default privileges in schema uk_aq_core
  grant execute on functions to service_role;
