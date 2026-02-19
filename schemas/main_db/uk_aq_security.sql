-- RLS policies for uk_aq_core, uk_aq_raw, uk_aq_pop
do $$
declare
  t text;
begin
  for t in
    select unnest(ARRAY['connectors','categories','phenomena','offerings','features','procedures','stations','station_metadata','station_network_memberships','uk_aq_networks','uk_air_sos_networks','uk_air_sos_network_pollutants','uk_aq_guidelines','timeseries','reference_values','observations','uk_aq_ingest_runs','dispatcher_settings','pcon_current','pcon_legacy','gss_codes','uk_aq_region_names','pollutant_thresholds']::text[])
  loop
    execute format('alter table uk_aq_core.%I enable row level security', t);
    if not exists (
      select 1 from pg_policies p
      where p.schemaname = 'uk_aq_core' and p.tablename = t and p.policyname = t || '_select_authenticated'
    ) then
      execute format(
        'create policy %I on uk_aq_core.%I for select using (auth.role() in (''authenticated'',''service_role''));',
        t || '_select_authenticated', t
      );
    end if;
    if not exists (
      select 1 from pg_policies p
      where p.schemaname = 'uk_aq_core' and p.tablename = t and p.policyname = t || '_write_service_role'
    ) then
      execute format(
        'create policy %I on uk_aq_core.%I for all using (auth.role() = ''service_role'') with check (auth.role() = ''service_role'');',
        t || '_write_service_role', t
      );
    end if;
  end loop;
end $$;

do $$
declare
  t text;
begin
  for t in
    select unnest(ARRAY['uk_air_sos_site_register','laqn_site_register','uk_air_sos_station_refs','breathelondon_station_checkpoints','erg_laqn_station_checkpoints','uk_air_sos_timeseries_checkpoints','openaq_station_checkpoints','openaq_timeseries_checkpoints','error_logs']::text[])
  loop
    execute format('alter table uk_aq_raw.%I enable row level security', t);
    if not exists (
      select 1 from pg_policies p
      where p.schemaname = 'uk_aq_raw' and p.tablename = t and p.policyname = t || '_select_service_role'
    ) then
      execute format(
        'create policy %I on uk_aq_raw.%I for select using (auth.role() = ''service_role'');',
        t || '_select_service_role', t
      );
    end if;
    if not exists (
      select 1 from pg_policies p
      where p.schemaname = 'uk_aq_raw' and p.tablename = t and p.policyname = t || '_write_service_role'
    ) then
      execute format(
        'create policy %I on uk_aq_raw.%I for all using (auth.role() = ''service_role'') with check (auth.role() = ''service_role'');',
        t || '_write_service_role', t
      );
    end if;
  end loop;
end $$;

-- Service role privileges (required for PostgREST access to non-public schemas).
grant usage on schema uk_aq_core to service_role;
grant all on all tables in schema uk_aq_core to service_role;
grant all on all sequences in schema uk_aq_core to service_role;
grant execute on all functions in schema uk_aq_core to service_role;

alter default privileges in schema uk_aq_core
  grant all on tables to service_role;
alter default privileges in schema uk_aq_core
  grant all on sequences to service_role;
alter default privileges in schema uk_aq_core
  grant execute on functions to service_role;

grant usage on schema uk_aq_raw to service_role;
grant all on all tables in schema uk_aq_raw to service_role;
grant all on all sequences in schema uk_aq_raw to service_role;
grant execute on all functions in schema uk_aq_raw to service_role;

alter default privileges in schema uk_aq_raw
  grant all on tables to service_role;
alter default privileges in schema uk_aq_raw
  grant all on sequences to service_role;
alter default privileges in schema uk_aq_raw
  grant execute on functions to service_role;

grant usage on schema uk_aq_pop to service_role;
grant all on all tables in schema uk_aq_pop to service_role;
grant all on all sequences in schema uk_aq_pop to service_role;
grant execute on all functions in schema uk_aq_pop to service_role;

alter default privileges in schema uk_aq_pop
  grant all on tables to service_role;
alter default privileges in schema uk_aq_pop
  grant all on sequences to service_role;
alter default privileges in schema uk_aq_pop
  grant execute on functions to service_role;

-- PostGIS types/functions live in public on Supabase; service_role needs schema
-- usage to write/read geography columns via PostgREST.
grant usage on schema public to service_role;

-- PostGIS system table: enable RLS when ownership/privileges allow it.
do $$
declare
  owner_name text;
begin
  select r.rolname into owner_name
  from pg_class c
  join pg_roles r on r.oid = c.relowner
  where c.relname = 'spatial_ref_sys'
    and c.relnamespace = 'public'::regnamespace;

  if owner_name is null then
    raise notice 'spatial_ref_sys not found; skipping RLS setup.';
    return;
  end if;

  if owner_name = current_user then
    execute 'alter table public.spatial_ref_sys enable row level security';
    if not exists (
      select 1 from pg_policies p
      where p.schemaname = 'public'
        and p.tablename = 'spatial_ref_sys'
        and p.policyname = 'spatial_ref_sys_select_all'
    ) then
      execute
        'create policy spatial_ref_sys_select_all on public.spatial_ref_sys for select using (auth.role() in (''anon'',''authenticated'',''service_role''));';
    end if;
  else
    raise notice 'Skipping RLS on public.spatial_ref_sys; owner is %, current_user is %', owner_name, current_user;
  end if;
exception when insufficient_privilege then
  raise notice 'Skipping RLS on public.spatial_ref_sys due to insufficient privileges.';
end $$;

-- View grants (core PM2.5 rollups).
alter view if exists uk_aq_core.la_latest_pm25 set (security_invoker = true);
alter view if exists uk_aq_core.pcon_latest_pm25 set (security_invoker = true);

grant select on uk_aq_core.la_latest_pm25 to authenticated, service_role;
grant select on uk_aq_core.pcon_latest_pm25 to authenticated, service_role;

do $$
declare
  t text;
begin
  for t in
    select unnest(ARRAY['pm25_population_exposure','pm25_amct_sites','nomis_population_observations','nomis_dataset_registry','nomis_ingest_runs','nomis_ingest_checkpoints','nomis_geography_catalogue','nrs_population_observations','nrs_dataset_registry','nrs_ingest_runs','nrs_ingest_checkpoints','nrs_geography_catalogue','nisra_population_observations','nisra_dataset_registry','nisra_ingest_runs','nisra_ingest_checkpoints','nisra_geography_catalogue']::text[])
  loop
    execute format('alter table uk_aq_pop.%I enable row level security', t);
    if not exists (
      select 1 from pg_policies p
      where p.schemaname = 'uk_aq_pop' and p.tablename = t and p.policyname = t || '_select_authenticated'
    ) then
      execute format(
        'create policy %I on uk_aq_pop.%I for select using (auth.role() in (''authenticated'',''service_role''));',
        t || '_select_authenticated', t
      );
    end if;
    if not exists (
      select 1 from pg_policies p
      where p.schemaname = 'uk_aq_pop' and p.tablename = t and p.policyname = t || '_write_service_role'
    ) then
      execute format(
        'create policy %I on uk_aq_pop.%I for all using (auth.role() = ''service_role'') with check (auth.role() = ''service_role'');',
        t || '_write_service_role', t
      );
    end if;
  end loop;
end $$;
