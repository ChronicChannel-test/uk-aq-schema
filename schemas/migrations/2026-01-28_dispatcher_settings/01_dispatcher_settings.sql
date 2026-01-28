-- Dispatcher settings table + public view.

set search_path = uk_aq_core, public;

create table if not exists uk_aq_core.dispatcher_settings (
  id smallint primary key default 1,
  dispatcher_parallel_ingest boolean default false,
  max_runs_per_dispatch_call int default 1,
  updated_at timestamptz default now(),
  check (max_runs_per_dispatch_call >= 1)
);

insert into uk_aq_core.dispatcher_settings (id, dispatcher_parallel_ingest, max_runs_per_dispatch_call)
values (1, false, 1)
on conflict (id) do nothing;

alter table uk_aq_core.dispatcher_settings enable row level security;

-- Policies (match uk_aq_security.sql expectations)
do $$
begin
  if not exists (
    select 1 from pg_policies p
    where p.schemaname = 'uk_aq_core'
      and p.tablename = 'dispatcher_settings'
      and p.policyname = 'dispatcher_settings_select_authenticated'
  ) then
    execute 'create policy dispatcher_settings_select_authenticated on uk_aq_core.dispatcher_settings for select using (auth.role() in (''authenticated'',''service_role''));';
  end if;
  if not exists (
    select 1 from pg_policies p
    where p.schemaname = 'uk_aq_core'
      and p.tablename = 'dispatcher_settings'
      and p.policyname = 'dispatcher_settings_write_service_role'
  ) then
    execute 'create policy dispatcher_settings_write_service_role on uk_aq_core.dispatcher_settings for all using (auth.role() = ''service_role'') with check (auth.role() = ''service_role'');';
  end if;
end $$;

-- Public view
set search_path = uk_aq_public, public;
create or replace view dispatcher_settings as
select id, dispatcher_parallel_ingest, max_runs_per_dispatch_call, updated_at
from uk_aq_core.dispatcher_settings;

alter view if exists dispatcher_settings set (security_invoker = true);

grant usage on schema uk_aq_public to authenticated, service_role;
grant select on uk_aq_public.dispatcher_settings to authenticated, service_role;
