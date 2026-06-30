-- Phase 8: destructive removal of the legacy network model.
-- Apply only after all readers and writers use
-- uk_aq_core.stations.network_id -> uk_aq_core.networks.id.

begin;

-- Remove the exposed dependent view before its source relation.
drop view if exists uk_aq_public.station_network_memberships;

do $$
declare
  relation_name text;
  policy_name text;
begin
  foreach relation_name in array array[
    'station_network_memberships',
    'uk_aq_networks'
  ]
  loop
    if to_regclass(format('uk_aq_core.%I', relation_name)) is null then
      continue;
    end if;
    execute format(
      'revoke all privileges on table uk_aq_core.%I from anon, authenticated, service_role',
      relation_name
    );
    for policy_name in
      select pol.polname
      from pg_policy pol
      join pg_class cls on cls.oid = pol.polrelid
      join pg_namespace ns on ns.oid = cls.relnamespace
      where ns.nspname = 'uk_aq_core'
        and cls.relname = relation_name
    loop
      execute format(
        'drop policy if exists %I on uk_aq_core.%I',
        policy_name,
        relation_name
      );
    end loop;
  end loop;
end
$$;

-- Required order: the membership relation references the old catalog.
drop table if exists uk_aq_core.station_network_memberships;
drop table if exists uk_aq_core.uk_aq_networks;

do $$
begin
  if to_regclass('uk_aq_core.station_network_memberships') is not null then
    raise exception 'legacy station membership relation still exists';
  end if;
  if to_regclass('uk_aq_core.uk_aq_networks') is not null then
    raise exception 'legacy network catalog relation still exists';
  end if;
end
$$;

commit;

-- Post-apply validation (both values must be null):
select
  to_regclass('uk_aq_core.station_network_memberships') as legacy_memberships,
  to_regclass('uk_aq_core.uk_aq_networks') as legacy_networks;

-- Dependency check (must return zero rows):
select
  dependent_ns.nspname as dependent_schema,
  dependent.relname as dependent_object
from pg_depend dependency
join pg_class referenced on referenced.oid = dependency.refobjid
join pg_namespace referenced_ns on referenced_ns.oid = referenced.relnamespace
join pg_class dependent on dependent.oid = dependency.objid
join pg_namespace dependent_ns on dependent_ns.oid = dependent.relnamespace
where referenced_ns.nspname = 'uk_aq_core'
  and referenced.relname in (
    'station_network_memberships',
    'uk_aq_networks'
  );
