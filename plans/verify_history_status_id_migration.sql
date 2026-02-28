-- Verify one-way history migration from observations.status (text) to observations.status_id (smallint).
-- Safe to run multiple times.

-- 1) `status` column should not exist on uk_aq_history.observations.
select
  case
    when exists (
      select 1
      from information_schema.columns
      where table_schema = 'uk_aq_history'
        and table_name = 'observations'
        and column_name = 'status'
    ) then 'FAIL: uk_aq_history.observations.status still exists'
    else 'PASS: uk_aq_history.observations.status is absent'
  end as status_column_removed_check;

-- 2) `status_id` should exist and be smallint (int2).
select
  case
    when exists (
      select 1
      from information_schema.columns
      where table_schema = 'uk_aq_history'
        and table_name = 'observations'
        and column_name = 'status_id'
        and udt_name = 'int2'
    ) then 'PASS: uk_aq_history.observations.status_id exists as smallint'
    else 'FAIL: uk_aq_history.observations.status_id missing or wrong type'
  end as status_id_type_check;

select
  column_name,
  data_type,
  udt_name,
  is_nullable
from information_schema.columns
where table_schema = 'uk_aq_history'
  and table_name = 'observations'
  and column_name = 'status_id';

-- 3) FK should exist on observations.status_id with ON DELETE SET NULL.
select
  tc.constraint_name,
  kcu.column_name,
  ccu.table_schema as referenced_schema,
  ccu.table_name as referenced_table,
  ccu.column_name as referenced_column,
  rc.update_rule,
  rc.delete_rule
from information_schema.table_constraints tc
join information_schema.key_column_usage kcu
  on tc.constraint_name = kcu.constraint_name
 and tc.constraint_schema = kcu.constraint_schema
join information_schema.referential_constraints rc
  on tc.constraint_name = rc.constraint_name
 and tc.constraint_schema = rc.constraint_schema
join information_schema.constraint_column_usage ccu
  on rc.unique_constraint_name = ccu.constraint_name
 and rc.unique_constraint_schema = ccu.constraint_schema
where tc.constraint_type = 'FOREIGN KEY'
  and tc.table_schema = 'uk_aq_history'
  and tc.table_name = 'observations'
  and kcu.column_name = 'status_id';

select
  case
    when exists (
      select 1
      from information_schema.table_constraints tc
      join information_schema.key_column_usage kcu
        on tc.constraint_name = kcu.constraint_name
       and tc.constraint_schema = kcu.constraint_schema
      join information_schema.referential_constraints rc
        on tc.constraint_name = rc.constraint_name
       and tc.constraint_schema = rc.constraint_schema
      where tc.constraint_type = 'FOREIGN KEY'
        and tc.table_schema = 'uk_aq_history'
        and tc.table_name = 'observations'
        and kcu.column_name = 'status_id'
        and rc.delete_rule = 'SET NULL'
    ) then 'PASS: status_id FK exists with ON DELETE SET NULL'
    else 'FAIL: status_id FK missing or delete rule is not SET NULL'
  end as status_id_fk_check;

-- 4) Inspect status_codes grants and confirm ingest-like roles have no privileges.
select
  grantee,
  privilege_type,
  is_grantable
from information_schema.table_privileges
where table_schema = 'uk_aq_history'
  and table_name = 'status_codes'
order by grantee, privilege_type;

-- Ingest-role privilege check: returns zero rows if no ingest-like roles exist.
select
  r.rolname as role_name,
  has_table_privilege(r.rolname, 'uk_aq_history.status_codes', 'SELECT') as can_select,
  has_table_privilege(r.rolname, 'uk_aq_history.status_codes', 'INSERT') as can_insert,
  has_table_privilege(r.rolname, 'uk_aq_history.status_codes', 'UPDATE') as can_update,
  has_table_privilege(r.rolname, 'uk_aq_history.status_codes', 'DELETE') as can_delete
from pg_roles r
where r.rolname ilike '%ingest%'
order by r.rolname;
