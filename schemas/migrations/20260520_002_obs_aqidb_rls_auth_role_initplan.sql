-- Obs AQI DB migration: rewrite RLS policy predicates to use initplan-safe auth role checks.
-- Run in obs_aqidb.

do $$
declare
  p record;
  v_using text;
  v_check text;
  v_sql text;
begin
  for p in
    select
      schemaname,
      tablename,
      policyname,
      qual,
      with_check
    from pg_policies
    where schemaname in ('public', 'uk_aq_core', 'uk_aq_observs', 'uk_aq_aqilevels', 'uk_aq_ops')
      and (
        (position('auth.role()' in coalesce(qual, '')) > 0 and position('(select auth.role())' in coalesce(qual, '')) = 0)
        or
        (position('auth.role()' in coalesce(with_check, '')) > 0 and position('(select auth.role())' in coalesce(with_check, '')) = 0)
      )
  loop
    v_using := p.qual;
    v_check := p.with_check;

    if position('auth.role()' in coalesce(v_using, '')) > 0
       and position('(select auth.role())' in coalesce(v_using, '')) = 0 then
      v_using := replace(v_using, 'auth.role()', '(select auth.role())');
    end if;

    if position('auth.role()' in coalesce(v_check, '')) > 0
       and position('(select auth.role())' in coalesce(v_check, '')) = 0 then
      v_check := replace(v_check, 'auth.role()', '(select auth.role())');
    end if;

    v_sql := format('alter policy %I on %I.%I', p.policyname, p.schemaname, p.tablename);
    if v_using is not null then
      v_sql := v_sql || format(' using (%s)', v_using);
    end if;
    if v_check is not null then
      v_sql := v_sql || format(' with check (%s)', v_check);
    end if;

    execute v_sql;
  end loop;
end
$$;
