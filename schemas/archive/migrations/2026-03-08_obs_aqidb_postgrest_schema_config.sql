-- Ensure PostgREST on obs_aqidb only targets active schemas after hard cut.
-- This prevents schema cache failures on removed schema names.

alter role authenticator in database postgres
  set pgrst.db_schemas = 'public,uk_aq_public';

alter role authenticator in database postgres
  set pgrst.db_extra_search_path = 'extensions,public';

notify pgrst, 'reload config';
notify pgrst, 'reload schema';
