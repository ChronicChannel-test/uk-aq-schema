# UK-AQ Shared Schema Repo

This outlines the shared schema repo layout and how to pull it into each project.

## Repo layout

```
uk-aq-schema/
  README.md
  schemas/
    uk_air_quality_schema.sql
    uk_air_quality_views.sql
    uk_aq_history_schema.sql
    purpleair_schema.sql
    drop_uk_air_quality_tables.sql
  seeds/
    uk_aq_networks_seed.sql
    uk_aq_phenomena_pollutants.sql
  dbml/
    uk_aq_schema.dbml
    uk_aq_schema_extended.dbml
    uk_air_sos_connector_membership_subset.dbml
  system_docs/
    schema-overview.md
    shared-schema-repo.md
    table_info/
```

## How to pull into each project

Recommended: Git submodule + a lightweight apply script.

1) Add submodule:
```
git submodule add git@github.com:<org>/uk-aq-schema.git vendor/uk-aq-schema
```

2) Add a small apply script in each project (example for the main DB):
```
#!/usr/bin/env bash
set -euo pipefail

SCHEMA_DIR="${PWD}/vendor/uk-aq-schema"
psql "$SUPABASE_DB_URL" <<'SQL'
\i '"$SCHEMA_DIR/schemas/uk_air_quality_schema.sql"'
\i '"$SCHEMA_DIR/schemas/uk_air_quality_views.sql"'
\i '"$SCHEMA_DIR/schemas/purpleair_schema.sql"'
\i '"$SCHEMA_DIR/seeds/uk_aq_networks_seed.sql"'
\i '"$SCHEMA_DIR/seeds/uk_aq_phenomena_pollutants.sql"'
SQL
```

3) History DB apply (example):
```
#!/usr/bin/env bash
set -euo pipefail

SCHEMA_DIR="${PWD}/vendor/uk-aq-schema"
psql "$HISTORY_DB_URL" <<'SQL'
\i '"$SCHEMA_DIR/schemas/uk_aq_history_schema.sql"'
SQL
```

Notes:
- `drop_uk_air_quality_tables.sql` is for manual use only.
- Supabase SQL Editor does not support `\i` include syntax. Use `psql` or the Supabase CLI.
- Keep environment-specific secrets and cron schedules in each project, not in the shared repo.

## Separate history repo

If you want a dedicated history repo (functions + jobs), keep:
- `uk-aq-history/` for Edge Functions and scheduling logic.
- `uk-aq-schema/` for shared SQL + DBML.
- Existing ingest repos unchanged; they only consume the shared schema.
