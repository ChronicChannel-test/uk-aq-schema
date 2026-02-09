# Cross-repo map: uk-aq-schema

## Main repo
- `CIC-test-uk-aq-ingest` is the main repo for this project and the default starting point for cross-repo tasks.

## Purpose
This repo defines the UK AQ database structure: schemas, tables, views, and security. It is the source of truth for DDL that the ingest and history repos depend on.

## Repo structure (top-level)
- `schemas/`: Primary SQL schema files (core/raw/history/pop), plus migrations and archive.
- `seeds/`: Seed data (if present) for initial DB population.
- `dbml/`: DBML representations of the schema.
- `system_docs/`: Schema and database documentation.
- `uk_aq_pcon_aiven_schema.sql`: Standalone schema for pcon Aiven (if used).

## How this repo connects to the others
- **Ingest repo**: `CIC-test-uk-aq-ingest` runs ingests that write into these schemas.
- **History repo**: `uk-aq-history` (if present) uses these schemas for historical backfills/analysis.
- **Edge Functions**: owned and deployed from the ingest repo; they rely on tables/functions defined here.
- **Change flow**: updating tables or functions here typically requires updating ingest queries and RPC calls in the ingest repo.

## Setup & run (lightweight)
### Required env vars (names only; discoverable in docs)
- `SUPABASE_DB_URL` (used in `system_docs/shared-schema-repo.md`)
- `HISTORY_DB_URL` (used in `system_docs/shared-schema-repo.md`)

### Commands
No scripted commands were found in this repo. There is no preferred schema-apply command yet; use your preferred SQL tooling (confirm).

## Where to start
- **Core schema files**: `schemas/uk_aq_core_schema.sql`, `schemas/uk_aq_raw_schema.sql`, `schemas/uk_aq_history_schema.sql`.
- **Public views**: `schemas/uk_aq_public_views.sql`.
- **Security**: `schemas/uk_aq_security.sql`.
- **Docs**: `system_docs/`.

## Conventions
- Schema namespaces use `uk_aq_core`, `uk_aq_raw`, `uk_aq_history`, `uk_aq_public`.
- Table names are snake_case; reference ids use `_ref` suffix (as noted in ingest repo README).
- RPC and view names are prefixed with `uk_aq_`.
- Naming conventions (project-wide) live in the ingest repo: `../CIC-test-uk-aq-ingest/AGENTS.md`.

## Permissions (REQUIRED)
- The agent may edit any files without asking for permission, except files under any `/archive` directory.

## Links
- Existing README: `README.md`
- Schemas directory: `schemas/`
- System docs: `system_docs/`
- Ingest repo (sibling): `../CIC-test-uk-aq-ingest`
- Naming conventions (ingest repo): `../CIC-test-uk-aq-ingest/AGENTS.md`
- History repo (sibling): `../CIC-test-uk-aq-history/uk-aq-history`

## WORKING STYLE (IMPORTANT)

REQUIRED OUTPUT FORMAT

Summary (2–5 bullets)
Files changed (paths)
Implementation details (short, specific)
Supabase steps (instructions only,)
Verification checklist (clear pass/fail)
