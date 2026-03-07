# Agent Notes

## Scope
- This repo is the canonical source for UK AQ SQL schema DDL.
- Files under `archive/` are read-only after creation.

## Schema Placement Policy
- Canonical table/function/view DDL must be authored in this repo under `schemas/`.
- Do not keep schema changes only in ingest/ops worker-local SQL files.
- For AggDaily DB changes, always update the main schema file:
  - `schemas/aggdaily_db/uk_aq_aggdaily_schema.sql`
- If a targeted apply file is used, keep it in this repo under:
  - `schemas/aggdaily_db/`
  and keep it aligned with the main AggDaily schema.
