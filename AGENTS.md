# Agent Notes

## Scope
- This repo is the canonical source for UK AQ SQL schema DDL.
- Files under `archive/` are read-only after creation.

## Codex operating mode
Default mode is code-only implementation.
Codex should:
- make focused code, schema, documentation, and test edits requested by the task;
- run only fast, local, non-destructive checks needed to verify the edit;
- provide a clear manual validation and deployment plan;
- include exact SQL, gcloud, wrangler, GitHub Actions, and Supabase commands for the user to run manually.
Codex must not, unless explicitly asked:
- run SQL against live/test Supabase databases;
- apply migration files;
- deploy Cloud Run services, Workers, or GitHub Actions workflows;
- run backfills, reconciliations, bulk jobs, or long-running data jobs;
- run broad external API fetches;
- repeatedly inspect cloud logs;
- make operational changes in GCP, Supabase, Cloudflare, R2, Dropbox, or GitHub settings.
When database or deployment work is needed, Codex should stop after producing:
1. files changed,
2. tests run,
3. exact manual commands,
4. expected outputs,
5. rollback notes,
6. post-deploy validation checklist.

## Permission levels
Unless the prompt says otherwise, use Level 1.
### Level 1 — Code only
Edit files and run small local/static tests. Do not touch external services or databases.
### Level 2 — Local validation
Level 1 plus local-only scripts/tests that do not call Supabase, GCP, Cloudflare, R2, Dropbox, or external APIs.
### Level 3 — Assisted operations
Prepare SQL, deploy commands, and validation commands, but do not run them.
### Level 4 — Execute operations
Only when explicitly requested in the prompt. May run database, deployment, or cloud commands.

## Schema Placement Policy
- Canonical table/function/view DDL must be authored in this repo under `schemas/`.
- Do not keep schema changes only in ingest/ops worker-local SQL files.
- For AQI levels schema changes, always update the main schema file:
  - `schemas/obs_aqi_db/uk_aq_obs_aqi_db_schema.sql`
- If a targeted apply file is used, keep it in this repo under:
  - `schemas/obs_aqi_db/`
  and keep it aligned with the main AQI levels schema.

## Implementation Reporting
- When changing code, schema, workflows, or config, always include clear implementation steps in the response.
- Implementation steps must state what changed, which files were changed, and any required apply/deploy/run commands.
- If no code changes were made, state that explicitly.

## R2/Cloudflare Cache Cost Policy
- For AQI history served via R2 + Cloudflare, assume cost is primarily driven by R2 operation counts (especially Class B reads) and Worker request volume, not R2 bandwidth egress.
- Prefer stable request URLs/params for normal traffic so Cloudflare cache can return warm-cache hits.
- Use cache-buster/version params only for diagnostics, forced-refresh actions, or explicit bypass-cache testing.
- When evaluating performance/cost changes, check cache-hit behavior (`CF-Cache-Status`) and distinguish cache-hit traffic from origin-fetch traffic.

## Search Tool Preference
- Prefer `grep` for text search and file discovery; do not use `rg` unless explicitly requested.
