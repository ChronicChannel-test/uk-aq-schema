# UK-AQ Supabase Schema Overview

This document summarizes the schema defined in `schemas/uk_air_quality_schema.sql` for ingesting UK-AIR SOS / 52°North timeseries data and PM2.5 target tracking.

## Extensions
- `postgis` for spatial columns (geography Point).
- `pgcrypto` for UUID generation (gen_random_uuid).
- TODO: When we refactor schemas, move extensions (postgis/pgcrypto) out of `public` into a dedicated schema (e.g. `extensions`) and update function search_path entries accordingly.

## Core reference tables
- External identifiers that arrive as text (even if numeric) are stored as `*_ref`; internal joins use `*_id` columns. `connectors.id`/`timeseries.id` and all `connector_id`/`timeseries_id` columns are integer; other ids can remain bigint.
- `connectors`: network connectors with integer `id` (internal) and `connector_code` for filename prefixes, plus `label` (source label) and `display_name` (UI), URL and polling fields (`station_display_name_template`, `overwrite_station_name`, `poll_enabled`, `poll_interval_minutes`, `poll_window_hours`, `poll_timeseries_batch_size`, `stations_bbox_supported`, `timeseries_station_filter_supported`, `last_polled_at`).
- `categories`: high-level grouping, per connector.
- `observed_properties`: canonical observed-property catalog shared across connectors (`code`, `display_name`, `domain` = `aq|met`, optional `canonical_uom`).
- `phenomena`: slim connector/source bridge for what is measured; stores per-connector/source labels (`source_label`, `label`, optional `notation`/`pollutant_label`) and links to canonical `observed_properties` via `observed_property_id`.
- `offerings`: logical groupings, per connector + `service_ref`.
- `features`: features of interest with geometry (Point, 4326), per connector + `service_ref`.
- `procedures`: sensors/methods; optional raw_formats list, per connector + `service_ref`.
- `stations`: monitoring sites; bigint `id` (internal) with `station_ref` (external) and `service_ref` (remote SOS service id), integer `connector_id`, unique `(connector_id, service_ref, station_ref)`, plus lifecycle fields `first_seen_at`, `last_seen_at`, `removed_at`. Includes `station_name` as a cleaned display name, `station_type` as the service-provided classification, `station_exposure` for indoor/outdoor, and stores `la_code`/`la_version` and `pcon_code`/`pcon_version` for geography lookups.
- `station_metadata`: per-station JSON attributes for network-specific fields not stored on `stations` (ownership, device, status, siting metadata).
- `station_network_memberships`: multi-network membership metadata for stations, including a `network_code` (aligned to `uk_aq_networks.network_code`) and `is_primary` flag for preferred ingest source.
- `uk_aq_networks`: curated network catalog with `network_code`, `display_name`, and `connector_code` (use `uk_air_sos` for SOS-derived networks).
- Seed networks live in `seeds/uk_aq_networks_seed.sql`.
- `uk_air_sos_networks`: lookup table for network labels from the UK-AIR monitoring sites register (exact `network_ref`, optional `network_code`, and display name).
- `uk_air_sos_network_pollutants`: pollutant matching rules used to filter SOS network memberships by pollutant coverage.
- `uk_air_sos_site_register`: snapshot of the UK-AIR monitoring sites CSV, including UK-AIR IDs, coordinates, networks array, and raw payload for audit.
- `laqn_site_register`: snapshot of the LAQN site list (e.g., LondonAir or ERG API), including LAQN site refs, coordinates, network flags, and raw payload for audit.
- `uk_air_sos_station_refs`: mapping of SOS `stations` to UK-AIR site ids (`uk_air_id`) with match metadata for membership backfills.
- `breathelondon_timeseries_checkpoints`: per-site/species checkpoints for staged Breathe London data pulls.
- `erg_laqn_station_checkpoints`: per-station checkpoints for ERG LAQN batch polling.
- `uk_aq_ingest_runs`: per-run ingest summaries captured by the dispatcher (status + counts + last_observed_at).
- `dispatcher_settings`: dispatcher toggles (parallel ingest + max runs per call).

## Station geography
- `stations.la_code`/`la_version` and `stations.pcon_code`/`pcon_version` are populated externally (no boundary lookup tables in Supabase).
- `uk_aq_fix_station_geometry_swapped()`: fixes stations with swapped lat/lon coordinates.

## Timeseries and metadata
- `timeseries`: SOS timeseries metadata; integer `id` (internal) with `timeseries_ref` (external), `service_ref`, integer `connector_id`, and `station_id` bigint FK.
- `reference_values`: optional reference lines attached to a timeseries (name, color, value).

## Observations
- `observations`: raw time-value pairs for each timeseries (observed_at timestamptz, value, status flag). Partitioned by integer `connector_id` with primary key `(connector_id, timeseries_id, observed_at)` where both ids are integer.

## History schema (uk_aq_history)
- Defined in `schemas/obs_aqi_db/uk_aq_obs_aqi_db_schema.sql`.
- `uk_aq_history.observations`: history-only fact table keyed by integer internal ids (`connector_id`, `timeseries_id`, `observed_at`) with `value` (no `status_id`).
- Partitioned by UTC day range on `observed_at`, with `uk_aq_history.observations_default` as the out-of-range catch-all partition.
- Hot partition index policy: UTC today plus previous 2 UTC days keep unique btree key `(connector_id, timeseries_id, observed_at)` plus BRIN on `observed_at`; cold partitions keep BRIN only.
- History upsert RPC (`uk_aq_public.uk_aq_rpc_history_observations_upsert`) routes writes by UTC day:
  - hot partitions use direct partition `INSERT ... ON CONFLICT ... DO UPDATE`;
  - non-hot/missing partitions use update-then-insert fallback on the partitioned parent.
- RLS: service_role only (intended for Edge Functions / server-side access).
- The history schema is additive only; no existing tables are moved out of `public` yet.

## Ops size telemetry (dashboard support)
- `uk_aq_ops.db_size_metrics_hourly`: hourly DB cluster size points keyed by `database_label` (target hard-cut labels: `ingestdb`, `obs_aqidb`).
- `uk_aq_ops.schema_size_metrics_hourly`: hourly schema size points for `uk_aq_observs` and `uk_aq_aqilevels` with per-schema oldest timestamp.
- `uk_aq_ops.r2_domain_size_metrics_hourly`: hourly R2 History domain size points for `observations` and `aqilevels`.
- Public read views:
  - `uk_aq_public.uk_aq_db_size_metrics_hourly`
  - `uk_aq_public.uk_aq_schema_size_metrics_hourly`
  - `uk_aq_public.uk_aq_r2_domain_size_metrics_hourly`
- Service-role writer RPCs:
  - `uk_aq_rpc_db_size_metric_upsert` / `uk_aq_rpc_db_size_metric_cleanup`
  - `uk_aq_rpc_schema_size_metric_upsert` / `uk_aq_rpc_schema_size_metric_cleanup`
  - `uk_aq_rpc_r2_domain_size_metric_upsert` / `uk_aq_rpc_r2_domain_size_metric_cleanup`

## PM2.5 target tracking (optional)
- `pm25_population_exposure`: yearly Population Exposure Indicator (PEI) series with deltas and % change vs 2018 baseline.
- `pm25_amct_sites`: annual mean concentration per site/year to track AMCT and interim exceedances.

## Constituency reference tables
- `pcon_current`: current constituency electorate data (`gss_code`, `name`, `electorate`, `region`, `country`).
- `pcon_legacy`: legacy constituency electorate data for historical backfill (same columns as `pcon_current`).
- `gss_codes`: canonical registry of GSS codes across geographies (`gss_code`, `name`, `geography_type`, `valid_from`, `valid_to`).

## Guideline limits
- `uk_aq_guidelines`: pollutant guideline limits (WHO/UK/EU, etc.) with `pollutant`, `averaging_period_label`, `averaging_period_interval`, `level_label`, `limit_value`, `uom`, and optional `source`/`notes`/validity dates.

## AggDaily AQI aggregates
- AggDaily DB mirrors `uk_aq_core` metadata needed for joins and stores derived AQI outputs in `uk_aq_aggdaily`.
- Reference tables:
  - `aqi_standard_versions`: DAQI/EAQI version registry with validity dates.
  - `aqi_breakpoints`: range-based pollutant thresholds by standard/version/averaging metric.
- Station outputs:
  - `station_aqi_hourly`: station-hour concentrations, hourly sample counts, rolling 24h PM means, and pollutant-specific DAQI/EAQI `index_level` fields.
  - `station_aqi_daily`: per-day level distributions (`index_level_hour_counts`) by station, AQI standard, and pollutant.
  - `station_aqi_monthly`: monthly level distributions by station, AQI standard, and pollutant.
- Ops telemetry:
  - `uk_aq_ops.aqi_compute_runs`: run-mode/window/change metrics for AQI sync jobs (currently `sync_hourly` and `backfill`; legacy run-mode values may remain historically).
- AQI runtime RPCs in `uk_aq_public` (service-role):
  - `uk_aq_rpc_aqi_breakpoints_active`
  - `uk_aq_rpc_station_aqi_hourly_upsert`
  - `uk_aq_rpc_station_aqi_rollups_refresh`
  - `uk_aq_rpc_aqi_compute_run_log`
  - `uk_aq_rpc_aqi_compute_runs_cleanup`

## RLS (Row Level Security)
- RLS enabled on all domain tables (not on system tables like spatial_ref_sys), including `station_metadata` and `station_network_memberships`.
- Policies (idempotent via DO block):
  - `select`: allowed for roles `authenticated` and `service_role`.
  - `all` (insert/update/delete): allowed for `service_role` only.
- Adjust policies if you need anon read or user-owned row scoping.

## Notes on multi-pollutant support
- Schema is pollutant-agnostic: add new phenomena, stations, timeseries, and observations for NO2, O3, PM10, etc. No structural changes needed.

## Minimal ingestion flow
1) Discover metadata from the SOS REST API: services, stations, timeseries (use `expanded=true` for richer fields).
2) Upsert metadata into `connectors`, `stations`, `timeseries`, and related reference tables.
3) Fetch data via `/timeseries/{id}/getData` (format=tvp) and insert into `observations` (convert epoch ms to timestamptz).
4) Store optional `referenceValues`, `status_intervals`, `rendering_hints`, and `extras` when present.
