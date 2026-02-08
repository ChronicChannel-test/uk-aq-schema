# uk_aq_history.observations

History-only observations table for long-term retention outside the main `public.observations` table.

## Fields
- connector_code: Source connector code (natural key component).
- service_ref: Upstream service identifier (natural key component).
- timeseries_ref: Upstream timeseries identifier (natural key component).
- observed_at: Observation timestamp in UTC (natural key component).
- value: Observation value (double precision).
- status: Upstream status/flag text.
- moved_at: Timestamp when the row was copied into history (default now()).

## Notes
- Primary key is `(connector_code, service_ref, timeseries_ref, observed_at)`.
- RLS is service_role only (intended for Edge Functions / server-side use).
- Designed for append-only history retention; no tables are moved out of `public` yet.
