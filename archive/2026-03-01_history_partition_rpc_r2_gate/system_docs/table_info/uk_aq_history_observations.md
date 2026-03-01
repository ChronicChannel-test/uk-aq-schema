# uk_aq_history.observations

History-only observations table for long-term retention outside the main `public.observations` table.

## Fields
- connector_id: Internal connector integer id (key component).
- timeseries_id: Internal timeseries integer id (key component).
- observed_at: Observation timestamp in UTC (natural key component).
- value: Observation value (double precision).
- status_id: Canonical status code id (`uk_aq_history.status_codes.status_id`).
- created_at: Timestamp when the row was first created in history (default now()).

## Notes
- Primary key is `(connector_id, timeseries_id, observed_at)`.
- RLS is service_role only (intended for Edge Functions / server-side use).
- Designed for append-only history retention tightly coupled to main DB ids.
