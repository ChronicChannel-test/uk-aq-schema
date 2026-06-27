# blondon_communities_timeseries_checkpoints

Checkpoint table for staged Breathe London Communities data pulls.

Columns:
- station_id: FK to `stations.id`.
- species: API species code (`IPM25`, `INO2`).
- timeseries_id: FK to `timeseries.id` for the station/species.
- last_observed_at: most recent observation timestamp ingested.
- last_polled_at: last time the API was queried for this station/species.
- last_error: last error message (if any).
- created_at, updated_at: audit timestamps.

Indexes:
- Index on `last_observed_at` for auditing lagging checkpoints.

Identity:
- This is connector-specific state for `connector_code = 'blondon_communities'`.
- Public `network_code` and shared `service_ref` remain `breathelondon`.
