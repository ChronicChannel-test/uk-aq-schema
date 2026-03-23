# timeseries_aqi_daily

Daily timeseries rollups for AQI level distributions by standard + pollutant.

## Fields
- timeseries_id: FK to `uk_aq_core.timeseries.id`.
- station_id: nullable FK to `uk_aq_core.stations.id` (denormalized for convenience).
- connector_id: FK to `uk_aq_core.connectors.id`.
- observed_day: UTC calendar day.
- standard_code: `daqi` or `eaqi`.
- pollutant_code: `pm25`, `pm10`, `no2`.
- index_level_hour_counts: Hour counts by AQI level (length 10 for DAQI, 6 for EAQI).
- valid_hour_count: Number of hourly slots with valid index level.
- max_index_level: Worst level reached for that day.
- created_at, updated_at: Audit timestamps.

## Notes
- Primary key is `(timeseries_id, observed_day, standard_code, pollutant_code)`.
- Rebuilt idempotently for affected day ranges.
