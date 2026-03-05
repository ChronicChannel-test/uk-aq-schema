# station_aqi_monthly

Monthly station rollups for AQI level distributions by standard + pollutant.

## Fields
- station_id: FK to mirrored `uk_aq_core.stations.id`.
- observed_month: UTC month start date.
- standard_code: `daqi` or `eaqi`.
- pollutant_code: `pm25`, `pm10`, `no2`.
- index_level_hour_counts: Hour counts by AQI level (length 10 for DAQI, 6 for EAQI).
- valid_hour_count: Number of hourly slots with valid index level.
- max_index_level: Worst level reached for the month.
- created_at, updated_at: Audit timestamps.

## Notes
- Primary key is `(station_id, observed_month, standard_code, pollutant_code)`.
- Rebuilt idempotently for affected month ranges.
