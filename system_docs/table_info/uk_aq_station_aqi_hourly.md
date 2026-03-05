# station_aqi_hourly

Station-hour AQI fact table in AggDaily DB.

## Fields
- station_id: FK to mirrored `uk_aq_core.stations.id`.
- timestamp_hour_utc: UTC hour bucket.
- `*_hourly_mean_ugm3`: Pollutant hourly means used in index calculations.
- `pm25_rolling24h_mean_ugm3`, `pm10_rolling24h_mean_ugm3`: Rolling 24-hour means used for DAQI PM levels.
- `*_hourly_capture_ratio`, `*_hourly_sample_count`, `*_hourly_expected_count`: Hourly completeness/cadence context.
- `pm25_rolling24h_valid_hours`, `pm10_rolling24h_valid_hours`: Count of valid hourly means in running 24h window.
- `daqi_*_index_level`, `daqi_*_index_band`: Pollutant-specific DAQI outputs.
- `eaqi_*_index_level`, `eaqi_*_index_band`: Pollutant-specific EAQI outputs.
- created_at, updated_at: Audit timestamps.

## Notes
- Primary key is `(station_id, timestamp_hour_utc)`.
- Rows are upserted idempotently from Cloud Run AQI worker.
