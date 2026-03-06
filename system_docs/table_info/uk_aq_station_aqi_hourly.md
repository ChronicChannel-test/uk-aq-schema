# station_aqi_hourly

Station-hour AQI fact table in AggDaily DB.

## Fields
- station_id: FK to mirrored `uk_aq_core.stations.id`.
- timestamp_hour_utc: UTC hour bucket.
- `*_hourly_mean_ugm3`: Pollutant hourly means used in index calculations.
- `pm25_rolling24h_mean_ugm3`, `pm10_rolling24h_mean_ugm3`: Rolling 24-hour means used for DAQI PM levels.
- `*_hourly_sample_count`: Hourly sample counts selected for each pollutant.
- `daqi_no2_index_level`: DAQI level from NO2 hourly mean.
- `daqi_pm25_rolling24h_index_level`, `daqi_pm10_rolling24h_index_level`: DAQI levels from PM rolling 24-hour means.
- `eaqi_no2_index_level`, `eaqi_pm25_index_level`, `eaqi_pm10_index_level`: Pollutant-specific EAQI levels from hourly means.
- created_at, updated_at: Audit timestamps.

## Notes
- Primary key is `(station_id, timestamp_hour_utc)`.
- Rows are upserted idempotently from Cloud Run AQI worker.
