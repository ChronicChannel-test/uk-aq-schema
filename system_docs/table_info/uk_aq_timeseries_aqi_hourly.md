# timeseries_aqi_hourly

Timeseries-hour AQI fact table in ObsAQIDB.

## Fields
- timeseries_id: FK to `uk_aq_core.timeseries.id`.
- station_id: nullable FK to `uk_aq_core.stations.id` (denormalized for convenience).
- connector_id: FK to `uk_aq_core.connectors.id`.
- pollutant_code: `pm25`, `pm10`, or `no2`.
- timestamp_hour_utc: UTC hour bucket.
- `daqi_input_value_ugm3`, `daqi_input_averaging_code`: Normalized DAQI input value and averaging code.
- `daqi_index_level`, `daqi_source_observation_count`, `daqi_required_observation_count`, `daqi_calculation_status`, `daqi_missing_reason`: Normalized DAQI output metadata.
- `eaqi_input_value_ugm3`, `eaqi_input_averaging_code`: Normalized EAQI input value and averaging code.
- `eaqi_index_level`, `eaqi_source_observation_count`, `eaqi_required_observation_count`, `eaqi_calculation_status`, `eaqi_missing_reason`: Normalized EAQI output metadata.
- hourly_sample_count: Hourly sample count for this timeseries + pollutant row.
- algorithm_version, computed_at_utc: Compute metadata for the normalized hourly row.
- Legacy compatibility columns are still exposed in the public view for the transition period:
  - `hourly_mean_ugm3`, `rolling24h_mean_ugm3`
  - `no2_hourly_mean_ugm3`, `pm25_hourly_mean_ugm3`, `pm10_hourly_mean_ugm3`
  - `pm25_rolling24h_mean_ugm3`, `pm10_rolling24h_mean_ugm3`
  - `daqi_no2_index_level`, `daqi_pm25_rolling24h_index_level`, `daqi_pm10_rolling24h_index_level`
  - `eaqi_no2_index_level`, `eaqi_pm25_index_level`, `eaqi_pm10_index_level`
  - `updated_at`

## Notes
- Primary key is `(timeseries_id, timestamp_hour_utc)`.
- Rows are upserted idempotently from Cloud Run AQI worker.
- The canonical public read contract is `uk_aq_public.uk_aq_timeseries_aqi_hourly`, which preserves the legacy column prefix for compatibility and appends the normalized DAQI/EAQI fields after `updated_at`.
- AQI lookup uses continuous upper-bound threshold matching, so decimal concentrations between published integer legend thresholds do not create gaps.
- PM2.5 examples:
  - EAQI hourly: `Good <=5`, `Fair >5 to <=15`, `Moderate >15 to <=50`, `Poor >50 to <=90`, `Very poor >90 to <=140`, `Extremely poor >140`
  - DAQI rolling 24h mean: `1 <=11`, `2 >11 to <=23`, `3 >23 to <=35`, `4 >35 to <=41`, `5 >41 to <=47`, `6 >47 to <=53`, `7 >53 to <=58`, `8 >58 to <=64`, `9 >64 to <=70`, `10 >70`
- PM10 examples:
  - EAQI hourly: `Good <=15`, `Fair >15 to <=45`, `Moderate >45 to <=120`, `Poor >120 to <=195`, `Very poor >195 to <=270`, `Extremely poor >270`
  - DAQI rolling 24h mean: `1 <=16`, `2 >16 to <=33`, `3 >33 to <=50`, `4 >50 to <=58`, `5 >58 to <=66`, `6 >66 to <=75`, `7 >75 to <=83`, `8 >83 to <=91`, `9 >91 to <=100`, `10 >100`
- NO2 examples:
  - EAQI hourly: `Good <=10`, `Fair >10 to <=25`, `Moderate >25 to <=60`, `Poor >60 to <=100`, `Very poor >100 to <=150`, `Extremely poor >150`
  - DAQI hourly mean: `1 <=67`, `2 >67 to <=134`, `3 >134 to <=200`, `4 >200 to <=267`, `5 >267 to <=334`, `6 >334 to <=400`, `7 >400 to <=467`, `8 >467 to <=534`, `9 >534 to <=600`, `10 >600`
