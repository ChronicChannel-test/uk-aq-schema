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
