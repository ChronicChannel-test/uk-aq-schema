# aqi_breakpoints

Range lookup table for pollutant-specific AQI levels and bands.

## Fields
- standard_code: AQI standard key (`daqi` or `eaqi`).
- version_code: Version key linked to `aqi_standard_versions`.
- pollutant_code: Pollutant key (`pm25`, `pm10`, `no2`).
- averaging_code: Metric basis (`hourly_mean`, `rolling_24h_mean`).
- index_level: Numeric AQI level within the standard.
- index_label: Optional label for the level.
- index_band: Band label/group for the level.
- color_hex: Optional display color.
- range_low: Stored threshold start value used for ordered upper-bound lookup.
- range_high: Inclusive upper concentration bound (null = open-ended).
- uom: Unit of measure.
- valid_from: Date breakpoint row becomes effective.
- valid_to: Optional end date.
- created_at: Row creation timestamp.

## Notes
- Primary key is `(standard_code, version_code, pollutant_code, averaging_code, index_level)`.
- Used by AQI worker/runtime lookup instead of hardcoded thresholds.
- Lookup is an ordered upper-bound cascade:
  - values below the first `range_low` return null
  - first row means `x <= range_high`
  - later rows mean `x > previous range_high and x <= current range_high`
  - open-ended final row means `x > previous range_high`
- PM2.5 examples:
  - EAQI hourly: `Good <=5`, `Fair >5 to <=15`, `Moderate >15 to <=50`, `Poor >50 to <=90`, `Very poor >90 to <=140`, `Extremely poor >140`
  - DAQI rolling 24h mean: `1 <=11`, `2 >11 to <=23`, `3 >23 to <=35`, `4 >35 to <=41`, `5 >41 to <=47`, `6 >47 to <=53`, `7 >53 to <=58`, `8 >58 to <=64`, `9 >64 to <=70`, `10 >70`
- PM10 examples:
  - EAQI hourly: `Good <=15`, `Fair >15 to <=45`, `Moderate >45 to <=120`, `Poor >120 to <=195`, `Very poor >195 to <=270`, `Extremely poor >270`
  - DAQI rolling 24h mean: `1 <=16`, `2 >16 to <=33`, `3 >33 to <=50`, `4 >50 to <=58`, `5 >58 to <=66`, `6 >66 to <=75`, `7 >75 to <=83`, `8 >83 to <=91`, `9 >91 to <=100`, `10 >100`
- NO2 examples:
  - EAQI hourly: `Good <=10`, `Fair >10 to <=25`, `Moderate >25 to <=60`, `Poor >60 to <=100`, `Very poor >100 to <=150`, `Extremely poor >150`
  - DAQI hourly mean: `1 <=67`, `2 >67 to <=134`, `3 >134 to <=200`, `4 >200 to <=267`, `5 >267 to <=334`, `6 >334 to <=400`, `7 >400 to <=467`, `8 >467 to <=534`, `9 >534 to <=600`, `10 >600`
