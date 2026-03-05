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
- range_low: Inclusive lower concentration bound.
- range_high: Inclusive upper concentration bound (null = open-ended).
- uom: Unit of measure.
- valid_from: Date breakpoint row becomes effective.
- valid_to: Optional end date.
- created_at: Row creation timestamp.

## Notes
- Primary key is `(standard_code, version_code, pollutant_code, averaging_code, index_level)`.
- Used by AQI worker/runtime lookup instead of hardcoded thresholds.
