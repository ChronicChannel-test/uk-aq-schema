# aqi_standard_versions

Reference versions for AQI standards used by AggDaily outputs.

## Fields
- standard_code: AQI standard key (`daqi` or `eaqi`).
- version_code: Version identifier within a standard.
- source_name: Human-readable source name for the standard/version.
- source_url: Optional source URL.
- notes: Optional implementation notes.
- valid_from: Date this version becomes effective.
- valid_to: Optional end date for validity.
- is_active: Flag used to mark active versions.
- created_at: Row creation timestamp.

## Notes
- Primary key is `(standard_code, version_code)`.
- Breakpoint rows reference this table via FK.
