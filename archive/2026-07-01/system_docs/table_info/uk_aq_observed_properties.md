# observed_properties

Canonical observed-property catalog shared across connectors.

## Fields
- id: Internal bigint primary key (generated identity).
- code: Canonical key (for example `pm25`, `no2`, `temperature`), unique and non-null.
- display_name: UI/display label for the canonical property.
- domain: Canonical property domain (`aq` or `met`).
- canonical_uom: Optional canonical unit for the property.
- created_at: Row creation timestamp.
- updated_at: Row update timestamp.

## Notes
- Connector-specific source mappings stay in `phenomena`.
- `phenomena.observed_property_id` links each source-specific phenomenon to a canonical property.
- API filtering and pollutant classification should prefer `observed_properties.code` where available.
