# observed_property_mappings

Connector-aware policy for mapping source phenomena to canonical observed
properties.

## Fields

- `id`: Internal bigint identity primary key.
- `connector_id`: Required source connector.
- `source_label`: Stable connector-specific phenomenon label.
- `notation`: Optional provider notation retained for audit.
- `pollutant_label`: Optional provider pollutant label retained for audit.
- `source_uom`: Optional source unit used by mapping safety checks.
- `observed_property_id`: Canonical observed-property ID for mappable rows.
- `observed_property_code`: Canonical code paired with
  `observed_property_id`.
- `mapping_kind`: One of `raw_observed_property`, `derived_index`,
  `derived_statistic`, `meteorological`, `unknown`, or `ignored`.
- `is_aqi_eligible`: True only for raw `pm25`, `pm10`, or `no2` mappings.
- `is_active`: Whether the mapping is available to the central mapping path.
- `confidence`: `explicit`, `inferred`, or `legacy_backfill`.
- `notes`: Optional mapping rationale.
- `created_at`, `updated_at`: Audit timestamps.

## Integrity rules

- `(connector_id, source_label)` is unique.
- The canonical ID and code form a composite foreign key, preventing ID/code
  drift.
- Raw and meteorological mappings require a canonical property.
- Derived, unknown, and ignored mappings must remain canonically unmapped.
- Raw mappings cannot use `DAQI`/`index` units or a `daqi_*` pollutant label.
- AQI eligibility is restricted to raw PM2.5, PM10, and NO2.

## Initial Breathe London Nodes policy

| Source label | Mapping kind | Canonical code | AQI eligible |
|---|---|---|---|
| `breathelondon_nodes:pm2.5` | `raw_observed_property` | `pm25` | true |
| `breathelondon_nodes:no2` | `raw_observed_property` | `no2` | true |
| `breathelondon_nodes:pm2.5:daqi` | `derived_index` | none | false |
| `breathelondon_nodes:no2:daqi` | `derived_index` | none | false |

Phase 1 only creates mapping policy. The hardened central phenomena RPC applies
the policy to `phenomena.observed_property_id` in the next phase.
