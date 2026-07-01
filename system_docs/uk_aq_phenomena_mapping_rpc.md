# Phenomena mapping RPC

`uk_aq_public.uk_aq_rpc_phenomena_upsert` is the central service-role write path
for connector phenomena and canonical observed-property mapping.

## Default ingest mode

Call the RPC with `rows` and omit `p_allow_mapping_upsert` or set it to false.
The RPC treats `(connector_id, source_label)` policy in
`uk_aq_core.observed_property_mappings` as authoritative.

- Known mappings set or clear `phenomena.observed_property_id` according to the
  active mapping policy.
- When the transitional timeseries columns are present, the RPC reconciles all
  linked `timeseries.observed_property_id` values with one set-based,
  write-only update. No timeseries metadata is returned to the caller.
- Optional caller-supplied mapping fields must agree with existing policy.
- An unclassified source label is recorded as `unknown`, remains canonically
  unmapped, and returns `mapping_warning = unknown_source_label`.
- A caller cannot introduce an explicit mapping in default mode.

## Administrative mapping mode

Set `p_allow_mapping_upsert = true` only for controlled migrations or connector
metadata registration. The request must include `mapping_kind`.

- Raw and meteorological mappings require an existing canonical
  `observed_property_code`.
- Derived, unknown, and ignored mappings must not provide a canonical code.
- Raw `DAQI`/`index` units and `daqi_*` pollutant labels are rejected.
- AQI eligibility is restricted to raw `pm25`, `pm10`, and `no2`.
- Duplicate connector/source keys in one request are rejected.

The RPC is service-role only. Administrative mode does not grant additional
database privileges; it makes mapping mutation explicit and auditable at the
call site.

## Return contract

One row is returned per input phenomenon:

- `connector_id`
- `source_label`
- `phenomenon_id`
- `observed_property_id`
- `observed_property_code`
- `mapping_kind`
- `is_aqi_eligible`
- `mapping_status`
- `mapping_warning`

## Validation

Run `schemas/ingest_db/uk_aq_phenomena_mapping_rpc_validation.sql` after applying
the RPC. It exercises raw PM2.5, derived DAQI, dangerous-input rejection,
authoritative conflict rejection, and unknown-label diagnostics inside a
transaction that always rolls back.
