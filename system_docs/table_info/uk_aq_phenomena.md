# phenomena

Defines what is measured (pollutant/parameter) within a connector.

## Fields
- id: Internal bigint primary key (generated identity).
- label: Human-readable phenomenon name.
- source_label: Source identifier for the connector's phenomenon entry (e.g. legacy Eionet URI or `connector_code:pollutant_label`).
- notation: Optional short code/notation from the source.
- pollutant_label: Optional emissions-inventory pollutant label (from reference list).
- observed_property_id: Optional FK to canonical `observed_properties.id`.
- connector_id: FK to `connectors.id`.

## Notes
- `eionet_uri` was renamed to `source_label`.
- Uniqueness is enforced on `(connector_id, source_label)`.
- `notation` and `pollutant_label` are retained for compatibility and migration support; canonical API logic should prefer `observed_properties.code` via `observed_property_id`.
- Deferred drop plan:
  - Keep `notation` and `pollutant_label` until all ingest writers always pass canonical `observed_property_code` and all read paths stop using these fields.
  - Keep `eionet_uri` aliases in RPC/view payloads until all API consumers switch to `source_label`/`observed_property_*`.
