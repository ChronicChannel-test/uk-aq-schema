# Public network contract v2

`uk_aq_core.networks` is the canonical network catalogue. A station's public
network identity is the single relationship
`uk_aq_core.stations.network_id -> uk_aq_core.networks.id`.

The public catalogue is exposed by `uk_aq_public.uk_aq_public_networks` and
`/api/aq/networks` with `contract_version: 2`. It contains only rows where
`public_display_enabled = true`, and each row includes `network_id`,
`network_code`, `network_label`, and `network_type`. Valid `network_type` values
are `official`, `community`, and `aggregator`.

Station, latest, chart, local-authority, and parliamentary-constituency rows use
scalar `network_id`, `network_code`, and `network_label`. They do not repeat
`network_type`; that field belongs to the catalogue. Connector identifiers and
labels remain separate source-provenance metadata and never determine public
network identity. Public connector filter parameters (`connector`,
`connector_id`, and `connector_code`) are rejected with HTTP 400.

Only enabled networks appear in public views and RPCs. OpenAQ remains absent
while its catalogue row is disabled. Breathe London Nodes and Breathe London
Communities remain separate connectors, but their stations reference the same
public network with code `breathelondon` and label `Breathe London`.

The Phase 8 destructive migration verifies that both retired network relations
are absent. No compatibility views, membership arrays, connector-derived
fallbacks, or public code remapping are part of v2.
