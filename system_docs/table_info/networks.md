# networks

Canonical network catalog used by stations and public website/API filtering.

## Fields

- `id`: Internal bigint primary key.
- `network_code`: Stable internal/public filter code.
- `display_name`: Curated public name, exposed as `network_label`.
- `network_type`: Required text value constrained to `official`, `community`,
  or `aggregator`.
- `ingest_enabled`: Whether ingest for the network is enabled.
- `public_display_enabled`: Whether the network and its stations may appear in
  public API and website responses.
- `default_priority`: Default station-selection priority.
- `metadata`: Network-specific JSON metadata.
- `created_at`: Row creation timestamp.
- `updated_at`: Last update timestamp.

## Current seed values

| network_code | network_type | public on TEST |
| --- | --- | --- |
| `gov_uk_aurn` | `official` | yes |
| `breathelondon` | `community` | yes |
| `openaq` | `aggregator` | no |
| `sensorcommunity` | `community` | no |
| `laqn` | `official` | no |

## Public contract

`uk_aq_public.networks` exposes only rows where
`public_display_enabled = true`. It exposes `network_type`; station/latest
payloads intentionally do not.
