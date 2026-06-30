# uk_air_sos_networks

Lookup table for network labels from the UK-AIR monitoring sites register.

## Fields
- network_ref: Exact network label from the CSV (primary key).
- network_code: Internal normalized code mapped to the canonical network catalog.
- network_display_name: UI-friendly display name (defaults to the CSV label).
- created_at: Row creation timestamp (default now()).
- updated_at: Row update timestamp (default now()).

## Notes
- Keep `network_ref` stable; it is the authoritative reference from the register.
- Keep `network_code` aligned with `uk_aq_core.networks.network_code`.
- `network_display_name` is preserved during `scripts/uk_air_sos/uk_air_sos_site_register.py --load` (new networks default to the CSV label).
