from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RPC = (ROOT / "schemas/ingest_db/uk_aq_rpc.sql").read_text()
VALIDATION = (
    ROOT / "schemas/ingest_db/uk_aq_phenomena_mapping_rpc_validation.sql"
).read_text()


def test_rpc_returns_mapping_diagnostics() -> None:
    required = (
        "p_allow_mapping_upsert boolean default false",
        "phenomenon_id bigint",
        "observed_property_id bigint",
        "observed_property_code text",
        "mapping_kind text",
        "is_aqi_eligible boolean",
        "mapping_status text",
        "mapping_warning text",
    )
    for fragment in required:
        assert fragment in RPC


def test_rpc_uses_authoritative_mapping_and_strict_guards() -> None:
    required = (
        "from uk_aq_core.observed_property_mappings m",
        "mapping_kind conflicts with authoritative mapping",
        "new explicit mapping requires p_allow_mapping_upsert=true",
        "raw observed-property mapping cannot use source_uom",
        "raw observed-property mapping cannot use pollutant_label",
        "AQI eligibility requires raw pm25, pm10, or no2 mapping",
        "unknown canonical observed_property_code",
        "duplicate phenomenon mapping key in request",
    )
    for fragment in required:
        assert fragment in RPC


def test_rpc_applies_null_mapping_to_derived_phenomena() -> None:
    assert "observed_property_id = excluded.observed_property_id" in RPC
    assert "coalesce(excluded.observed_property_id" not in RPC[
        RPC.index("create or replace function uk_aq_public.uk_aq_rpc_phenomena_upsert") :
        RPC.index("create or replace function uk_aq_public.uk_aq_rpc_phenomena_ids")
    ]


def test_rpc_reconciles_transitional_direct_timeseries_link_setwise() -> None:
    assert "and column_name = 'phenomenon_id'" in RPC
    assert "and column_name = 'observed_property_id'" in RPC
    assert "and observed_property_id is distinct from $1" in RPC


def test_transactional_validation_covers_raw_index_and_rejections() -> None:
    required = (
        "breathelondon_nodes:pm2.5",
        "breathelondon_nodes:pm2.5:daqi",
        "DAQI-as-raw input was not rejected",
        "authoritative mapping conflict was not rejected",
        "administrative mapping idempotency failed",
        "unknown canonical code was not rejected",
        "duplicate request key was not rejected",
        "mapping_warning <> 'unknown_source_label'",
        "rollback;",
    )
    for fragment in required:
        assert fragment in VALIDATION
