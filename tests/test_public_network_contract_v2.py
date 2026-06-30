from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PHASE1 = (ROOT / "schemas/ingest_db/uk_aq_network_public_contract_phase1.sql").read_text()
PHASE2 = (ROOT / "schemas/ingest_db/uk_aq_network_public_contract_phase2.sql").read_text()
RPC = (ROOT / "schemas/ingest_db/uk_aq_rpc.sql").read_text()
DROP = (
    ROOT
    / "schemas/migrations/v0.2.0/ingestdb/011_remove_legacy_network_relations.sql"
).read_text()


def test_catalog_network_types_and_visibility_are_constrained() -> None:
    assert "network_type in ('official', 'community', 'aggregator')" in PHASE1
    assert "where n.public_display_enabled is true" in PHASE1
    assert "('openaq', 'aggregator')" in PHASE1
    assert "('breathelondon', 'community')" in PHASE1


def test_public_rows_use_scalar_network_identity() -> None:
    for field in ("network_id", "network_code", "network_label"):
        assert field in RPC
    assert "pg_get_function_result(p.oid) like '%network_type%'" in PHASE2
    assert "public_display_enabled = true" in RPC


def test_legacy_relations_are_validated_absent() -> None:
    assert "to_regclass('uk_aq_core.station_network_memberships') is not null" in DROP
    assert "to_regclass('uk_aq_core.uk_aq_networks') is not null" in DROP
    first = DROP.index("drop table if exists uk_aq_core.station_network_memberships")
    second = DROP.index("drop table if exists uk_aq_core.uk_aq_networks")
    assert first < second
