from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MAIN = (ROOT / "schemas/ingest_db/uk_aq_core_schema.sql").read_text()
PATCH = (ROOT / "schemas/ingest_db/uk_aq_observed_property_mappings.sql").read_text()
V020_CORE = (ROOT / "schemas/v0.2.0/001_core_schema.sql").read_text()
V020_SEED = (ROOT / "schemas/v0.2.0/006_seed_core.sql").read_text()
V020_SECURITY = (ROOT / "schemas/v0.2.0/007_security.sql").read_text()


def test_mapping_table_contract_is_in_main_and_focused_sql() -> None:
    required = (
        "create table if not exists observed_property_mappings",
        "unique (connector_id, source_label)",
        "foreign key (observed_property_id, observed_property_code)",
        "references observed_properties(id, code)",
        "observed_property_mappings_property_presence_check",
        "observed_property_mappings_aqi_eligibility_check",
        "observed_property_mappings_raw_uom_check",
        "observed_property_mappings_raw_pollutant_label_check",
    )
    for sql in (MAIN, PATCH):
        for fragment in required:
            assert fragment in sql


def test_blondon_nodes_raw_and_index_mappings_are_seeded() -> None:
    required = (
        "'breathelondon_nodes:pm2.5'",
        "'breathelondon_nodes:no2'",
        "'breathelondon_nodes:pm2.5:daqi'",
        "'breathelondon_nodes:no2:daqi'",
        "'raw_observed_property'",
        "'derived_index'",
    )
    for sql in (MAIN, PATCH):
        for fragment in required:
            assert fragment in sql


def test_mapping_table_is_in_main_rls_table_list() -> None:
    assert "'observed_property_mappings'" in MAIN
    assert "alter table observed_property_mappings enable row level security;" in PATCH


def test_v020_schema_seed_and_security_include_mapping_contract() -> None:
    assert "create table if not exists observed_property_mappings" in V020_CORE
    assert "'breathelondon_nodes:pm2.5'" in V020_SEED
    assert "'breathelondon_nodes:pm2.5:daqi'" in V020_SEED
    assert "'observed_property_mappings'" in V020_SECURITY
