from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SQL = (
    ROOT
    / "schemas/ingest_db/uk_aq_observed_property_mappings_phase5_seed.sql"
).read_text()


def test_phase5_seed_preserves_existing_canonical_relationships() -> None:
    assert "join observed_properties op" in SQL
    assert "op.id" in SQL
    assert "op.code" in SQL
    assert "op.domain = 'met'" in SQL
    assert "'meteorological'" in SQL
    assert "'raw_observed_property'" in SQL


def test_phase5_seed_excludes_unstable_or_phase3_rows() -> None:
    assert "p.source_label is not null" in SQL
    assert "c.connector_code <> 'blondon_nodes'" in SQL
    assert "op.code in ('pm25', 'pm10', 'no2')" in SQL
