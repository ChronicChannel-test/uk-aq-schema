from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
RPC_FILE = ROOT / "schemas" / "ingest_db" / "uk_aq_rpc.sql"

def test_timeseries_upsert_derives_observed_property() -> None:
    source = RPC_FILE.read_text(encoding="utf-8")
    
    # Check that the function exists
    assert "create or replace function uk_aq_public.uk_aq_rpc_timeseries_upsert(rows jsonb)" in source

    # Check for the dynamic column existence check
    assert "information_schema.columns" in source
    assert "observed_property_id" in source
    
    # Check that it derives the property id from phenomena when using the dynamic path
    assert "left join uk_aq_core.phenomena p on p.id = r.phenomenon_id" in source.lower()
    
    # Ensure coalesce is used to prefer provided observed_property_id if given
    assert "coalesce(r.observed_property_id, p.observed_property_id)" in source.lower()
    
    # Make sure we check has_op_id before executing the dynamic SQL
    assert "if has_op_id then" in source.lower()
