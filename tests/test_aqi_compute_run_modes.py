import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MAIN_SCHEMA = ROOT / "schemas/obs_aqi_db/uk_aq_obs_aqi_db_schema.sql"
TARGETED_SQL = ROOT / "schemas/obs_aqi_db/uk_aq_aqi_compute_run_modes.sql"
EXPECTED_MODES = {
    "sync_hourly",
    "backfill",
    "fast",
    "reconcile_short",
    "reconcile_deep",
    "reconcile_deep_rolling",
}


def quoted_values(sql_fragment: str) -> set[str]:
    return set(re.findall(r"'([^']+)'", sql_fragment))


class AqiComputeRunModeSchemaTests(unittest.TestCase):
    def test_main_schema_constraint_accepts_all_run_modes(self) -> None:
        sql = MAIN_SCHEMA.read_text(encoding="utf-8")
        match = re.search(
            r"constraint\s+aqi_compute_runs_run_mode_check\s+"
            r"check\s*\(\s*run_mode\s+in\s*\(([^)]+)\)",
            sql,
            flags=re.IGNORECASE,
        )
        self.assertIsNotNone(match)
        self.assertTrue(EXPECTED_MODES.issubset(quoted_values(match.group(1))))

    def test_main_schema_rpc_accepts_all_run_modes(self) -> None:
        sql = MAIN_SCHEMA.read_text(encoding="utf-8")
        function = sql.split(
            "create or replace function uk_aq_public.uk_aq_rpc_aqi_compute_run_log",
            1,
        )[1]
        allowlist = re.search(
            r"p_run_mode.*?not\s+in\s*\(([^)]+)\)",
            function,
            flags=re.IGNORECASE | re.DOTALL,
        )
        self.assertIsNotNone(allowlist)
        self.assertTrue(EXPECTED_MODES.issubset(quoted_values(allowlist.group(1))))

    def test_targeted_apply_updates_constraint_and_rpc(self) -> None:
        sql = TARGETED_SQL.read_text(encoding="utf-8")
        self.assertIn("drop constraint if exists aqi_compute_runs_run_mode_check", sql)
        self.assertIn("validate constraint aqi_compute_runs_run_mode_check", sql)
        self.assertGreaterEqual(sql.count("'reconcile_deep_rolling'"), 2)
        for mode in EXPECTED_MODES:
            self.assertIn(f"'{mode}'", sql)


if __name__ == "__main__":
    unittest.main()
