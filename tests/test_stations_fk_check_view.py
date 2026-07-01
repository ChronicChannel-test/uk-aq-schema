from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MAIN = (ROOT / "schemas/obs_aqi_db/uk_aq_obs_aqi_db_schema.sql").read_text()
PATCH = (ROOT / "schemas/obs_aqi_db/uk_aq_stations_fk_check_view.sql").read_text()


def test_station_fk_check_view_is_id_only_and_service_role_readable() -> None:
    expected = """create or replace view uk_aq_public.stations_fk_check as
select
  id
from uk_aq_core.stations;"""
    for sql in (MAIN, PATCH):
        assert expected in sql
        assert "grant select on uk_aq_public.stations_fk_check to service_role;" in sql
        assert "comment on view uk_aq_public.stations_fk_check" in sql
