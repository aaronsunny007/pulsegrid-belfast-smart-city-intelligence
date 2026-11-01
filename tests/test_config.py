import dataclasses

import pytest

from pulsecity.config import CityConfig, RiskBands, Tables, load_city


def test_london_config_loads():
    cfg = load_city("london")
    assert cfg.timezone == "Europe/London"
    assert "tube" in cfg.modes
    assert cfg.max_gap_minutes >= cfg.poll_interval_minutes


def test_unknown_city_is_rejected():
    with pytest.raises(ValueError, match="unknown city"):
        load_city("atlantis")


def test_invalid_config_is_rejected(london):
    bad = dataclasses.replace(london, max_gap_minutes=1)
    with pytest.raises(ValueError, match="max_gap_minutes"):
        bad.validate()


@pytest.mark.parametrize(
    ("score", "label"),
    [
        (None, "Insufficient data"),
        (99.0, "Stable"),
        (98.0, "Stable"),
        (96.0, "Moderate Risk"),
        (94.9, "High Risk"),
    ],
)
def test_risk_bands(score, label):
    assert RiskBands(stable_min=98.0, moderate_min=95.0).label(score) == label


def test_table_names_on_databricks_use_unity_catalog_three_part_names():
    t = Tables.for_databricks("pulsecity")
    assert t.silver_line_status == "pulsecity.silver.line_status"
    assert (
        t.landing_dir("london", "line_status")
        == "/Volumes/pulsecity/landing/raw/london/line_status"
    )


def test_table_names_locally_have_no_catalog():
    t = Tables(catalog=None, landing_root="/tmp/l", checkpoint_root="/tmp/c")
    assert t.fact_line_status == "gold.fact_line_status"


def test_from_dict_round_trip(london):
    raw = {
        "city": "x",
        "display_name": "X",
        "timezone": "UTC",
        "source": "tfl",
        "modes": ["tube"],
        "poll_interval_minutes": 5,
        "max_gap_minutes": 15,
        "service_day_start_hour": 4,
        "min_observed_minutes": 60,
        "risk_bands": {"stable_min": 98, "moderate_min": 95},
    }
    assert CityConfig.from_dict(raw).modes == ("tube",)
