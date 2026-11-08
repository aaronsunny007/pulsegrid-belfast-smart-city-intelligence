"""City configuration and the names of every table and path the pipeline uses."""

from __future__ import annotations

from dataclasses import dataclass
from importlib import resources

import yaml


@dataclass(frozen=True)
class RiskBands:
    stable_min: float
    moderate_min: float

    def label(self, score: float | None) -> str:
        if score is None:
            return "Insufficient data"
        if score >= self.stable_min:
            return "Stable"
        if score >= self.moderate_min:
            return "Moderate Risk"
        return "High Risk"


@dataclass(frozen=True)
class CityConfig:
    city: str
    display_name: str
    timezone: str
    source: str
    modes: tuple[str, ...]
    poll_interval_minutes: int
    max_gap_minutes: int
    service_day_start_hour: int
    min_observed_minutes: int
    risk_bands: RiskBands

    @classmethod
    def from_dict(cls, raw: dict) -> CityConfig:
        bands = raw["risk_bands"]
        cfg = cls(
            city=raw["city"],
            display_name=raw["display_name"],
            timezone=raw["timezone"],
            source=raw["source"],
            modes=tuple(raw["modes"]),
            poll_interval_minutes=int(raw["poll_interval_minutes"]),
            max_gap_minutes=int(raw["max_gap_minutes"]),
            service_day_start_hour=int(raw["service_day_start_hour"]),
            min_observed_minutes=int(raw["min_observed_minutes"]),
            risk_bands=RiskBands(float(bands["stable_min"]), float(bands["moderate_min"])),
        )
        cfg.validate()
        return cfg

    def validate(self) -> None:
        if not self.modes:
            raise ValueError("at least one mode is required")
        if self.poll_interval_minutes <= 0:
            raise ValueError("poll_interval_minutes must be positive")
        if self.max_gap_minutes < self.poll_interval_minutes:
            raise ValueError("max_gap_minutes must be >= poll_interval_minutes")
        if not 0 <= self.service_day_start_hour <= 23:
            raise ValueError("service_day_start_hour must be between 0 and 23")
        if self.risk_bands.moderate_min > self.risk_bands.stable_min:
            raise ValueError("risk_bands.moderate_min must be <= stable_min")


def load_city(name: str) -> CityConfig:
    """Load a packaged city config, e.g. ``load_city("london")``."""
    path = resources.files("pulsecity.cities").joinpath(f"{name}.yml")
    if not path.is_file():
        raise ValueError(f"unknown city '{name}'")
    return CityConfig.from_dict(yaml.safe_load(path.read_text()))


@dataclass(frozen=True)
class Tables:
    """Fully qualified table names and storage paths.

    On Databricks ``catalog`` is a Unity Catalog name and names are ``catalog.schema.table``.
    Locally (tests and the demo) ``catalog`` is ``None`` and names are ``schema.table``.
    """

    catalog: str | None
    landing_root: str
    checkpoint_root: str

    @classmethod
    def for_databricks(cls, catalog: str) -> Tables:
        return cls(
            catalog=catalog,
            landing_root=f"/Volumes/{catalog}/landing/raw",
            checkpoint_root=f"/Volumes/{catalog}/ops/checkpoints",
        )

    def name(self, schema: str, table: str) -> str:
        return f"{self.catalog}.{schema}.{table}" if self.catalog else f"{schema}.{table}"

    def schema(self, schema: str) -> str:
        return f"{self.catalog}.{schema}" if self.catalog else schema

    # Bronze
    @property
    def bronze_line_status(self) -> str:
        return self.name("bronze", "line_status_raw")

    # Silver
    @property
    def silver_line_status(self) -> str:
        return self.name("silver", "line_status")

    @property
    def silver_quarantine(self) -> str:
        return self.name("silver", "quarantine")

    # Gold
    @property
    def dim_line(self) -> str:
        return self.name("gold", "dim_line")

    @property
    def dim_status_severity(self) -> str:
        return self.name("gold", "dim_status_severity")

    @property
    def dim_date(self) -> str:
        return self.name("gold", "dim_date")

    @property
    def fact_line_status(self) -> str:
        return self.name("gold", "fact_line_status")

    @property
    def fact_disruption_incident(self) -> str:
        return self.name("gold", "fact_disruption_incident")

    @property
    def agg_line_reliability_daily(self) -> str:
        return self.name("gold", "agg_line_reliability_daily")

    @property
    def agg_network_health_hourly(self) -> str:
        return self.name("gold", "agg_network_health_hourly")

    @property
    def agg_disruption_risk_hourly(self) -> str:
        return self.name("gold", "agg_disruption_risk_hourly")

    @property
    def line_status_current(self) -> str:
        return self.name("gold", "line_status_current")

    # Ops
    @property
    def run_log(self) -> str:
        return self.name("ops", "run_log")

    def landing_dir(self, city: str, dataset: str) -> str:
        return f"{self.landing_root}/{city}/{dataset}"

    def checkpoint(self, name: str) -> str:
        return f"{self.checkpoint_root}/{name}"
