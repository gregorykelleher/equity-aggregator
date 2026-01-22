# data_integrity_analysis/models.py

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class AnalysisSettings:
    """
    Configuration values controlling analysis thresholds.
    """

    min_sample_size: int = 10
    mega_cap_threshold: int = 200_000_000_000
    micro_cap_threshold: int = 300_000_000
    rare_currency_count: int = 10
    round_price_threshold: float = 30.0
    identifier_gap_alert: float = 50.0
    symbol_length_limit: int = 5
    duplicate_group_limit: int = 3
    finding_sample_limit: int = 5
    dividend_yield_alert: Decimal = Decimal("15")
    profit_margin_high: Decimal = Decimal("100")
    profit_margin_low: Decimal = Decimal("-100")
    price_tolerance: Decimal = Decimal("1.1")
    price_to_min_factor: Decimal = Decimal("0.9")
    isin_length: int = 12
    cusip_length: int = 9
    lei_length: int = 20
    penny_stock_threshold: Decimal = Decimal("0.01")


def default_settings() -> AnalysisSettings:
    """
    Return default analysis thresholds.

    Returns:
        Default configuration values.
    """
    return AnalysisSettings()


@dataclass(frozen=True)
class Finding:
    """
    Captures a single insight or anomaly.
    """

    message: str
    highlights: tuple[str, ...] = ()


@dataclass(frozen=True)
class SectionReport:
    """
    Structured results for an analysis section.
    """

    title: str
    findings: tuple[Finding, ...]
