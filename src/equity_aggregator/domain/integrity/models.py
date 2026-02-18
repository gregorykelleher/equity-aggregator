# integrity/models.py

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class AnalysisSettings:
    """
    Configuration values controlling analysis thresholds.
    """

    # Minimum equities needed before statistical analysis is meaningful
    min_sample_size: int = 10
    # Market cap above $200B is considered mega-cap
    mega_cap_threshold: int = 200_000_000_000
    # Market cap below $300M is considered micro-cap
    micro_cap_threshold: int = 300_000_000
    # Currencies appearing fewer than this many times are flagged as rare
    rare_currency_count: int = 10
    # Percentage of round-dollar prices that triggers a clustering warning
    round_price_threshold: float = 30.0
    # Percentage of missing identifiers that triggers an alert
    identifier_gap_alert: float = 50.0
    # Symbols longer than this are flagged as unusually long
    symbol_length_limit: int = 5
    # Maximum duplicate name groups shown in findings
    duplicate_group_limit: int = 3
    # Maximum sample equities shown per finding
    finding_sample_limit: int = 5
    # Dividend yield above 15% is flagged as extreme
    dividend_yield_alert: Decimal = Decimal("15")
    # Profit margin above 100% is flagged as extreme
    profit_margin_high: Decimal = Decimal("100")
    # Profit margin below -100% is flagged as extreme
    profit_margin_low: Decimal = Decimal("-100")
    # Price exceeding 52-week max by more than 10% is flagged
    price_tolerance: Decimal = Decimal("1.1")
    # Price falling below 90% of 52-week min is flagged
    price_to_min_factor: Decimal = Decimal("0.9")
    # Expected ISIN length per ISO 6166
    isin_length: int = 12
    # Expected CUSIP length per CUSIP standard
    cusip_length: int = 9
    # Expected LEI length per ISO 17442
    lei_length: int = 20
    # Prices below $0.01 are flagged as penny stocks
    penny_stock_threshold: Decimal = Decimal("0.01")


def default_settings() -> AnalysisSettings:
    """
    Return default analysis thresholds.

    Returns:
        AnalysisSettings: Default configuration values.
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
