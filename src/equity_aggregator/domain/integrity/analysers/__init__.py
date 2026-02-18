# integrity/analysers/__init__.py

from ._helpers import build_format_finding
from .cross_field_logic import (
    analyse_cross_field_logic,
    detect_cap_without_price,
    detect_missing_price_and_cap,
    detect_partial_range,
    detect_price_without_cap,
)
from .currency_and_geography import (
    analyse_currency_and_geography,
    currency_distribution,
    geography_proxies,
)
from .data_consistency import (
    _collect_duplicate_name_groups as _collect_duplicate_name_groups,
)
from .data_consistency import (
    analyse_currency_rarity,
    analyse_data_consistency,
    analyse_duplicate_names,
    analyse_symbol_patterns,
)
from .data_quality import (
    analyse_data_quality,
    identity_completeness,
    top_complete_profiles,
    valuation_coverage,
)
from .dataset_overview import build_dataset_overview
from .extreme_financial_values import (
    detect_extreme_dividends,
    detect_extreme_financial_values,
    detect_negative_price_to_book,
    detect_penny_stocks,
    detect_profit_margin_extremes,
    detect_round_price_clusters,
)
from .financial_outliers import (
    analyse_financial_outliers,
    compute_market_cap_findings,
    compute_negative_metric_findings,
    compute_pe_findings,
    compute_price_range_findings,
)
from .identifier_quality import (
    analyse_identifier_quality,
    missing_identifier_counts,
    validate_identifier_formats,
)
from .temporal_anomalies import (
    detect_price_below_min,
    detect_range_inversions,
    detect_stale_range_data,
    detect_temporal_anomalies,
)

__all__ = [
    "build_format_finding",
    "_collect_duplicate_name_groups",
    "analyse_cross_field_logic",
    "analyse_currency_and_geography",
    "analyse_currency_rarity",
    "analyse_data_consistency",
    "analyse_data_quality",
    "analyse_duplicate_names",
    "analyse_financial_outliers",
    "analyse_identifier_quality",
    "analyse_symbol_patterns",
    "build_dataset_overview",
    "compute_market_cap_findings",
    "compute_negative_metric_findings",
    "compute_pe_findings",
    "compute_price_range_findings",
    "currency_distribution",
    "detect_cap_without_price",
    "detect_extreme_dividends",
    "detect_extreme_financial_values",
    "detect_missing_price_and_cap",
    "detect_negative_price_to_book",
    "detect_partial_range",
    "detect_penny_stocks",
    "detect_price_below_min",
    "detect_price_without_cap",
    "detect_profit_margin_extremes",
    "detect_range_inversions",
    "detect_round_price_clusters",
    "detect_stale_range_data",
    "detect_temporal_anomalies",
    "geography_proxies",
    "identity_completeness",
    "missing_identifier_counts",
    "top_complete_profiles",
    "validate_identifier_formats",
    "valuation_coverage",
]
