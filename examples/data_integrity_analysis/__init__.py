# data_integrity_analysis/__init__.py

"""
Data Integrity Analysis package for Equity Aggregator.

This package provides comprehensive data quality analysis for canonical equity
records, examining financial outliers, temporal anomalies, data consistency,
identifier quality, and completeness metrics.

Usage:
    python -m examples.data_integrity_analysis
"""

from .main import main, run_analysis
from .models import AnalysisSettings, Finding, SectionReport, default_settings

__all__ = [
    "AnalysisSettings",
    "Finding",
    "SectionReport",
    "default_settings",
    "main",
    "run_analysis",
]
