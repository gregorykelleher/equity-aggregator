# cli/test_config.py

import argparse

import pytest

from equity_aggregator.cli.config import determine_log_level

pytestmark = pytest.mark.unit


def test_determine_log_level_debug_flag() -> None:
    """
    ARRANGE: args with debug=True
    ACT:     determine_log_level
    ASSERT:  returns "debug"
    """
    args = argparse.Namespace(debug=True, verbose=False, quiet=False)

    actual = determine_log_level(args)

    assert actual == "debug"


def test_determine_log_level_verbose_flag() -> None:
    """
    ARRANGE: args with verbose=True
    ACT:     determine_log_level
    ASSERT:  returns "development"
    """
    args = argparse.Namespace(debug=False, verbose=True, quiet=False)

    actual = determine_log_level(args)

    assert actual == "development"


def test_determine_log_level_quiet_flag() -> None:
    """
    ARRANGE: args with quiet=True
    ACT:     determine_log_level
    ASSERT:  returns "production"
    """
    args = argparse.Namespace(debug=False, verbose=False, quiet=True)

    actual = determine_log_level(args)

    assert actual == "production"


def test_determine_log_level_no_flags() -> None:
    """
    ARRANGE: args with all flags False
    ACT:     determine_log_level
    ASSERT:  returns None
    """
    args = argparse.Namespace(debug=False, verbose=False, quiet=False)

    actual = determine_log_level(args)

    assert actual is None


def test_determine_log_level_debug_priority_over_verbose() -> None:
    """
    ARRANGE: args with debug=True, verbose=True
    ACT:     determine_log_level
    ASSERT:  returns "debug" (debug has priority)
    """
    args = argparse.Namespace(debug=True, verbose=True, quiet=False)

    actual = determine_log_level(args)

    assert actual == "debug"


def test_determine_log_level_debug_priority_over_quiet() -> None:
    """
    ARRANGE: args with debug=True, quiet=True
    ACT:     determine_log_level
    ASSERT:  returns "debug" (debug has priority)
    """
    args = argparse.Namespace(debug=True, verbose=False, quiet=True)

    actual = determine_log_level(args)

    assert actual == "debug"


def test_determine_log_level_verbose_priority_over_quiet() -> None:
    """
    ARRANGE: args with verbose=True, quiet=True
    ACT:     determine_log_level
    ASSERT:  returns "development" (verbose has priority)
    """
    args = argparse.Namespace(debug=False, verbose=True, quiet=True)

    actual = determine_log_level(args)

    assert actual == "development"


def test_determine_log_level_all_flags_true() -> None:
    """
    ARRANGE: args with all flags True
    ACT:     determine_log_level
    ASSERT:  returns "debug" (debug has highest priority)
    """
    args = argparse.Namespace(debug=True, verbose=True, quiet=True)

    actual = determine_log_level(args)

    assert actual == "debug"


def test_determine_log_level_missing_attributes() -> None:
    """
    ARRANGE: empty args namespace (missing attributes)
    ACT:     determine_log_level
    ASSERT:  returns None (getattr handles missing attrs)
    """
    args = argparse.Namespace()

    actual = determine_log_level(args)

    assert actual is None
