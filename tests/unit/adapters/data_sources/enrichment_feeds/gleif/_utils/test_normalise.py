# _utils/test_normalise.py

import pytest

from equity_aggregator.adapters.data_sources.enrichment_feeds.gleif._utils import (
    strip_corporate_suffix,
)

pytestmark = pytest.mark.unit


def test_strips_plc() -> None:
    """
    ARRANGE: equity name ending with PLC
    ACT:     strip corporate suffix
    ASSERT:  PLC is removed
    """
    actual = strip_corporate_suffix("AIB GROUP PLC")

    assert actual == "AIB GROUP"


def test_strips_inc_with_dot() -> None:
    """
    ARRANGE: equity name ending with Inc.
    ACT:     strip corporate suffix
    ASSERT:  Inc. is removed
    """
    actual = strip_corporate_suffix("Apple Inc.")

    assert actual == "Apple"


def test_strips_ag() -> None:
    """
    ARRANGE: equity name ending with AG
    ACT:     strip corporate suffix
    ASSERT:  AG is removed
    """
    actual = strip_corporate_suffix("Volkswagen AG")

    assert actual == "Volkswagen"


def test_strips_se() -> None:
    """
    ARRANGE: equity name ending with SE
    ACT:     strip corporate suffix
    ASSERT:  SE is removed
    """
    actual = strip_corporate_suffix("SAP SE")

    assert actual == "SAP"


def test_strips_gmbh() -> None:
    """
    ARRANGE: equity name ending with GMBH
    ACT:     strip corporate suffix
    ASSERT:  GMBH is removed
    """
    actual = strip_corporate_suffix("Siemens Energy GMBH")

    assert actual == "Siemens Energy"


def test_strips_corp() -> None:
    """
    ARRANGE: equity name ending with CORP
    ACT:     strip corporate suffix
    ASSERT:  CORP is removed
    """
    actual = strip_corporate_suffix("MICROSOFT CORP")

    assert actual == "MICROSOFT"


def test_strips_ltd() -> None:
    """
    ARRANGE: equity name ending with LTD
    ACT:     strip corporate suffix
    ASSERT:  LTD is removed
    """
    actual = strip_corporate_suffix("ROLLS ROYCE LTD")

    assert actual == "ROLLS ROYCE"


def test_strips_dotted_plc() -> None:
    """
    ARRANGE: equity name ending with P.L.C.
    ACT:     strip corporate suffix
    ASSERT:  dotted form is removed
    """
    actual = strip_corporate_suffix("AIB Group (UK) P.L.C.")

    assert actual == "AIB Group (UK)"


def test_strips_public_limited_company() -> None:
    """
    ARRANGE: equity name ending with PUBLIC LIMITED COMPANY
    ACT:     strip corporate suffix
    ASSERT:  multi-word suffix is removed
    """
    expected = "ALLIED IRISH BANKS"

    actual = strip_corporate_suffix(
        "ALLIED IRISH BANKS PUBLIC LIMITED COMPANY",
    )

    assert actual == expected


def test_strips_comma_separated_suffix() -> None:
    """
    ARRANGE: equity name with comma before suffix
    ACT:     strip corporate suffix
    ASSERT:  suffix and trailing comma are removed
    """
    actual = strip_corporate_suffix("Alphabet, Inc.")

    assert actual == "Alphabet"


def test_returns_original_when_no_suffix() -> None:
    """
    ARRANGE: equity name without a corporate suffix
    ACT:     strip corporate suffix
    ASSERT:  returns the original name unchanged
    """
    expected = "Shell"

    actual = strip_corporate_suffix("Shell")

    assert actual == expected


def test_returns_original_when_name_is_only_suffix() -> None:
    """
    ARRANGE: equity name that is just a corporate suffix
    ACT:     strip corporate suffix
    ASSERT:  returns the original name (does not produce empty string)
    """
    expected = "PLC"

    actual = strip_corporate_suffix("PLC")

    assert actual == expected
