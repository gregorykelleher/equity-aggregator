# _utils/test_parser.py

import pytest

from equity_aggregator.adapters.data_sources.discovery_feeds.lseg._utils.parser import (
    _extract_equity_record,
    _find_content_item,
    _find_price_explorer_component,
    _process_value_data,
    parse_response,
)

pytestmark = pytest.mark.unit


def test_parse_response_extracts_records() -> None:
    """
    ARRANGE: valid LSEG API response with one equity record
    ACT:     call parse_response
    ASSERT:  equity record is extracted
    """
    data = {
        "components": [
            {
                "type": "price-explorer",
                "content": [
                    {
                        "name": "priceexplorersearch",
                        "value": {
                            "content": [
                                {
                                    "issuername": "Test Co",
                                    "tidm": "TST",
                                    "isin": "GB0001",
                                    "currency": "GBX",
                                    "lastprice": 100,
                                    "marketcapitalization": 1000,
                                    "fiftyTwoWeeksMin": 50,
                                    "fiftyTwoWeeksMax": 150,
                                },
                            ],
                            "totalPages": 1,
                        },
                    },
                ],
            },
        ],
    }

    actual, _ = parse_response(data)

    assert len(actual) == 1


def test_parse_response_extracts_pagination_info() -> None:
    """
    ARRANGE: valid LSEG API response with pagination metadata
    ACT:     call parse_response
    ASSERT:  pagination info contains totalPages
    """
    data = {
        "components": [
            {
                "type": "price-explorer",
                "content": [
                    {
                        "name": "priceexplorersearch",
                        "value": {
                            "content": [],
                            "totalPages": 5,
                        },
                    },
                ],
            },
        ],
    }

    _, pagination_info = parse_response(data)

    assert pagination_info["totalPages"] == 5


def test_parse_response_returns_empty_when_no_component() -> None:
    """
    ARRANGE: response without price-explorer component
    ACT:     call parse_response
    ASSERT:  returns empty list
    """
    data = {"components": []}

    actual, _ = parse_response(data)

    assert actual == []


def test_parse_response_returns_none_pagination_when_no_component() -> None:
    """
    ARRANGE: response without price-explorer component
    ACT:     call parse_response
    ASSERT:  returns None for pagination info
    """
    data = {"components": []}

    _, pagination_info = parse_response(data)

    assert pagination_info is None


def test_parse_response_returns_empty_when_no_search_item() -> None:
    """
    ARRANGE: price-explorer component without priceexplorersearch
    ACT:     call parse_response
    ASSERT:  returns empty list
    """
    data = {
        "components": [
            {
                "type": "price-explorer",
                "content": [],
            },
        ],
    }

    actual, _ = parse_response(data)

    assert actual == []


def test_parse_response_returns_empty_when_value_missing_content() -> None:
    """
    ARRANGE: priceexplorersearch exists but value has no content key
    ACT:     call parse_response
    ASSERT:  returns empty list
    """
    data = {
        "components": [
            {
                "type": "price-explorer",
                "content": [
                    {
                        "name": "priceexplorersearch",
                        "value": {"otherField": "data"},
                    },
                ],
            },
        ],
    }

    actual, _ = parse_response(data)

    assert actual == []


def test_parse_response_returns_empty_when_value_is_none() -> None:
    """
    ARRANGE: priceexplorersearch exists but value is None
    ACT:     call parse_response
    ASSERT:  returns empty list
    """
    data = {
        "components": [
            {
                "type": "price-explorer",
                "content": [
                    {
                        "name": "priceexplorersearch",
                        "value": None,
                    },
                ],
            },
        ],
    }

    actual, _ = parse_response(data)

    assert actual == []


def test_find_price_explorer_component_returns_component() -> None:
    """
    ARRANGE: components list with price-explorer
    ACT:     call _find_price_explorer_component
    ASSERT:  price-explorer component is returned
    """
    data = {
        "components": [
            {"type": "other"},
            {"type": "price-explorer", "content": []},
        ],
    }

    actual = _find_price_explorer_component(data)

    assert actual["type"] == "price-explorer"


def test_find_price_explorer_component_returns_none_when_missing() -> None:
    """
    ARRANGE: components list without price-explorer
    ACT:     call _find_price_explorer_component
    ASSERT:  returns None
    """
    data = {"components": [{"type": "other"}]}

    actual = _find_price_explorer_component(data)

    assert actual is None


def test_find_price_explorer_component_returns_none_when_empty() -> None:
    """
    ARRANGE: empty components list
    ACT:     call _find_price_explorer_component
    ASSERT:  returns None
    """
    data = {"components": []}

    actual = _find_price_explorer_component(data)

    assert actual is None


def test_find_content_item_returns_item() -> None:
    """
    ARRANGE: component with named content item
    ACT:     call _find_content_item
    ASSERT:  content item is returned
    """
    component = {
        "content": [
            {"name": "other"},
            {"name": "priceexplorersearch", "value": {}},
        ],
    }

    actual = _find_content_item(component, "priceexplorersearch")

    assert actual["name"] == "priceexplorersearch"


def test_find_content_item_returns_none_when_missing() -> None:
    """
    ARRANGE: component without matching content item
    ACT:     call _find_content_item
    ASSERT:  returns None
    """
    component = {"content": [{"name": "other"}]}

    actual = _find_content_item(component, "priceexplorersearch")

    assert actual is None


def test_find_content_item_returns_none_when_component_none() -> None:
    """
    ARRANGE: component is None
    ACT:     call _find_content_item
    ASSERT:  returns None
    """
    actual = _find_content_item(None, "priceexplorersearch")

    assert actual is None


def test_process_value_data_extracts_records() -> None:
    """
    ARRANGE: value data with two equity records
    ACT:     call _process_value_data
    ASSERT:  two records are extracted
    """
    value_data = {
        "content": [
            {"issuername": "Co 1", "tidm": "CO1"},
            {"issuername": "Co 2", "tidm": "CO2"},
        ],
        "totalPages": 1,
    }

    actual, _ = _process_value_data(value_data)

    assert len(actual) == 2


def test_process_value_data_extracts_pagination() -> None:
    """
    ARRANGE: value data with totalPages field
    ACT:     call _process_value_data
    ASSERT:  pagination info contains totalPages
    """
    value_data = {
        "content": [],
        "totalPages": 3,
    }

    _, pagination_info = _process_value_data(value_data)

    assert pagination_info["totalPages"] == 3


def test_extract_equity_record_maps_issuername() -> None:
    """
    ARRANGE: equity dict with issuername field
    ACT:     call _extract_equity_record
    ASSERT:  issuername is preserved
    """
    equity = {
        "issuername": "Test Company",
        "tidm": "TST",
        "isin": "GB0001",
        "currency": "GBX",
        "lastprice": 100,
        "marketcapitalization": 1000,
        "fiftyTwoWeeksMin": 50,
        "fiftyTwoWeeksMax": 150,
    }

    actual = _extract_equity_record(equity)

    assert actual["issuername"] == "Test Company"


def test_extract_equity_record_maps_tidm() -> None:
    """
    ARRANGE: equity dict with tidm field
    ACT:     call _extract_equity_record
    ASSERT:  tidm is preserved
    """
    equity = {
        "issuername": "Test",
        "tidm": "SYMB",
        "isin": "GB0001",
        "currency": "GBX",
        "lastprice": 100,
        "marketcapitalization": 1000,
        "fiftyTwoWeeksMin": 50,
        "fiftyTwoWeeksMax": 150,
    }

    actual = _extract_equity_record(equity)

    assert actual["tidm"] == "SYMB"


def test_extract_equity_record_maps_all_fields() -> None:
    """
    ARRANGE: equity dict with all fields
    ACT:     call _extract_equity_record
    ASSERT:  all expected fields are present
    """
    equity = {
        "issuername": "Test",
        "tidm": "TST",
        "isin": "GB0001",
        "currency": "GBX",
        "lastprice": 100,
        "marketcapitalization": 1000,
        "fiftyTwoWeeksMin": 50,
        "fiftyTwoWeeksMax": 150,
        "extrafield": "ignored",
    }

    actual = _extract_equity_record(equity)

    assert set(actual.keys()) == {
        "issuername",
        "tidm",
        "isin",
        "currency",
        "lastprice",
        "marketcapitalization",
        "fiftyTwoWeeksMin",
        "fiftyTwoWeeksMax",
    }


def test_extract_equity_record_handles_none_values() -> None:
    """
    ARRANGE: equity dict with None values
    ACT:     call _extract_equity_record
    ASSERT:  None values are preserved
    """
    equity = {
        "issuername": "Test",
        "tidm": "TST",
        "isin": None,
        "currency": None,
        "lastprice": None,
        "marketcapitalization": None,
        "fiftyTwoWeeksMin": None,
        "fiftyTwoWeeksMax": None,
    }

    actual = _extract_equity_record(equity)

    assert actual["isin"] is None
