# _utils/parsing.py

from equity_aggregator.adapters.data_sources._utils._record_types import (
    EquityRecord,
)

from .extraction import extract_equity_record


def _find_search_component(data: dict) -> dict | None:
    """
    Find the turquoise-markets-instruments-search component.

    Args:
        data (dict): Raw JSON response from LSEG API.

    Returns:
        dict | None: The search component if found, None otherwise.
    """
    return next(
        (
            component
            for component in data.get("components", [])
            if component.get("type") == "turquoise-markets-instruments-search"
        ),
        None,
    )


def _find_content_item(component: dict | None, item_name: str) -> dict | None:
    """
    Find a specific content item by name within a component.

    Args:
        component (dict | None): The component to search within.
        item_name (str): Name of the content item to find.

    Returns:
        dict | None: The content item if found, None otherwise.
    """
    return component and next(
        (
            item
            for item in component.get("content", [])
            if item.get("name") == item_name
        ),
        None,
    )


def _get_value_data(item: dict | None) -> dict | None:
    """
    Extract value data from content item.

    Args:
        item (dict | None): Content item to extract value data from.

    Returns:
        dict | None: Value data if item exists, None otherwise.
    """
    return item.get("value") if item else None


def _has_content(value_data: dict | None) -> bool:
    """
    Check if value data contains content array.

    Args:
        value_data (dict | None): Value data to check for content.

    Returns:
        bool: True if value data contains content array, False otherwise.
    """
    return value_data is not None and "content" in value_data


def _process_value_data(value_data: dict) -> tuple[list[EquityRecord], dict]:
    """
    Process value data into records and pagination info.

    Args:
        value_data (dict): Value data containing content and pagination metadata.

    Returns:
        tuple[list[EquityRecord], dict]: Tuple containing processed equity
            records and pagination information.
    """
    records = [extract_equity_record(equity) for equity in value_data["content"]]
    pagination_info = _extract_pagination_info(value_data)
    return records, pagination_info


def _extract_pagination_info(value_data: dict) -> dict:
    """
    Extract pagination metadata from LSEG API response.

    Args:
        value_data (dict): Value section from LSEG API response with
            pagination metadata fields.

    Returns:
        dict: Dictionary containing pagination information including total
            pages, elements count, current page details, and navigation flags.
    """
    return {
        "totalPages": value_data.get("totalPages"),
        "totalElements": value_data.get("totalElements"),
        "numberOfElements": value_data.get("numberOfElements"),
        "size": value_data.get("size"),
        "number": value_data.get("number"),
        "first": value_data.get("first"),
        "last": value_data.get("last"),
    }


def extract_available_exchanges(data: dict) -> list[dict[str, str]]:
    """
    Extract available exchanges from LSEG API response.

    Args:
        data (dict): Raw JSON response from LSEG API.

    Returns:
        list[dict[str, str]]: List of available exchanges with metadata.
    """
    search_component = _find_search_component(data)
    if not search_component:
        return []

    tudmarkets_item = _find_content_item(search_component, "tudmarkets")
    return tudmarkets_item.get("value", []) if tudmarkets_item else []


def extract_exchange_page_data(data: dict) -> tuple[list[EquityRecord], dict | None]:
    """
    Extract equity records and pagination info from API response.

    Args:
        data (dict): Raw JSON response from LSEG API.

    Returns:
        tuple[list[EquityRecord], dict | None]: Tuple containing equity records
            and pagination metadata, or empty list and None if no data found.
    """
    search_component = _find_search_component(data)
    instruments_item = _find_content_item(search_component, "tudinstrumentsbymarket")
    value_data = _get_value_data(instruments_item)

    return _process_value_data(value_data) if _has_content(value_data) else ([], None)
