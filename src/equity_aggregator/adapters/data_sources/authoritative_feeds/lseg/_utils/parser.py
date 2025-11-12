# _utils/parser.py

from equity_aggregator.adapters.data_sources._utils._record_types import (
    EquityRecord,
)


def parse_response(data: dict) -> tuple[list[EquityRecord], dict | None]:
    """
    Parse LSEG response to extract equity records and pagination metadata.

    Extracts the price-explorer component, then the priceexplorersearch item,
    then the value data containing the content array. Processes the content into
    equity records with pagination info. Returns empty result if extraction fails.

    Args:
        data (dict): Raw JSON response from LSEG.

    Returns:
        tuple[list[EquityRecord], dict | None]: Tuple containing equity records
            and pagination metadata, or empty list and None if no data found.
    """
    component = _find_price_explorer_component(data)

    search_item = (
        _find_content_item(component, "priceexplorersearch") if component else None
    )

    if not search_item:
        return ([], None)

    value_data = search_item.get("value")

    if value_data and "content" in value_data:
        return _process_value_data(value_data)

    return ([], None)


def _find_price_explorer_component(data: dict) -> dict | None:
    """
    Find the price-explorer component.

    Args:
        data (dict): Raw JSON response from LSEG API.

    Returns:
        dict | None: The price-explorer component if found, None otherwise.
    """
    return next(
        (
            component
            for component in data.get("components", [])
            if component.get("type") == "price-explorer"
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


def _process_value_data(value_data: dict) -> tuple[list[EquityRecord], dict]:
    """
    Process value data into records and pagination info.

    Args:
        value_data (dict): Value data containing content and pagination metadata.

    Returns:
        tuple[list[EquityRecord], dict]: Tuple containing processed equity
            records and pagination information.
    """
    records = [_extract_equity_record(equity) for equity in value_data["content"]]
    pagination_info = {"totalPages": value_data.get("totalPages")}
    return records, pagination_info


def _extract_equity_record(equity: dict) -> EquityRecord:
    """
    Normalise raw LSEG JSON equity data into EquityRecord dictionary.

    Maps the raw API fields to the expected LsegFeedData schema fields.

    Args:
        equity (dict): Raw equity data from LSEG price-explorer API response
            with equity information and market data fields.

    Returns:
        EquityRecord: Normalised equity record dictionary with field names
            matching LsegFeedData schema expectations.
    """
    return {
        "issuername": equity.get("issuername"),
        "tidm": equity.get("tidm"),
        "isin": equity.get("isin"),
        "currency": equity.get("currency"),
        "lastprice": equity.get("lastprice"),
        "marketcapitalization": equity.get("marketcapitalization"),
        "fiftyTwoWeeksMin": equity.get("fiftyTwoWeeksMin"),
        "fiftyTwoWeeksMax": equity.get("fiftyTwoWeeksMax"),
    }
