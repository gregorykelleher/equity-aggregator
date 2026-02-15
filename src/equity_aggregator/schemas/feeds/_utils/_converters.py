# feeds/_utils/_converters.py

from decimal import Decimal, InvalidOperation


def percent_to_decimal(
    value: str | float | Decimal | None,
) -> Decimal | None:
    """
    Convert a percentage value to its decimal ratio representation.

    Converts percentage values (e.g., 20.6 representing 20.6%) to decimal
    format (0.206) for consistency across all feed schemas.

    Returns:
        Decimal | None: The decimal ratio, or None if the input is None
            or unconvertible.
    """
    if value is None:
        return None

    try:
        return Decimal(str(value)) / Decimal("100")
    except (ValueError, TypeError, InvalidOperation):
        return None
