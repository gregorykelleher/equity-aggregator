# integrity/analysers/_helpers.py

from equity_aggregator.schemas import CanonicalEquity

from ..formatters import format_equity, limit_items
from ..models import Finding


def build_format_finding(
    invalid_equities: list[CanonicalEquity],
    message: str,
    sample_limit: int,
) -> Finding | None:
    """
    Build a format validation finding from invalid equities.

    Args:
        invalid_equities: Equities that failed validation.
        message: Descriptive message for the finding.
        sample_limit: Maximum number of sample equities to include.

    Returns:
        Finding | None: Finding with samples if violations exist, None otherwise.
    """
    if not invalid_equities:
        return None
    samples = limit_items(
        (format_equity(eq) for eq in invalid_equities),
        sample_limit,
    )
    return Finding(message, samples)
