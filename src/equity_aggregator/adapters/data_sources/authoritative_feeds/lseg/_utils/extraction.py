# _utils/extraction.py

import logging

from equity_aggregator.adapters.data_sources._utils._record_types import (
    EquityRecord,
    RecordStream,
    RecordUniqueKeyExtractor,
    UniqueRecordStream,
)

logger = logging.getLogger(__name__)


def create_deduplication_stream(
    extract_key: RecordUniqueKeyExtractor,
) -> UniqueRecordStream:
    """
    Create a deduplication stream processor for equity records.

    Returns a coroutine that filters duplicate records based on an extracted key,
    maintaining insertion order and logging validation issues.

    Args:
        extract_key: Pure function extracting unique identifier from record.

    Returns:
        Async coroutine accepting RecordStream, yielding unique records only.
    """

    async def unique_record_stream(records: RecordStream) -> RecordStream:
        seen_keys: set[object] = set()

        async for record in records:
            key = extract_key(record)

            if _is_invalid_deduplication_key(key):
                _log_invalid_key_warning(record)
                continue

            if key not in seen_keys:
                seen_keys.add(key)
                yield record

    return unique_record_stream


def extract_equity_record(equity: dict) -> EquityRecord:
    """
    Normalise raw LSEG JSON equity data into EquityRecord dictionary.

    Maps the raw API fields to the expected LsegFeedData schema fields.

    Note: Raw equity records from LSEG API do not contain MIC codes.
    However, each record is labeled with a 'mic_code' field during processing
    using the 'marketid' from the exchange metadata API call.

    Args:
        equity (dict): Raw equity data from LSEG API response with equity
            information, market data fields, and MIC code stamp.

    Returns:
        EquityRecord: Normalised equity record dictionary with field names
            matching LsegFeedData schema expectations.
    """
    # Use the MIC code that was labeled by the producer
    mics = []
    if mic_code := equity.get("mic_code"):
        mics.append(mic_code)

    return {
        "name": equity.get("name"),
        "symbol": equity.get("symbol"),
        "isin": equity.get("isin"),
        "currency": equity.get("currency"),
        "lastvalue": equity.get("lastvalue"),
        "mics": mics,
    }


def _is_invalid_deduplication_key(key: object) -> bool:
    """
    Validate whether a deduplication key is usable for record filtering.

    A key is considered invalid if it is None or an empty string, as these
    values cannot reliably identify unique records in the deduplication process.

    Args:
        key (object): The deduplication key extracted from an equity record.

    Returns:
        bool: True if the key is invalid (None or empty string), False otherwise.
    """
    return key is None or key == ""


def _log_invalid_key_warning(record: EquityRecord) -> None:
    """
    Log a warning message for equity records with invalid deduplication keys.

    Extracts the record name for identification purposes and logs a structured
    warning that helps with debugging data quality issues during processing.

    Args:
        record (EquityRecord): The equity record with an invalid deduplication
            key that will be skipped during processing.

    Returns:
        None
    """
    record_name = (
        record.get("name", "unknown") if isinstance(record, dict) else "non-dict"
    )
    logger.warning(
        "Skipping record with missing/empty deduplication key: %s",
        record_name,
    )
