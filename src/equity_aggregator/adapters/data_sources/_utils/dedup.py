# _utils/dedup.py


from ._record_types import RecordStream, RecordUniqueKeyExtractor, UniqueRecordStream


def deduplicate_records(extract_key: RecordUniqueKeyExtractor) -> UniqueRecordStream:
    """
    Build a deduplicator for an async record stream, keyed by an extracted value.

    Returns a coroutine that consumes an async iterator of records and yields only
    the first record seen for each extracted key, preserving arrival order.

    Args:
        extract_key (RecordUniqueKeyExtractor): Function mapping a record to the
            value used to determine uniqueness.

    Returns:
        UniqueRecordStream: A coroutine accepting an async record stream and
            yielding only the records whose extracted key is seen for the first time.
    """

    async def deduplicator(records: RecordStream) -> RecordStream:
        """
        Yield records whose extracted key has not been seen before.

        Args:
            records (RecordStream): Async iterator of records to deduplicate.

        Yields:
            EquityRecord: Unique records, as determined by the extracted key.
        """
        seen: set[object] = set()
        async for record in records:
            key = extract_key(record)
            if key in seen:
                continue
            seen.add(key)
            yield record

    return deduplicator
