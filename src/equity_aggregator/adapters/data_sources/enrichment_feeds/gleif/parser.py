# gleif/parser.py

import csv
import io
import zipfile
from pathlib import Path


def parse_zip(zip_path: Path) -> dict[str, str]:
    """
    Extract and parse the CSV from a ZIP file into an ISIN->LEI index.

    Finds the first CSV file in the archive and parses it row by row,
    building a dictionary that maps ISIN codes to LEI codes.

    Args:
        zip_path: Path to the ZIP file.

    Returns:
        Dictionary mapping ISIN codes to LEI codes.

    Raises:
        ValueError: If no CSV file is found in the archive.
    """

    with zipfile.ZipFile(zip_path, "r") as zf:
        csv_name = _find_csv(zf)
        if csv_name is None:
            raise ValueError("No CSV file found in GLEIF ZIP archive.")

        with zf.open(csv_name) as csv_file:
            return _parse_csv(csv_file)


def _find_csv(zf: zipfile.ZipFile) -> str | None:
    """
    Find the first CSV file in a ZIP archive.

    Args:
        zf: Open ZIP file handle.

    Returns:
        Name of the first CSV file found, or None if not found.
    """
    return next(
        (name for name in zf.namelist() if name.lower().endswith(".csv")),
        None,
    )


def _parse_csv(csv_file: io.BufferedReader) -> dict[str, str]:
    """
    Parse the GLEIF ISIN->LEI CSV file into a look-up dictionary.

    The CSV has columns: LEI, ISIN.

    Args:
        csv_file: File-like object for the CSV data.

    Returns:
        Dictionary mapping ISIN codes to LEI codes.
    """
    text_wrapper = io.TextIOWrapper(csv_file, encoding="utf-8")
    reader = csv.DictReader(text_wrapper)

    index: dict[str, str] = {}

    for row in reader:
        isin = row.get("ISIN", "").strip().upper() or None
        lei = row.get("LEI", "").strip().upper() or None

        if isin and lei:
            index[isin] = lei

    return index
