# gleif/test_parser.py

import zipfile
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from equity_aggregator.adapters.data_sources.enrichment_feeds.gleif.parser import (
    parse_zip,
)

pytestmark = pytest.mark.unit


def _create_test_zip(zip_path: Path, csv_name: str, csv_content: str) -> None:
    """
    Helper to create a ZIP file containing a CSV for testing.
    """
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr(csv_name, csv_content)


def test_parse_zip_returns_empty_dict_for_empty_csv() -> None:
    """
    ARRANGE: ZIP file containing CSV with only headers
    ACT:     call parse_zip
    ASSERT:  returns empty dictionary
    """
    csv_content = "LEI,ISIN\n"

    with TemporaryDirectory() as temp_dir:
        zip_path = Path(temp_dir) / "test.zip"
        _create_test_zip(zip_path, "mapping.csv", csv_content)

        actual = parse_zip(zip_path)

    assert actual == {}


def test_parse_zip_returns_single_mapping() -> None:
    """
    ARRANGE: ZIP file containing CSV with one valid row
    ACT:     call parse_zip
    ASSERT:  returns dictionary with one ISIN->LEI mapping
    """
    csv_content = "LEI,ISIN\n529900T8BM49AURSDO55,US0378331005\n"

    with TemporaryDirectory() as temp_dir:
        zip_path = Path(temp_dir) / "test.zip"
        _create_test_zip(zip_path, "mapping.csv", csv_content)

        actual = parse_zip(zip_path)

    assert actual == {"US0378331005": "529900T8BM49AURSDO55"}


def test_parse_zip_returns_multiple_mappings() -> None:
    """
    ARRANGE: ZIP file containing CSV with multiple valid rows
    ACT:     call parse_zip
    ASSERT:  returns dictionary with all ISIN->LEI mappings
    """
    expected_isins = {"US0378331005", "US5949181045", "GB00B03MLX29"}
    csv_content = (
        "LEI,ISIN\n"
        "529900T8BM49AURSDO55,US0378331005\n"
        "HWUPKR0MPOU8FGXBT394,US5949181045\n"
        "549300GGN6YROH77Y439,GB00B03MLX29\n"
    )

    with TemporaryDirectory() as temp_dir:
        zip_path = Path(temp_dir) / "test.zip"
        _create_test_zip(zip_path, "data.csv", csv_content)

        actual = parse_zip(zip_path)

    assert set(actual.keys()) == expected_isins


def test_parse_zip_maps_isin_to_lei() -> None:
    """
    ARRANGE: ZIP file containing CSV with known ISIN->LEI pair
    ACT:     call parse_zip
    ASSERT:  ISIN key maps to correct LEI value
    """
    csv_content = "LEI,ISIN\nHWUPKR0MPOU8FGXBT394,US5949181045\n"

    with TemporaryDirectory() as temp_dir:
        zip_path = Path(temp_dir) / "test.zip"
        _create_test_zip(zip_path, "mapping.csv", csv_content)

        actual = parse_zip(zip_path)

    assert actual["US5949181045"] == "HWUPKR0MPOU8FGXBT394"


def test_parse_zip_uppercases_isin() -> None:
    """
    ARRANGE: ZIP file containing CSV with lowercase ISIN
    ACT:     call parse_zip
    ASSERT:  ISIN key is uppercase
    """
    csv_content = "LEI,ISIN\n529900T8BM49AURSDO55,us0378331005\n"

    with TemporaryDirectory() as temp_dir:
        zip_path = Path(temp_dir) / "test.zip"
        _create_test_zip(zip_path, "mapping.csv", csv_content)

        actual = parse_zip(zip_path)

    assert "US0378331005" in actual


def test_parse_zip_uppercases_lei() -> None:
    """
    ARRANGE: ZIP file containing CSV with lowercase LEI
    ACT:     call parse_zip
    ASSERT:  LEI value is uppercase
    """
    csv_content = "LEI,ISIN\n529900t8bm49aursdo55,US0378331005\n"

    with TemporaryDirectory() as temp_dir:
        zip_path = Path(temp_dir) / "test.zip"
        _create_test_zip(zip_path, "mapping.csv", csv_content)

        actual = parse_zip(zip_path)

    assert actual["US0378331005"] == "529900T8BM49AURSDO55"


def test_parse_zip_strips_whitespace_from_isin() -> None:
    """
    ARRANGE: ZIP file containing CSV with whitespace around ISIN
    ACT:     call parse_zip
    ASSERT:  ISIN key has whitespace stripped
    """
    csv_content = "LEI,ISIN\n529900T8BM49AURSDO55,  US0378331005  \n"

    with TemporaryDirectory() as temp_dir:
        zip_path = Path(temp_dir) / "test.zip"
        _create_test_zip(zip_path, "mapping.csv", csv_content)

        actual = parse_zip(zip_path)

    assert "US0378331005" in actual


def test_parse_zip_strips_whitespace_from_lei() -> None:
    """
    ARRANGE: ZIP file containing CSV with whitespace around LEI
    ACT:     call parse_zip
    ASSERT:  LEI value has whitespace stripped
    """
    csv_content = "LEI,ISIN\n  529900T8BM49AURSDO55  ,US0378331005\n"

    with TemporaryDirectory() as temp_dir:
        zip_path = Path(temp_dir) / "test.zip"
        _create_test_zip(zip_path, "mapping.csv", csv_content)

        actual = parse_zip(zip_path)

    assert actual["US0378331005"] == "529900T8BM49AURSDO55"


def test_parse_zip_skips_row_with_empty_isin() -> None:
    """
    ARRANGE: ZIP file containing CSV with empty ISIN field
    ACT:     call parse_zip
    ASSERT:  row is skipped, empty string not present
    """
    csv_content = "LEI,ISIN\n529900T8BM49AURSDO55,\n"

    with TemporaryDirectory() as temp_dir:
        zip_path = Path(temp_dir) / "test.zip"
        _create_test_zip(zip_path, "mapping.csv", csv_content)

        actual = parse_zip(zip_path)

    assert actual == {}


def test_parse_zip_skips_row_with_empty_lei() -> None:
    """
    ARRANGE: ZIP file containing CSV with empty LEI field
    ACT:     call parse_zip
    ASSERT:  row is skipped, ISIN not present
    """
    csv_content = "LEI,ISIN\n,US0378331005\n"

    with TemporaryDirectory() as temp_dir:
        zip_path = Path(temp_dir) / "test.zip"
        _create_test_zip(zip_path, "mapping.csv", csv_content)

        actual = parse_zip(zip_path)

    assert "US0378331005" not in actual


def test_parse_zip_skips_row_with_whitespace_only_isin() -> None:
    """
    ARRANGE: ZIP file containing CSV with whitespace-only ISIN
    ACT:     call parse_zip
    ASSERT:  row is skipped
    """
    csv_content = "LEI,ISIN\n529900T8BM49AURSDO55,   \n"

    with TemporaryDirectory() as temp_dir:
        zip_path = Path(temp_dir) / "test.zip"
        _create_test_zip(zip_path, "mapping.csv", csv_content)

        actual = parse_zip(zip_path)

    assert actual == {}


def test_parse_zip_skips_row_with_whitespace_only_lei() -> None:
    """
    ARRANGE: ZIP file containing CSV with whitespace-only LEI
    ACT:     call parse_zip
    ASSERT:  row is skipped
    """
    csv_content = "LEI,ISIN\n   ,US0378331005\n"

    with TemporaryDirectory() as temp_dir:
        zip_path = Path(temp_dir) / "test.zip"
        _create_test_zip(zip_path, "mapping.csv", csv_content)

        actual = parse_zip(zip_path)

    assert actual == {}


def test_parse_zip_finds_csv_with_uppercase_extension() -> None:
    """
    ARRANGE: ZIP file containing CSV with .CSV uppercase extension
    ACT:     call parse_zip
    ASSERT:  CSV is found and parsed successfully
    """
    csv_content = "LEI,ISIN\n529900T8BM49AURSDO55,US0378331005\n"

    with TemporaryDirectory() as temp_dir:
        zip_path = Path(temp_dir) / "test.zip"
        _create_test_zip(zip_path, "MAPPING.CSV", csv_content)

        actual = parse_zip(zip_path)

    assert "US0378331005" in actual


def test_parse_zip_raises_when_no_csv_in_archive() -> None:
    """
    ARRANGE: ZIP file containing no CSV files
    ACT:     call parse_zip
    ASSERT:  raises ValueError with descriptive message
    """
    with TemporaryDirectory() as temp_dir:
        zip_path = Path(temp_dir) / "test.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("readme.txt", "No CSV here")

        with pytest.raises(ValueError) as exc_info:
            parse_zip(zip_path)

    assert "No CSV file found" in str(exc_info.value)


def test_parse_zip_handles_mixed_valid_and_invalid_rows() -> None:
    """
    ARRANGE: ZIP file containing CSV with mix of valid and invalid rows
    ACT:     call parse_zip
    ASSERT:  only valid rows are included
    """
    expected_isins = {"US0378331005", "GB00B03MLX29"}
    csv_content = (
        "LEI,ISIN\n"
        "529900T8BM49AURSDO55,US0378331005\n"
        ",US1234567890\n"
        "HWUPKR0MPOU8FGXBT394,\n"
        "549300GGN6YROH77Y439,GB00B03MLX29\n"
    )

    with TemporaryDirectory() as temp_dir:
        zip_path = Path(temp_dir) / "test.zip"
        _create_test_zip(zip_path, "mapping.csv", csv_content)

        actual = parse_zip(zip_path)

    assert set(actual.keys()) == expected_isins


def test_parse_zip_last_mapping_wins_for_duplicate_isin() -> None:
    """
    ARRANGE: ZIP file containing CSV with duplicate ISIN values
    ACT:     call parse_zip
    ASSERT:  last LEI value for the ISIN is preserved
    """
    csv_content = (
        "LEI,ISIN\n"
        "FIRSTLEI00000000000000,US0378331005\n"
        "SECONDLEI0000000000000,US0378331005\n"
    )

    with TemporaryDirectory() as temp_dir:
        zip_path = Path(temp_dir) / "test.zip"
        _create_test_zip(zip_path, "mapping.csv", csv_content)

        actual = parse_zip(zip_path)

    assert actual["US0378331005"] == "SECONDLEI0000000000000"
