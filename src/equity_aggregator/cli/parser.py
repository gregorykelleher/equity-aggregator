# cli/parser.py

import argparse


def create_parser() -> argparse.ArgumentParser:
    """
    Create and configure the argument parser with options and subcommands.

    Returns:
        argparse.ArgumentParser: Configured parser with all CLI options.
    """
    parser = argparse.ArgumentParser(
        prog="equity-aggregator",
        description="Aggregate, download, and export canonical equity data",
        epilog="Use 'equity-aggregator <command> --help' for help",
    )

    _add_logging_options(parser)
    _add_subcommands(parser)

    return parser


def _add_logging_options(parser: argparse.ArgumentParser) -> None:
    """
    Add logging level options to the argument parser.

    Args:
        parser: The argument parser to add options to.
    """
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose logging (INFO level)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging (DEBUG level)",
    )
    parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Quiet mode - only show warnings and errors",
    )


def _add_subcommands(parser: argparse.ArgumentParser) -> None:
    """
    Add all subcommands to the argument parser.

    Args:
        parser: The argument parser to add subcommands to.
    """
    sub = parser.add_subparsers(
        dest="cmd",
        required=True,
        title="commands",
        description="Available operations",
    )

    # add seed subcommand
    sub.add_parser(
        "seed",
        help="Aggregate equity data from authoritative sources",
        description="Execute the full aggregation pipeline to collect equity "
        "data from authoritative feeds (Euronext, LSE, SEC, XETRA), enrich "
        "it with supplementary data, and store as canonical equities",
    )

    # add export subcommand
    sub.add_parser(
        "export",
        help="Export canonical equity data to compressed JSONL format",
        description="Export processed canonical equity data from the database "
        "as gzip-compressed newline-delimited JSON (NDJSON) for distribution",
    )

    # add download subcommand
    sub.add_parser(
        "download",
        help="Download latest canonical equity data from remote repository",
        description="Retrieve the most recent canonical equity dataset from "
        "the remote data repository",
    )
