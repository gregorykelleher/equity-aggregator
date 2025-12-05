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
        description="aggregate, download, and export canonical equity data",
        epilog="use 'equity-aggregator <command> --help' for help",
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
        help="enable verbose logging (INFO level)",
    )
    parser.add_argument(
        "-d",
        "--debug",
        action="store_true",
        help="enable debug logging (DEBUG level)",
    )
    parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="quiet mode - only show warnings and errors",
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
        description="available operations",
    )

    # add seed subcommand
    sub.add_parser(
        "seed",
        help="aggregate enriched canonical equity data sourced from data feeds",
        description="execute the full aggregation pipeline to collect equity "
        "data from discovery feeds (LSEG, SEC, XETRA), enrich "
        "it with data from enrichment feeds, and store as canonical equities",
    )

    # add export subcommand
    sub.add_parser(
        "export",
        help="export local canonical equity data to compressed JSONL format",
        description="export local canonical equity data from the database "
        "as gzip-compressed newline-delimited JSON (NDJSON) for distribution",
    ).add_argument(
        "--output-dir",
        required=True,
        help="directory where canonical_equities.jsonl.gz will be created",
    )

    # add download subcommand
    sub.add_parser(
        "download",
        help="download latest canonical equity data from remote repository",
        description="retrieve the most recent canonical equity dataset from "
        "the remote data repository",
    )
