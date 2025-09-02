# equity_aggregator/cli.py

import argparse
import sys

from .domain import download_canonical_equities as download
from .domain import seed_canonical_equities as seed
from .logging_config import configure_logging
from .storage import export_canonical_equities as export


def run_command(fn: callable) -> None:
    """
    Executes a command function with exception handling.

    Runs the provided function and handles exceptions by printing
    the error to stderr and exiting with status code 1.

    Args:
        fn (callable): A function to execute.

    Returns:
        None
    """
    try:
        fn()
    except Exception as exc:
        print(f"{exc.__class__.__name__}: {exc}", file=sys.stderr)
        raise SystemExit(1) from None


def main() -> None:
    """
    Entry point for the equity-aggregator CLI application.

    Sets up command-line argument parsing, configures logging only when a valid
    command is provided, and dispatches execution to the appropriate subcommand handler.

    Args:
        None

    Returns:
        None
    """
    parser = argparse.ArgumentParser(
        prog="equity-aggregator",
        description="Aggregate, download, and export canonical equity data",
        epilog="Use 'equity-aggregator <command> --help' for command-specific options",
    )

    # add global logging options
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

    # add required subcommands
    sub = parser.add_subparsers(
        dest="cmd",
        required=True,
        title="commands",
        description="Available operations",
    )

    # add seed subcommand
    sub.add_parser(
        "seed",
        help=(
            "Aggregate equity data from authoritative sources and populate the database"
        ),
        description="Execute the full aggregation pipeline to collect equity data from "
        "authoritative feeds (Euronext, LSE, SEC, XETRA), enrich it with supplementary "
        "data, and store as canonical equities",
    )

    # add export subcommand
    sub.add_parser(
        "export",
        help="Export canonical equity data to compressed JSONL format",
        description="Export processed canonical equity data from the database as "
        "gzip-compressed newline-delimited JSON (NDJSON) for distribution",
    )

    # add download subcommand
    sub.add_parser(
        "download",
        help="Download latest canonical equity data from remote repository",
        description="Retrieve the most recent canonical equity dataset from the "
        "remote data repository",
    )

    # read args from command line
    args = parser.parse_args()

    # determine log level from flags
    log_level = None
    if args.debug:
        log_level = "debug"
    elif args.verbose:
        log_level = "development"
    elif args.quiet:
        log_level = "production"

    # configure logging
    configure_logging(log_level)

    if args.cmd == "seed":
        run_command(seed)
        return

    if args.cmd == "export":
        run_command(lambda: export(download))
        return

    if args.cmd == "download":
        run_command(download)
        return
