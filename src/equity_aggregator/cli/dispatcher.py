# cli/dispatcher.py

import sys
from argparse import Namespace

from equity_aggregator.domain import download_canonical_equities as download
from equity_aggregator.domain import seed_canonical_equities as seed
from equity_aggregator.storage import export_canonical_equities as export


def run_command(fn: callable) -> None:
    """
    Execute a command function with exception handling.

    Runs the provided function and handles exceptions by printing
    the error to stderr and exiting with status code 1.

    Args:
        fn: A function to execute.
    """
    try:
        fn()
    except Exception as exc:
        print(f"{exc.__class__.__name__}: {exc}", file=sys.stderr)
        raise SystemExit(1) from None


def dispatch_command(args: Namespace, handlers: dict | None = None) -> None:
    """
    Dispatch execution to the appropriate command handler.

    Args:
        args: Parsed command line arguments from argparse.
        handlers: Optional dictionary mapping command names to handler functions.
            If not provided, uses the default production handlers.

    Raises:
        ValueError: If the command is not recognised.
    """
    if handlers is None:
        handlers = {
            "seed": lambda: run_command(seed),
            "export": lambda: run_command(lambda: export(args.output_dir, download)),
            "download": lambda: run_command(download),
        }

    handler = handlers.get(args.cmd)
    if handler:
        handler()
    else:
        raise ValueError(f"Unknown command: {args.cmd}")
