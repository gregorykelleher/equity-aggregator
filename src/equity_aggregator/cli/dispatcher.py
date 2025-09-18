# cli/dispatcher.py

import sys

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


def dispatch_command(command: str) -> None:
    """
    Dispatch execution to the appropriate command handler.

    Args:
        command: The command name to execute.

    Raises:
        ValueError: If the command is not recognised.
    """
    commands = {
        "seed": lambda: run_command(seed),
        "export": lambda: run_command(lambda: export(download)),
        "download": lambda: run_command(download),
    }

    handler = commands.get(command)
    if handler:
        handler()
    else:
        raise ValueError(f"Unknown command: {command}")
