# cli/main.py

import signal
from collections.abc import Callable
from typing import Any

from equity_aggregator.logging_config import configure_logging

from .config import determine_log_level
from .dispatcher import dispatch_command
from .parser import create_parser
from .signals import handle_sigint


def main(dispatcher: Callable[[Any], None] | None = None) -> None:
    """
    Entry point for the equity-aggregator CLI application.

    This function orchestrates the entire CLI workflow by setting up argument
    parsing, configuring the logging system based on user preferences, and
    dispatching execution to the appropriate command handler. It serves as
    the main entry point defined in pyproject.toml for the CLI script.

    The function handles the complete CLI lifecycle:
    1. Creates and configures the argument parser
    2. Parses command line arguments and options
    3. Determines appropriate logging level from CLI flags
    4. Configures the application logging system
    5. Dispatches to the selected command handler

    Args:
        dispatcher: Optional callable to dispatch commands. If not provided,
            uses the default dispatch_command function.

    Raises:
        SystemExit: When command execution fails or invalid arguments provided.
    """
    if dispatcher is None:
        dispatcher = dispatch_command

    # Install signal handler for clean Ctrl+C handling
    signal.signal(signal.SIGINT, handle_sigint)

    # Create the argument parser with all CLI options and subcommands
    parser = create_parser()

    # Parse the command line arguments provided by the user
    args = parser.parse_args()

    # Determine logging level from verbose, debug, or quiet flags
    log_level = determine_log_level(args)

    # Configure the application logging system with the determined level
    configure_logging(log_level)

    # Dispatch execution to the appropriate command handler
    dispatcher(args)
