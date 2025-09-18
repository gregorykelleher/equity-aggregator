# cli/main.py

import os
import signal

from equity_aggregator.logging_config import configure_logging

from .config import determine_log_level
from .dispatcher import dispatch_command
from .parser import create_parser


def main() -> None:
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

    Raises:
        SystemExit: When command execution fails or invalid arguments provided.
    """
    # Immediate force exit on Ctrl+C
    signal.signal(signal.SIGINT, lambda s, f: os._exit(130))

    # Create the argument parser with all CLI options and subcommands
    parser = create_parser()

    # Parse the command line arguments provided by the user
    args = parser.parse_args()

    # Determine logging level from verbose, debug, or quiet flags
    log_level = determine_log_level(args)

    # Configure the application logging system with the determined level
    configure_logging(log_level)

    # Dispatch execution to the appropriate command handler
    dispatch_command(args.cmd)
