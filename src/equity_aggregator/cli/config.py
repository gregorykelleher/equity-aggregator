# cli/config.py

import argparse


def determine_log_level(args: argparse.Namespace) -> str | None:
    """
    Determine the appropriate log level from command line arguments.

    Args:
        args: Parsed command line arguments containing logging flags.

    Returns:
        str | None: The log level string, or None for default logging.
    """
    log_level_mapping = {
        "debug": "debug",
        "verbose": "development",
        "quiet": "production",
    }

    for flag, level in log_level_mapping.items():
        if getattr(args, flag, False):
            return level

    return None
