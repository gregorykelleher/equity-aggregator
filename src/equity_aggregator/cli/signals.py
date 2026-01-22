# cli/signals.py

import os
import sys
from types import FrameType


def handle_sigint(signum: int, frame: FrameType | None) -> None:  # pragma: no cover
    """
    Handle SIGINT (Ctrl+C) by exiting immediately.

    When the user presses Ctrl+C, print a clean message and exit immediately
    with status code 130 (standard Unix convention for SIGINT). Uses os._exit()
    for immediate termination and redirects stderr to /dev/null to suppress
    any process cleanup errors from the parent process manager.

    Args:
        signum: The signal number (SIGINT).
        frame: The current stack frame.
    """
    print("\nOperation cancelled by user", file=sys.stderr)
    sys.stderr.flush()

    # Redirect stderr to suppress further output during exit
    try:
        devnull = os.open(os.devnull, os.O_WRONLY)
        os.dup2(devnull, sys.stderr.fileno())
        os.close(devnull)
    except OSError:
        pass

    os._exit(130)
