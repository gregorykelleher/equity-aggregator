# cli/signals.py

import os
import sys
from collections.abc import Callable
from types import FrameType

# Standard Unix exit code for a process terminated by SIGINT (128 + SIGINT).
CANCELLED_EXIT_CODE = 130


def create_signal_handler() -> Callable[[int, FrameType | None], None]:
    """
    Build a signal handler that escalates on a repeated signal.

    The first SIGINT/SIGTERM raises SystemExit(130), allowing Python to unwind
    the stack so that finally blocks and context managers run for a clean
    shutdown. A second signal calls os._exit(130) for immediate, unconditional
    termination should a graceful shutdown stall.

    Returns:
        Callable[[int, FrameType | None], None]: Handler suitable for
            registration via signal.signal.
    """
    cancelling = False

    def handle_signal(signum: int, frame: FrameType | None) -> None:
        nonlocal cancelling

        if cancelling:
            _force_exit()  # pragma: no cover

        cancelling = True
        print("\nOperation cancelled by user", file=sys.stderr)
        sys.stderr.flush()
        raise SystemExit(CANCELLED_EXIT_CODE)

    return handle_signal


def _force_exit() -> None:  # pragma: no cover
    """
    Terminate the process immediately without running cleanup handlers.

    Redirects stderr to /dev/null to suppress any cleanup errors emitted during
    teardown, then exits with the standard SIGINT status code.
    """
    try:
        devnull = os.open(os.devnull, os.O_WRONLY)
        os.dup2(devnull, sys.stderr.fileno())
        os.close(devnull)
    except OSError:
        pass

    os._exit(CANCELLED_EXIT_CODE)
