# cli/test_signals.py

import signal

import pytest

from equity_aggregator.cli.signals import create_signal_handler

pytestmark = pytest.mark.unit


def test_first_signal_raises_system_exit() -> None:
    """
    ARRANGE: a fresh signal handler
    ACT:     deliver a first SIGINT
    ASSERT:  raises SystemExit (clean unwinding rather than os._exit)
    """
    handle_signal = create_signal_handler()

    with pytest.raises(SystemExit):
        handle_signal(signal.SIGINT, None)


def test_first_signal_exits_with_code_130() -> None:
    """
    ARRANGE: a fresh signal handler
    ACT:     deliver a first SIGINT and capture the exit code
    ASSERT:  exit code is 130 (standard SIGINT convention)
    """
    handle_signal = create_signal_handler()
    expected_code = 130

    with pytest.raises(SystemExit) as exit_info:
        handle_signal(signal.SIGINT, None)

    assert exit_info.value.code == expected_code


def test_first_sigterm_raises_system_exit() -> None:
    """
    ARRANGE: a fresh signal handler
    ACT:     deliver a first SIGTERM
    ASSERT:  raises SystemExit (SIGTERM handled like SIGINT)
    """
    handle_signal = create_signal_handler()

    with pytest.raises(SystemExit):
        handle_signal(signal.SIGTERM, None)
