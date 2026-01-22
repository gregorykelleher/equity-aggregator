# cli/test_dispatcher.py

import sys
from argparse import Namespace
from io import StringIO

import pytest

from equity_aggregator.cli.dispatcher import dispatch_command, run_command

pytestmark = pytest.mark.unit


def test_run_command_successful_execution() -> None:
    """
    ARRANGE: function that executes successfully
    ACT:     run_command
    ASSERT:  function executes without raising
    """
    executed = False

    def mock_function() -> None:
        nonlocal executed
        executed = True

    run_command(mock_function)

    assert executed is True


def test_run_command_handles_exception() -> None:
    """
    ARRANGE: function that raises ValueError
    ACT:     run_command
    ASSERT:  raises SystemExit with code 1
    """

    def failing_function() -> None:
        raise ValueError("test error")

    with pytest.raises(SystemExit) as exc_info:
        run_command(failing_function)

    assert exc_info.value.code == 1


def test_run_command_prints_error_to_stderr() -> None:
    """
    ARRANGE: function that raises RuntimeError and captured stderr
    ACT:     run_command
    ASSERT:  error message printed to stderr
    """

    def failing_function() -> None:
        raise RuntimeError("runtime error")

    original_stderr = sys.stderr
    captured_stderr = StringIO()

    try:
        sys.stderr = captured_stderr
        with pytest.raises(SystemExit):
            run_command(failing_function)
    finally:
        sys.stderr = original_stderr

    assert "RuntimeError: runtime error" in captured_stderr.getvalue()


def test_dispatch_command_unknown_command() -> None:
    """
    ARRANGE: unknown command in Namespace
    ACT:     dispatch_command
    ASSERT:  raises ValueError with appropriate message
    """
    args = Namespace(cmd="unknown")
    with pytest.raises(ValueError) as exc_info:
        dispatch_command(args)

    assert "Unknown command: unknown" in str(exc_info.value)


def test_dispatch_command_empty_string() -> None:
    """
    ARRANGE: empty command string in Namespace
    ACT:     dispatch_command
    ASSERT:  raises ValueError with appropriate message
    """
    args = Namespace(cmd="")
    with pytest.raises(ValueError) as exc_info:
        dispatch_command(args)

    assert "Unknown command: " in str(exc_info.value)


def test_dispatch_command_none_value() -> None:
    """
    ARRANGE: None command value in Namespace
    ACT:     dispatch_command
    ASSERT:  raises ValueError with appropriate message
    """
    args = Namespace(cmd=None)
    with pytest.raises(ValueError) as exc_info:
        dispatch_command(args)

    assert "Unknown command: None" in str(exc_info.value)


def test_dispatch_command_case_sensitive() -> None:
    """
    ARRANGE: uppercase command string in Namespace
    ACT:     dispatch_command
    ASSERT:  raises ValueError (commands are case-sensitive)
    """
    args = Namespace(cmd="SEED")
    with pytest.raises(ValueError) as exc_info:
        dispatch_command(args)

    assert "Unknown command: SEED" in str(exc_info.value)


def test_dispatch_command_whitespace_handling() -> None:
    """
    ARRANGE: command string with whitespace in Namespace
    ACT:     dispatch_command
    ASSERT:  raises ValueError (whitespace not stripped)
    """
    args = Namespace(cmd=" seed ")
    with pytest.raises(ValueError) as exc_info:
        dispatch_command(args)

    assert "Unknown command:  seed " in str(exc_info.value)


def test_dispatch_command_seed_handler_execution() -> None:
    """
    ARRANGE: seed command in Namespace with no-op handler
    ACT:     dispatch_command
    ASSERT:  seed handler is called
    """
    called = False

    def seed_handler() -> None:
        nonlocal called
        called = True

    args = Namespace(cmd="seed")
    handlers = {"seed": seed_handler, "export": lambda: None, "download": lambda: None}

    dispatch_command(args, handlers=handlers)

    assert called is True


def test_dispatch_command_export_handler_execution() -> None:
    """
    ARRANGE: export command in Namespace with no-op handler
    ACT:     dispatch_command
    ASSERT:  export handler is called
    """
    called = False

    def export_handler() -> None:
        nonlocal called
        called = True

    args = Namespace(cmd="export", output_dir="/tmp/test")
    handlers = {
        "seed": lambda: None,
        "export": export_handler,
        "download": lambda: None,
    }

    dispatch_command(args, handlers=handlers)

    assert called is True


def test_dispatch_command_download_handler_execution() -> None:
    """
    ARRANGE: download command in Namespace with no-op handler
    ACT:     dispatch_command
    ASSERT:  download handler is called
    """
    called = False

    def download_handler() -> None:
        nonlocal called
        called = True

    args = Namespace(cmd="download")
    handlers = {
        "seed": lambda: None,
        "export": lambda: None,
        "download": download_handler,
    }

    dispatch_command(args, handlers=handlers)

    assert called is True
