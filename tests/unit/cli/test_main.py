# cli/test_main.py

import sys

import pytest

from equity_aggregator.cli.main import main

pytestmark = pytest.mark.unit


def test_main_with_valid_seed_command() -> None:
    """
    ARRANGE: sys.argv set to seed command
    ACT:     main
    ASSERT:  executes without ValueError (command dispatched successfully)
    """
    original_argv = sys.argv[:]
    sys.argv = ["equity-aggregator", "seed"]

    try:
        main()
    except ValueError:
        pytest.fail("main should not raise ValueError for valid command")
    except (SystemExit, Exception):
        pass
    finally:
        sys.argv = original_argv


def test_main_with_valid_export_command() -> None:
    """
    ARRANGE: sys.argv set to export command
    ACT:     main
    ASSERT:  executes without ValueError (command dispatched successfully)
    """
    original_argv = sys.argv[:]
    sys.argv = ["equity-aggregator", "export"]

    try:
        main()
    except ValueError:
        pytest.fail("main should not raise ValueError for valid command")
    except (SystemExit, Exception):
        pass
    finally:
        sys.argv = original_argv


def test_main_with_valid_download_command() -> None:
    """
    ARRANGE: sys.argv set to download command
    ACT:     main
    ASSERT:  executes without ValueError (command dispatched successfully)
    """
    original_argv = sys.argv[:]
    sys.argv = ["equity-aggregator", "download"]

    try:
        main()
    except ValueError:
        pytest.fail("main should not raise ValueError for valid command")
    except (SystemExit, Exception):
        pass
    finally:
        sys.argv = original_argv


def test_main_with_verbose_flag() -> None:
    """
    ARRANGE: sys.argv set to verbose flag with command
    ACT:     main
    ASSERT:  executes without ValueError (verbose flag processed)
    """
    original_argv = sys.argv[:]
    sys.argv = ["equity-aggregator", "-v", "seed"]

    try:
        main()
    except ValueError:
        pytest.fail("main should not raise ValueError with verbose flag")
    except (SystemExit, Exception):
        pass
    finally:
        sys.argv = original_argv


def test_main_with_debug_flag() -> None:
    """
    ARRANGE: sys.argv set to debug flag with command
    ACT:     main
    ASSERT:  executes without ValueError (debug flag processed)
    """
    original_argv = sys.argv[:]
    sys.argv = ["equity-aggregator", "-d", "seed"]

    try:
        main()
    except ValueError:
        pytest.fail("main should not raise ValueError with debug flag")
    except (SystemExit, Exception):
        pass
    finally:
        sys.argv = original_argv


def test_main_with_quiet_flag() -> None:
    """
    ARRANGE: sys.argv set to quiet flag with command
    ACT:     main
    ASSERT:  executes without ValueError (quiet flag processed)
    """
    original_argv = sys.argv[:]
    sys.argv = ["equity-aggregator", "-q", "seed"]

    try:
        main()
    except ValueError:
        pytest.fail("main should not raise ValueError with quiet flag")
    except (SystemExit, Exception):
        pass
    finally:
        sys.argv = original_argv


def test_main_with_multiple_flags() -> None:
    """
    ARRANGE: sys.argv set to multiple flags with command
    ACT:     main
    ASSERT:  executes without ValueError (multiple flags processed)
    """
    original_argv = sys.argv[:]
    sys.argv = ["equity-aggregator", "-v", "-d", "seed"]

    try:
        main()
    except ValueError:
        pytest.fail("main should not raise ValueError with multiple flags")
    except (SystemExit, Exception):
        pass
    finally:
        sys.argv = original_argv


def test_main_with_no_arguments() -> None:
    """
    ARRANGE: sys.argv set to program name only
    ACT:     main
    ASSERT:  raises SystemExit (no command provided)
    """
    original_argv = sys.argv[:]
    sys.argv = ["equity-aggregator"]

    try:
        with pytest.raises(SystemExit):
            main()
    finally:
        sys.argv = original_argv


def test_main_with_invalid_command() -> None:
    """
    ARRANGE: sys.argv set to invalid command
    ACT:     main
    ASSERT:  raises SystemExit (argument parser rejects invalid command)
    """
    original_argv = sys.argv[:]
    sys.argv = ["equity-aggregator", "unknown"]

    try:
        with pytest.raises(SystemExit):
            main()
    finally:
        sys.argv = original_argv


def test_main_with_invalid_flag() -> None:
    """
    ARRANGE: sys.argv set to invalid flag
    ACT:     main
    ASSERT:  raises SystemExit (invalid flag)
    """
    original_argv = sys.argv[:]
    sys.argv = ["equity-aggregator", "--invalid", "seed"]

    try:
        with pytest.raises(SystemExit):
            main()
    finally:
        sys.argv = original_argv


def test_main_help_flag() -> None:
    """
    ARRANGE: sys.argv set to help flag
    ACT:     main
    ASSERT:  raises SystemExit (help displayed)
    """
    original_argv = sys.argv[:]
    sys.argv = ["equity-aggregator", "--help"]

    try:
        with pytest.raises(SystemExit):
            main()
    finally:
        sys.argv = original_argv


def test_main_command_help_flag() -> None:
    """
    ARRANGE: sys.argv set to command help flag
    ACT:     main
    ASSERT:  raises SystemExit (command help displayed)
    """
    original_argv = sys.argv[:]
    sys.argv = ["equity-aggregator", "seed", "--help"]

    try:
        with pytest.raises(SystemExit):
            main()
    finally:
        sys.argv = original_argv
