# cli/test_main.py

import argparse
import sys

import pytest

from equity_aggregator.cli.main import main

pytestmark = pytest.mark.unit


def test_main_with_valid_seed_command() -> None:
    """
    ARRANGE: sys.argv set to seed command with no-op dispatcher
    ACT:     main
    ASSERT:  dispatcher is called with correct command
    """
    original_argv = sys.argv[:]
    sys.argv = ["equity-aggregator", "seed"]
    dispatched_cmd = None

    def noop_dispatcher(args: argparse.Namespace) -> None:
        nonlocal dispatched_cmd
        dispatched_cmd = args.cmd

    try:
        main(dispatcher=noop_dispatcher)
    finally:
        sys.argv = original_argv

    assert dispatched_cmd == "seed"


def test_main_with_valid_export_command() -> None:
    """
    ARRANGE: sys.argv set to export command with no-op dispatcher
    ACT:     main
    ASSERT:  dispatcher is called with correct command
    """
    original_argv = sys.argv[:]
    sys.argv = ["equity-aggregator", "export", "--output-dir", "/tmp/test"]
    dispatched_cmd = None

    def noop_dispatcher(args: argparse.Namespace) -> None:
        nonlocal dispatched_cmd
        dispatched_cmd = args.cmd

    try:
        main(dispatcher=noop_dispatcher)
    finally:
        sys.argv = original_argv

    assert dispatched_cmd == "export"


def test_main_with_valid_download_command() -> None:
    """
    ARRANGE: sys.argv set to download command with no-op dispatcher
    ACT:     main
    ASSERT:  dispatcher is called with correct command
    """
    original_argv = sys.argv[:]
    sys.argv = ["equity-aggregator", "download"]
    dispatched_cmd = None

    def noop_dispatcher(args: argparse.Namespace) -> None:
        nonlocal dispatched_cmd
        dispatched_cmd = args.cmd

    try:
        main(dispatcher=noop_dispatcher)
    finally:
        sys.argv = original_argv

    assert dispatched_cmd == "download"


def test_main_with_verbose_flag() -> None:
    """
    ARRANGE: sys.argv set to verbose flag with command
    ACT:     main
    ASSERT:  dispatcher receives args with verbose flag set
    """
    original_argv = sys.argv[:]
    sys.argv = ["equity-aggregator", "-v", "seed"]
    received_args = None

    def noop_dispatcher(args: argparse.Namespace) -> None:
        nonlocal received_args
        received_args = args

    try:
        main(dispatcher=noop_dispatcher)
    finally:
        sys.argv = original_argv

    assert received_args.verbose is True


def test_main_with_debug_flag() -> None:
    """
    ARRANGE: sys.argv set to debug flag with command
    ACT:     main
    ASSERT:  dispatcher receives args with debug flag set
    """
    original_argv = sys.argv[:]
    sys.argv = ["equity-aggregator", "-d", "seed"]
    received_args = None

    def noop_dispatcher(args: argparse.Namespace) -> None:
        nonlocal received_args
        received_args = args

    try:
        main(dispatcher=noop_dispatcher)
    finally:
        sys.argv = original_argv

    assert received_args.debug is True


def test_main_with_quiet_flag() -> None:
    """
    ARRANGE: sys.argv set to quiet flag with command
    ACT:     main
    ASSERT:  dispatcher receives args with quiet flag set
    """
    original_argv = sys.argv[:]
    sys.argv = ["equity-aggregator", "-q", "seed"]
    received_args = None

    def noop_dispatcher(args: argparse.Namespace) -> None:
        nonlocal received_args
        received_args = args

    try:
        main(dispatcher=noop_dispatcher)
    finally:
        sys.argv = original_argv

    assert received_args.quiet is True


def test_main_with_multiple_flags() -> None:
    """
    ARRANGE: sys.argv set to multiple flags with command
    ACT:     main
    ASSERT:  dispatcher receives args with both flags set
    """
    original_argv = sys.argv[:]
    sys.argv = ["equity-aggregator", "-v", "-d", "seed"]
    received_args = None

    def noop_dispatcher(args: argparse.Namespace) -> None:
        nonlocal received_args
        received_args = args

    try:
        main(dispatcher=noop_dispatcher)
    finally:
        sys.argv = original_argv

    assert received_args.verbose is True


def test_main_with_multiple_flags_debug_set() -> None:
    """
    ARRANGE: sys.argv set to multiple flags with command
    ACT:     main
    ASSERT:  dispatcher receives args with debug flag set
    """
    original_argv = sys.argv[:]
    sys.argv = ["equity-aggregator", "-v", "-d", "seed"]
    received_args = None

    def noop_dispatcher(args: argparse.Namespace) -> None:
        nonlocal received_args
        received_args = args

    try:
        main(dispatcher=noop_dispatcher)
    finally:
        sys.argv = original_argv

    assert received_args.debug is True


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
            main(dispatcher=lambda _: None)
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
            main(dispatcher=lambda _: None)
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
            main(dispatcher=lambda _: None)
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
            main(dispatcher=lambda _: None)
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
            main(dispatcher=lambda _: None)
    finally:
        sys.argv = original_argv


def test_main_export_output_dir_passed_to_dispatcher() -> None:
    """
    ARRANGE: sys.argv set to export command with output-dir
    ACT:     main
    ASSERT:  dispatcher receives args with output_dir set
    """
    original_argv = sys.argv[:]
    sys.argv = ["equity-aggregator", "export", "--output-dir", "/custom/path"]
    received_args = None

    def noop_dispatcher(args: argparse.Namespace) -> None:
        nonlocal received_args
        received_args = args

    try:
        main(dispatcher=noop_dispatcher)
    finally:
        sys.argv = original_argv

    assert received_args.output_dir == "/custom/path"


def test_main_uses_default_dispatcher_when_none_provided() -> None:
    """
    ARRANGE: sys.argv set to help flag, no dispatcher provided
    ACT:     main (without dispatcher argument)
    ASSERT:  raises SystemExit (help displayed, default dispatcher assigned)
    """
    original_argv = sys.argv[:]
    sys.argv = ["equity-aggregator", "--help"]

    try:
        with pytest.raises(SystemExit):
            main()
    finally:
        sys.argv = original_argv
