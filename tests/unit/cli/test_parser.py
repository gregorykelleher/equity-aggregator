# cli/test_parser.py

import argparse

import pytest

from equity_aggregator.cli.parser import create_parser

pytestmark = pytest.mark.unit


def test_create_parser_returns_argument_parser() -> None:
    """
    ARRANGE: no arguments
    ACT:     create_parser
    ASSERT:  returns ArgumentParser instance
    """
    parser = create_parser()

    assert isinstance(parser, argparse.ArgumentParser)


def test_create_parser_sets_program_name() -> None:
    """
    ARRANGE: no arguments
    ACT:     create_parser
    ASSERT:  parser has correct program name
    """
    parser = create_parser()

    assert parser.prog == "equity-aggregator"


def test_create_parser_sets_description() -> None:
    """
    ARRANGE: no arguments
    ACT:     create_parser
    ASSERT:  parser has correct description
    """
    parser = create_parser()

    assert "aggregate, download, and export canonical equity data" in parser.description


def test_create_parser_sets_epilog() -> None:
    """
    ARRANGE: no arguments
    ACT:     create_parser
    ASSERT:  parser has correct epilog
    """
    parser = create_parser()

    assert "use 'equity-aggregator <command> --help' for help" in parser.epilog


def test_parser_accepts_verbose_short_flag() -> None:
    """
    ARRANGE: parser and verbose short flag argument
    ACT:     parse_args with -v
    ASSERT:  verbose flag is True
    """
    parser = create_parser()

    args = parser.parse_args(["-v", "seed"])

    assert args.verbose is True


def test_parser_accepts_verbose_long_flag() -> None:
    """
    ARRANGE: parser and verbose long flag argument
    ACT:     parse_args with --verbose
    ASSERT:  verbose flag is True
    """
    parser = create_parser()

    args = parser.parse_args(["--verbose", "seed"])

    assert args.verbose is True


def test_parser_accepts_debug_short_flag() -> None:
    """
    ARRANGE: parser and debug short flag argument
    ACT:     parse_args with -d
    ASSERT:  debug flag is True
    """
    parser = create_parser()

    args = parser.parse_args(["-d", "seed"])

    assert args.debug is True


def test_parser_accepts_debug_long_flag() -> None:
    """
    ARRANGE: parser and debug long flag argument
    ACT:     parse_args with --debug
    ASSERT:  debug flag is True
    """
    parser = create_parser()

    args = parser.parse_args(["--debug", "seed"])

    assert args.debug is True


def test_parser_accepts_quiet_short_flag() -> None:
    """
    ARRANGE: parser and quiet short flag argument
    ACT:     parse_args with -q
    ASSERT:  quiet flag is True
    """
    parser = create_parser()

    args = parser.parse_args(["-q", "seed"])

    assert args.quiet is True


def test_parser_accepts_quiet_long_flag() -> None:
    """
    ARRANGE: parser and quiet long flag argument
    ACT:     parse_args with --quiet
    ASSERT:  quiet flag is True
    """
    parser = create_parser()

    args = parser.parse_args(["--quiet", "seed"])

    assert args.quiet is True


def test_parser_accepts_seed_command() -> None:
    """
    ARRANGE: parser and seed command
    ACT:     parse_args with seed
    ASSERT:  cmd is set to seed
    """
    parser = create_parser()

    args = parser.parse_args(["seed"])

    assert args.cmd == "seed"


def test_parser_accepts_export_command() -> None:
    """
    ARRANGE: parser and export command with required output-dir
    ACT:     parse_args with export and --output-dir
    ASSERT:  cmd is set to export and output_dir is set
    """
    parser = create_parser()

    args = parser.parse_args(["export", "--output-dir", "/tmp/test"])

    assert args.cmd == "export"
    assert args.output_dir == "/tmp/test"


def test_parser_accepts_download_command() -> None:
    """
    ARRANGE: parser and download command
    ACT:     parse_args with download
    ASSERT:  cmd is set to download
    """
    parser = create_parser()

    args = parser.parse_args(["download"])

    assert args.cmd == "download"


def test_parser_verbose_flag_defaults_false() -> None:
    """
    ARRANGE: parser with only command
    ACT:     parse_args with just seed
    ASSERT:  verbose flag defaults to False
    """
    parser = create_parser()

    args = parser.parse_args(["seed"])

    assert args.verbose is False


def test_parser_debug_flag_defaults_false() -> None:
    """
    ARRANGE: parser with only command
    ACT:     parse_args with just seed
    ASSERT:  debug flag defaults to False
    """
    parser = create_parser()

    args = parser.parse_args(["seed"])

    assert args.debug is False


def test_parser_quiet_flag_defaults_false() -> None:
    """
    ARRANGE: parser with only command
    ACT:     parse_args with just seed
    ASSERT:  quiet flag defaults to False
    """
    parser = create_parser()

    args = parser.parse_args(["seed"])

    assert args.quiet is False


def test_parser_requires_command() -> None:
    """
    ARRANGE: parser with no command
    ACT:     parse_args with no arguments
    ASSERT:  raises SystemExit
    """
    parser = create_parser()

    with pytest.raises(SystemExit):
        parser.parse_args([])


def test_parser_rejects_unknown_command() -> None:
    """
    ARRANGE: parser with unknown command
    ACT:     parse_args with invalid command
    ASSERT:  raises SystemExit
    """
    parser = create_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["unknown"])


def test_parser_rejects_unknown_flag() -> None:
    """
    ARRANGE: parser with unknown flag
    ACT:     parse_args with invalid flag
    ASSERT:  raises SystemExit
    """
    parser = create_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["--unknown", "seed"])


def test_parser_multiple_flags_with_command() -> None:
    """
    ARRANGE: parser with multiple flags and command
    ACT:     parse_args with -v -d seed
    ASSERT:  cmd is set correctly
    """
    parser = create_parser()

    args = parser.parse_args(["-v", "-d", "seed"])

    assert args.cmd == "seed"


def test_parser_multiple_flags_verbose_set() -> None:
    """
    ARRANGE: parser with multiple flags and command
    ACT:     parse_args with -v -d seed
    ASSERT:  verbose flag is True
    """
    parser = create_parser()

    args = parser.parse_args(["-v", "-d", "seed"])

    assert args.verbose is True


def test_parser_multiple_flags_debug_set() -> None:
    """
    ARRANGE: parser with multiple flags and command
    ACT:     parse_args with -v -d seed
    ASSERT:  debug flag is True
    """
    parser = create_parser()

    args = parser.parse_args(["-v", "-d", "seed"])

    assert args.debug is True
