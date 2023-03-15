import math
import re
import os
import atexit
import tempfile
import sys
import traceback
import itertools
import argparse
import logging
import json
from textwrap import indent
import dateutil.parser
try:  # Python 3.9
    from importlib import metadata
except ImportError:  # Python 3.6
    import importlib_metadata as metadata
from google.cloud import secretmanager
import jq
import kcidb.io as io

# Module's logger
LOGGER = logging.getLogger(__name__)

# A dictionary of names of logging levels and their values
LOGGING_LEVEL_MAP = {
    name: value
    for name, value in logging.__dict__.items()
    if name.isalpha() and name.isupper() and isinstance(value, int) and value
}
# Logging level disabling all logging
LOGGING_LEVEL_MAP["NONE"] = max(LOGGING_LEVEL_MAP.values()) + 1
# Sort levels highest->lowest
# I don't see it, pylint: disable=unnecessary-comprehension
LOGGING_LEVEL_MAP = {
    k: v
    for k, v in sorted(LOGGING_LEVEL_MAP.items(),
                       key=lambda i: i[1], reverse=True)
}


def logging_setup(level):
    """
    Setup logging: set root logger log level and disable irrelevant logging.

    Args:
        level:  Logging level for the root logger.
    """
    assert isinstance(level, int)
    logging.getLogger().setLevel(level)
    # TODO Consider separate arguments for controlling the below
    logging.getLogger("urllib3").setLevel(LOGGING_LEVEL_MAP["NONE"])
    logging.getLogger("google").setLevel(LOGGING_LEVEL_MAP["NONE"])


class ArgumentParser(argparse.ArgumentParser):
    """
    KCIDB command-line argument parser handling common arguments.
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize the parser, adding common arguments.

        Args:
            args:   Positional arguments to initialize ArgumentParser with.
            kwargs: Keyword arguments to initialize ArgumentParser with.
        """
        super().__init__(*args, **kwargs)
        self.add_argument(
            '--version',
            action='version',
            version=f"Version {metadata.version('kcidb')}"
        )
        self.add_argument(
            '-l', '--log-level',
            metavar="LEVEL",
            default="NONE",
            choices=LOGGING_LEVEL_MAP.keys(),
            help='Limit logging to LEVEL (%(choices)s). Default is NONE.'
        )

    def parse_args(self, args=None, namespace=None):
        """
        Parse arguments, including common ones, apply ones affecting global
        state.

        Args:
            args:       List of strings to parse. The default is taken from
                        sys.argv.
            namespace:  An object to take the attributes. The default is a new
                        empty argparse.Namespace object.

        Returns:
            Namespace populated with arguments.
        """
        args = super().parse_args(args=args, namespace=namespace)
        logging.basicConfig()
        logging_setup(LOGGING_LEVEL_MAP[args.log_level])
        return args


def non_negative_int(string):
    """
    Parse a non-negative integer out of a string.
    Matches the argparse type function interface.

    Args:
        string: The string to parse.

    Returns:
        The non-negative integer parsed out of the string.

    Raises:
        argparse.ArgumentTypeError: the string wasn't representing a
        non-negative integer.
    """
    if not re.fullmatch("[0-9]+", string):
        raise argparse.ArgumentTypeError(
            f'{repr(string)} is not a positive integer, nor zero'
        )
    return int(string)


def non_negative_int_or_inf(string):
    """
    Parse a non-negative integer or a positive infinity out of a string.
    Matches the argparse type function interface.

    Args:
        string: The string to parse.

    Returns:
        The non-negative integer, or positive infinity parsed out of the
        string.

    Raises:
        argparse.ArgumentTypeError: the string wasn't representing a
        non-negative integer or infinity.
    """
    try:
        value = float(string)
        if value != math.inf:
            value = non_negative_int(string)
    except (ValueError, argparse.ArgumentTypeError) as exc:
        raise argparse.ArgumentTypeError(
            f'{repr(string)} is not zero, a positive integer, or infinity'
        ) from exc
    return value


def argparse_output_add_args(parser):
    """
    Add JSON output arguments to a command-line argument parser.

    Args:
        parser: The parser to add arguments to.
    """
    parser.add_argument(
        '--indent',
        metavar="NUMBER",
        type=non_negative_int,
        help='Pretty-print JSON using NUMBER of spaces for indenting. '
             'Print single-line if zero. Default is 4.',
        default=4,
        required=False
    )
    parser.add_argument(
        '--seq',
        help='Prefix JSON output with the RS character, to match '
             'RFC 7464 and "application/json-seq" media type.',
        action='store_true'
    )


class OutputArgumentParser(ArgumentParser):
    """
    Command-line argument parser for tools outputting JSON.
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize the parser, adding JSON output arguments.

        Args:
            args:   Positional arguments to initialize ArgumentParser with.
            kwargs: Keyword arguments to initialize ArgumentParser with.
        """
        super().__init__(*args, **kwargs)
        argparse_output_add_args(self)


class SplitOutputArgumentParser(OutputArgumentParser):
    """
    Command-line argument parser for tools supporting split-report output.
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize the parser, adding split-report output arguments.

        Args:
            args:   Positional arguments to initialize ArgumentParser with.
            kwargs: Keyword arguments to initialize ArgumentParser with.
        """
        super().__init__(*args, **kwargs)
        self.add_argument(
            '-o', '--objects-per-report',
            metavar="NUMBER",
            type=non_negative_int,
            help='Put maximum NUMBER of objects into each output '
                 'report, or all, if zero. Default is zero.',
            default=0,
            required=False
        )


def argparse_schema_add_args(parser, version_verb):
    """
    Add schema selection arguments to a command-line argument parser.

    Args:
        parser:         The parser to add arguments to.
        version_verb:   The verb to apply to the schema in the version
                        argument description.
    """
    def schema_version(string):
        """
        Lookup a schema version object using a major version number string.
        Matches the argparse type function interface.

        Args:
            string: The string representing the major version number of the
                    schema to lookup.

        Returns:
            The looked up schema version object.

        Raises:
            ValueError if the string was not representing a positive integer
            number, or the schema with the supplied major version number was
            not found.
        """
        if not re.fullmatch("0*[1-9][0-9]*", string):
            raise argparse.ArgumentTypeError(
                f"Invalid major version number: {string!r}"
            )
        major = int(string)
        # It's OK, pylint: disable=redefined-outer-name
        version = io.SCHEMA
        while version and version.major != major:
            version = version.previous
        if version is None:
            raise argparse.ArgumentTypeError(
                f"No schema version found for major number {major}"
            )
        return version

    parser.add_argument(
        'schema_version',
        metavar="SCHEMA_VERSION",
        type=schema_version,
        help=f"{version_verb.capitalize()} the schema with the specified "
        f"major version. Default is the current schema version "
        f"(currently {io.SCHEMA.major}).",
        nargs='?',
        default=io.SCHEMA
    )
