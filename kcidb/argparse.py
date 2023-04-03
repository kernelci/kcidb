"""KCIDB general command-line argument parsing"""

import math
import re
import os
import argparse
import logging
import dateutil.parser
try:  # Python 3.9
    from importlib import metadata
except ImportError:  # Python 3.6
    import importlib_metadata as metadata
import kcidb.io as io
from kcidb.misc import logging_setup, LOGGING_LEVEL_MAP


# Check light assertions only, if True
LIGHT_ASSERTS = not os.environ.get("KCIDB_HEAVY_ASSERTS", "")


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


def iso_timestamp(string):
    """
    Parse an ISO-8601 timestamp out of a string, assuming local timezone, if
    not specified. Matches the argparse type function interface.

    Args:
        string: The string to parse.

    Returns:
        The timestamp parsed out of the string.

    Raises:
        argparse.ArgumentTypeError: the string wasn't representing an ISO-8601
        timestamp.
    """
    try:
        value = dateutil.parser.isoparse(string)
        if value.tzinfo is None:
            value = value.astimezone()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f'{repr(string)} is not an ISO-8601 timestamp'
        ) from exc
    return value


def version(string):
    """
    Parse a version string into a tuple with major and minor version numbers
    (both non-negative integers). Matches the argparse type function
    interface.

    Args:
        string: The string representing the version to parse.

    Returns:
        A tuple containing major and minor version numbers.

    Raises:
        ValueError if the string was not representing a version.
    """
    match = re.fullmatch(r"([0-9]+)\.([0-9]+)", string)
    if not match:
        raise argparse.ArgumentTypeError(
            f"Invalid version: {string!r}"
        )
    return int(match.group(1)), int(match.group(2))


def output_add_args(parser):
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
        output_add_args(self)


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


def schema_add_args(parser, version_verb):
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
