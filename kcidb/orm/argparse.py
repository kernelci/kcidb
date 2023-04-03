"""
KCIDB object-relational mapping (ORM)-management
command-line argument parsing
"""

import argparse
import kcidb.misc
import kcidb.argparse
from kcidb.orm.query import Pattern

# We'll get to it, pylint: disable=too-many-lines


class PatternHelpAction(argparse.Action):
    """Argparse action outputting pattern string help and exiting."""
    def __init__(self,
                 option_strings,
                 dest=argparse.SUPPRESS,
                 default=argparse.SUPPRESS,
                 help=None):
        super().__init__(
            option_strings=option_strings,
            dest=dest,
            default=default,
            nargs=0,
            help=help)

    def __call__(self, parser, namespace, values, option_string=None):
        print(
            Pattern.STRING_DOC +
            "\n" +
            "NOTE: Specifying object ID lists separately is not "
            "supported using\n"
            "      command-line tools. "
            "Only inline ID lists are supported.\n"
        )
        parser.exit()


def add_args(parser):
    """
    Add common ORM arguments to an argument parser.

    Args:
        The parser to add arguments to.
    """
    parser.add_argument(
        "pattern_strings",
        nargs='*',
        default=[],
        metavar="PATTERN",
        help="Object-matching pattern. "
             "See pattern documentation with --pattern-help."
    )
    parser.add_argument(
        "--pattern-help",
        action=PatternHelpAction,
        help="Print pattern string documentation and exit."
    )


class ArgumentParser(kcidb.argparse.ArgumentParser):
    """
    Command-line argument parser with common ORM arguments added.
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize the parser, adding common ORM arguments.

        Args:
            args:   Positional arguments to initialize ArgumentParser with.
            kwargs: Keyword arguments to initialize ArgumentParser with.
        """
        super().__init__(*args, **kwargs)
        add_args(self)


class OutputArgumentParser(kcidb.argparse.OutputArgumentParser):
    """
    Command-line argument parser for tools outputting JSON,
    with common ORM arguments added.
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize the parser, adding JSON output arguments.

        Args:
            args:   Positional arguments to initialize ArgumentParser with.
            kwargs: Keyword arguments to initialize ArgumentParser with.
        """
        super().__init__(*args, **kwargs)
        add_args(self)
