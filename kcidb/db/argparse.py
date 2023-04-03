"""KCIDB database-management command-line argument parsing"""

import argparse
import kcidb.orm
import kcidb.misc
import kcidb.argparse


class DBHelpAction(argparse.Action):
    """Argparse action outputting database string help and exiting."""
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
        print("KCIDB has several database drivers for both actual and "
              "virtual databases.\n"
              "You can specify a particular driver to use, and its "
              "parameters, using the\n"
              "-d/--database option.\n"
              "\n"
              "The format of the option value is <DRIVER>[:<PARAMS>], "
              "where <DRIVER> is the\n"
              "name of the driver, and <PARAMS> is a (sometimes optional) "
              "driver-specific\n"
              "parameter string.\n"
              "\n"
              "For example, \"-d bigquery:kernelci-production.kcidb_01\" "
              "requests the use of\n"
              "the \"bigquery\" database driver with the parameter string\n"
              "\"kernelci-production.kcidb_01\", from which the driver "
              "extracts the Google\n"
              "Cloud project \"kernelci-production\" and the dataset "
              "\"kcidb_01\" to connect to.\n"
              "\n"
              "Available drivers and format of their parameter strings "
              "follow.\n")
        for name, driver in parser.driver_types.items():
            print(f"\n{name!r} driver\n" +
                  "-" * (len(name) + 9) + "\n" +
                  driver.get_doc())
        parser.exit()


def add_args(parser, database=None):
    """
    Add common database arguments to an argument parser.

    Args:
        parser:     The parser to add arguments to.
        database:   The default database specification to use.
    """
    assert database is None or isinstance(database, str)
    parser.add_argument(
        '-d', '--database',
        help=("Specify DATABASE to use, formatted as <DRIVER>:<PARAMS>. " +
              "Use --database-help for more details." +
              ("" if database is None
               else f"Default is {database!r}.")),
        default=database,
        required=(database is None)
    )
    parser.add_argument(
        '--database-help',
        action=DBHelpAction,
        help='Print documentation on database specification strings and exit.'
    )


class ArgumentParser(kcidb.argparse.ArgumentParser):
    """
    Command-line argument parser with common database arguments added.
    """

    def __init__(self, driver_types, *args, database=None, **kwargs):
        """
        Initialize the parser, adding common database arguments.

        Args:
            driver_types:   A dictionary of known driver names and types
            args:           Positional arguments to initialize
                            ArgumentParser with.
            database:       The default database specification to use,
                            or None to make database specification required.
            kwargs:         Keyword arguments to initialize ArgumentParser
                            with.
        """
        assert isinstance(driver_types, dict)
        assert all(isinstance(key, str) for key in driver_types.keys())
        assert all(isinstance(value, type) for value in driver_types.values())
        self.driver_types = driver_types
        super().__init__(*args, **kwargs)
        add_args(self, database=database)


class OutputArgumentParser(kcidb.argparse.OutputArgumentParser):
    """
    Command-line argument parser for tools outputting JSON,
    with common database arguments added.
    """

    def __init__(self, driver_types, *args, database=None, **kwargs):
        """
        Initialize the parser, adding JSON output arguments.

        Args:
            driver_types:   A dictionary of known driver names and types
            args:           Positional arguments to initialize
                            ArgumentParser with.
            database:       The default database specification to use,
                            or None to make database specification required.
            kwargs:         Keyword arguments to initialize ArgumentParser
                            with.
        """
        assert isinstance(driver_types, dict)
        assert all(isinstance(key, str) for key in driver_types.keys())
        assert all(isinstance(value, type) for value in driver_types.values())
        self.driver_types = driver_types
        super().__init__(*args, **kwargs)
        add_args(self, database=database)


class SplitOutputArgumentParser(kcidb.argparse.SplitOutputArgumentParser):
    """
    Command-line argument parser for tools outputting split-report streams,
    with common database arguments added.
    """

    def __init__(self, driver_types, *args, database=None, **kwargs):
        """
        Initialize the parser, adding split-report output arguments.

        Args:
            driver_types:   A dictionary of known driver names and types
            args:           Positional arguments to initialize
                            ArgumentParser with.
            database:       The default database specification to use,
                            or None to make database specification required.
            kwargs:         Keyword arguments to initialize ArgumentParser
                            with.
        """
        assert isinstance(driver_types, dict)
        assert all(isinstance(key, str) for key in driver_types.keys())
        assert all(isinstance(value, type) for value in driver_types.values())
        self.driver_types = driver_types
        super().__init__(*args, **kwargs)
        add_args(self, database=database)


# No, it's OK, pylint: disable=too-many-ancestors
class QueryArgumentParser(SplitOutputArgumentParser):
    """
    Command-line argument parser with common database query arguments added.
    """

    def __init__(self, driver_types, *args, **kwargs):
        """
        Initialize the parser, adding common database query arguments.

        Args:
            driver_types:   A dictionary of known driver names and types
            args:           Positional arguments to initialize
                            ArgumentParser with.
            kwargs:         Keyword arguments to initialize ArgumentParser
                            with.
        """
        assert isinstance(driver_types, dict)
        assert all(isinstance(key, str) for key in driver_types.keys())
        assert all(isinstance(value, type) for value in driver_types.values())
        self.driver_types = driver_types
        super().__init__(self.driver_types, *args, **kwargs)

        self.add_argument(
            '-c', '--checkout-id',
            metavar="ID",
            default=[],
            help='ID of a checkout to match',
            dest="checkout_ids",
            action='append',
        )
        self.add_argument(
            '-b', '--build-id',
            metavar="ID",
            default=[],
            help='ID of a build to match',
            dest="build_ids",
            action='append',
        )
        self.add_argument(
            '-t', '--test-id',
            metavar="ID",
            default=[],
            help='ID of a test to match',
            dest="test_ids",
            action='append',
        )
        self.add_argument(
            '-i', '--issue-id',
            metavar="ID",
            default=[],
            help='ID of an issue to match',
            dest="issue_ids",
            action='append',
        )
        self.add_argument(
            '-n', '--incident-id',
            metavar="ID",
            default=[],
            help='ID of an incident to match',
            dest="incident_ids",
            action='append',
        )
        self.add_argument(
            '--parents',
            help='Match parents of matching objects',
            action='store_true'
        )
        self.add_argument(
            '--children',
            help='Match children of matching objects',
            action='store_true'
        )
