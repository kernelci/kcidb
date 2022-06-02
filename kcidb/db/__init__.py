"""Kernel CI report database"""

import sys
import logging
import argparse
import textwrap
import datetime
import kcidb.io as io
import kcidb.orm
import kcidb.misc
from kcidb.misc import LIGHT_ASSERTS
from kcidb.db import bigquery, postgresql, sqlite, json, null, misc

# Module's logger
LOGGER = logging.getLogger(__name__)


class MuxDriver(misc.Driver):
    """Kernel CI multiplexing database driver"""

    DOC = textwrap.dedent("""\
        The mux driver allows loading data into multiple databases at once,
        and querying one of them.

        Parameters: <DATABASES>

        <DATABASES> A whitespace-separated list of strings describing the
                    multiplexed databases (<DRIVER>[:<PARAMS>] pairs). Any
                    spaces or backslashes in database strings need to be
                    escaped with backslashes. Each database will receive the
                    loaded data, but only the first one will be queried.

                    Example: postgresql bigquery:kcidb_01
    """)

    @staticmethod
    def databases(params):
        """
        Create a generator parsing and returning database strings for member
        drivers from the driver's parameter string. Account for and remove
        backslash-escaping.

        Args:
            params: The parameter string to parse.

        Returns:
            The generator returning database strings for member drivers.
        """
        assert isinstance(params, str)

        escape = False
        database = ""
        for char in params:
            if char == "\\":
                escape = True
                continue
            if escape:
                database += char
                escape = False
                continue
            if char.isspace():
                if database:
                    yield database
                    database = ""
                continue
            database += char

        if escape:
            raise Exception(
                f"Incomplete escape sequence at the end of params {params!r}"
            )

        if database:
            yield database

    # Yes, parent is an abstract class, pylint: disable=super-init-not-called
    def __init__(self, params):
        """
        Initialize the multiplexing driver.

        Args:
            params: A parameter string describing the databases to
                    access. See MuxDriver.DOC for documentation.
                    Cannot be None (must be specified).

        Raises:
            NotFound            - if a database does not exist;
            IncompatibleSchema  - if a database is not empty and its schema
                                  is incompatible with the current I/O schema.
        """
        assert params is None or isinstance(params, str)
        if params is None:
            raise Exception("Database parameters must be specified\n\n" +
                            MuxDriver.DOC)
        self.drivers = [
            _driver_create(db) for db in MuxDriver.databases(params)
        ]

    def is_initialized(self):
        """
        Check if at least one database is initialized (not empty).

        Returns:
            True if at least one database is initialized,
            False if all are not.
        """
        return any(driver.is_initialized() for driver in self.drivers)

    def get_schema_version(self):
        """
        Get the lowest version of the I/O schemas the database schemas
        correspond to.

        Returns:
            Major and minor version numbers.
        """
        return min(driver.get_schema_version() for driver in self.drivers)

    def init(self):
        """
        Initialize the member databases.
        All the databases must be empty (uninitialized).
        """
        for driver in self.drivers:
            driver.init()

    def cleanup(self):
        """
        Cleanup (empty) the databases, removing all data.
        All the databases must be initialized (not empty).
        """
        for driver in self.drivers:
            driver.cleanup()

    def get_last_modified(self):
        """
        Get the latest time the data in the databases was last modified.
        All the databases must be initialized (not empty).

        Returns:
            The datetime object representing the last modification time.
        """
        return max(driver.get_last_modified() for driver in self.drivers)

    def dump_iter(self, objects_per_report):
        """
        Dump all data from the first database in object number-limited chunks.

        Args:
            objects_per_report: An integer number of objects per each returned
                                report data, or zero for no limit.

        Returns:
            An iterator returning report JSON data adhering to the current I/O
            schema version, each containing at most the specified number of
            objects.
        """
        yield from self.drivers[0].dump_iter(objects_per_report)

    # We can live with this for now, pylint: disable=too-many-arguments
    def query_iter(self, ids, children, parents, objects_per_report):
        """
        Match and fetch objects from the first database, in object
        number-limited chunks.

        Args:
            ids:                A dictionary of object list names, and lists
                                of IDs of objects to match. None means empty
                                dictionary.
            children:           True if children of matched objects should be
                                matched as well.
            parents:            True if parents of matched objects should be
                                matched as well.
            objects_per_report: An integer number of objects per each returned
                                report data, or zero for no limit.

        Returns:
            An iterator returning report JSON data adhering to the current I/O
            schema version, each containing at most the specified number of
            objects.
        """
        yield from self.drivers[0].query_iter(
            ids, children, parents, objects_per_report
        )

    def oo_query(self, pattern_set):
        """
        Query raw object-oriented data from the first database.

        Args:
            pattern_set:    A set of patterns ("kcidb.oo.data.Pattern"
                            instances) matching objects to fetch.
        Returns:
            A dictionary of object type names and lists containing retrieved
            objects of the corresponding type.
        """
        return self.drivers[0].oo_query(pattern_set)

    def load(self, data):
        """
        Load data into the databases.

        Args:
            data:   The JSON data to load into the databases.
                    Must adhere to the current version of I/O schema.
        """
        for driver in self.drivers:
            driver.load(data)


# A dictionary of known driver names and types
DRIVER_TYPES = dict(
    bigquery=bigquery.Driver,
    postgresql=postgresql.Driver,
    sqlite=sqlite.Driver,
    json=json.Driver,
    null=null.Driver,
    mux=MuxDriver,
)


def _driver_create(database):
    """
    Create an instance of a Kernel CI report database driver accessing a
    database.

    Args:
        database:   A string specifying the database to access, formatted
                    as "<DRIVER>:<PARAMS>" or just "<DRIVER>". Where
                    "<DRIVER>" is the driver name, and "<PARAMS>" is the
                    optional driver-specific database parameter string.

    Return:
        The created instance.

    Raises:
        NotFound            - if the database does not exist;
        IncompatibleSchema  - if the database is not empty and its schema
                              is incompatible with the current I/O schema.
    """
    assert isinstance(database, str)
    try:
        colon_pos = database.index(":")
        driver_name = database[:colon_pos]
        driver_params = database[colon_pos + 1:]
    except ValueError:
        driver_name = database
        driver_params = None
    try:
        driver_type = DRIVER_TYPES[driver_name]
    except KeyError:
        raise Exception(f"Unknown driver {driver_name!r} in database "
                        f"specification: {database!r}") from None
    try:
        driver = driver_type(driver_params)
    except misc.NotFound:
        raise
    except Exception as exc:
        raise Exception(
            f"Failed connecting to {driver_name!r} database"
        ) from exc
    if driver.is_initialized():
        major, minor = driver.get_schema_version()
        if major != io.SCHEMA.major:
            raise misc.IncompatibleSchema(major, minor)
    return driver


class Client(kcidb.orm.Source):
    """Kernel CI report database client"""

    def __init__(self, database):
        """
        Initialize the Kernel CI report database client.

        Args:
            database:   A string specifying the database to access, formatted
                        as "<DRIVER>:<PARAMS>" or just "<DRIVER>". Where
                        "<DRIVER>" is the driver name, and "<PARAMS>" is the
                        optional driver-specific database parameter string.

        Raises:
            NotFound            - if the database does not exist;
            IncompatibleSchema  - if the database is not empty and its schema
                                  is incompatible with the current I/O schema.
        """
        assert isinstance(database, str)
        self.driver = _driver_create(database)

    def is_initialized(self):
        """
        Check if the database is initialized (not empty).

        Returns:
            True if the database is initialized, False otherwise.
        """
        return self.driver.is_initialized()

    def init(self):
        """
        Initialize the database. The database must be empty (not initialized).
        """
        assert not self.is_initialized()
        self.driver.init()

    def cleanup(self):
        """
        Cleanup (empty) the database, removing all data.
        The database must be initialized (not empty).
        """
        assert self.is_initialized()
        self.driver.cleanup()

    def get_last_modified(self):
        """
        Get the time the data in the database was last modified.
        The database must be initialized (not empty).

        Returns:
            The datetime object representing the last modification time.
        """
        assert self.is_initialized()
        last_modified = self.driver.get_last_modified()
        assert isinstance(last_modified, datetime.datetime)
        assert last_modified.tzinfo
        return last_modified

    def dump_iter(self, objects_per_report=0):
        """
        Dump all data from the database in object number-limited chunks.

        Args:
            objects_per_report: An integer number of objects per each returned
                                report data, or zero for no limit.

        Returns:
            An iterator returning report JSON data adhering to the current I/O
            schema version, each containing at most the specified number of
            objects.
        """
        assert self.is_initialized()
        assert isinstance(objects_per_report, int)
        assert objects_per_report >= 0
        yield from self.driver.dump_iter(
            objects_per_report=objects_per_report
        )

    def dump(self):
        """
        Dump all data from the database.

        Returns:
            The JSON data from the database adhering to the current I/O schema
            version.
        """
        assert self.is_initialized()
        try:
            return next(self.dump_iter(objects_per_report=0))
        except StopIteration:
            return io.SCHEMA.new()

    def query_iter(self, ids=None,
                   children=False, parents=False,
                   objects_per_report=0):
        """
        Match and fetch objects from the database, in object number-limited
        chunks.

        Args:
            ids:                A dictionary of object list names, and lists
                                of IDs of objects to match. None means empty
                                dictionary.
            children:           True if children of matched objects should be
                                matched as well.
            parents:            True if parents of matched objects should be
                                matched as well.
            objects_per_report: An integer number of objects per each returned
                                report data, or zero for no limit.

        Returns:
            An iterator returning report JSON data adhering to the current I/O
            schema version, each containing at most the specified number of
            objects.
        """
        assert LIGHT_ASSERTS or self.is_initialized()
        if ids is None:
            ids = {}
        assert isinstance(ids, dict)
        assert all(isinstance(k, str) and isinstance(v, list) and
                   all(isinstance(e, str) for e in v)
                   for k, v in ids.items())
        assert isinstance(objects_per_report, int)
        assert objects_per_report >= 0
        yield from self.driver.query_iter(
            ids=ids,
            children=children, parents=parents,
            objects_per_report=objects_per_report
        )

    def query(self, ids=None, children=False, parents=False):
        """
        Match and fetch objects from the database.

        Args:
            ids:        A dictionary of object list names, and lists of IDs of
                        objects to match. None means empty dictionary.
            children:   True if children of matched objects should be matched
                        as well.
            parents:    True if parents of matched objects should be matched
                        as well.

        Returns:
            The JSON data from the database adhering to the current I/O schema
            version.
        """
        assert LIGHT_ASSERTS or self.is_initialized()
        assert ids is None or (
            isinstance(ids, dict) and
            all(isinstance(k, str) and isinstance(v, list) and
                all(isinstance(e, str) for e in v)
                for k, v in ids.items())
        )
        try:
            return next(self.query_iter(ids=ids,
                                        children=children, parents=parents,
                                        objects_per_report=0))
        except StopIteration:
            return io.SCHEMA.new()

    def oo_query(self, pattern_set):
        """
        Query raw object-oriented data from the database.

        Args:
            pattern_set:    A set of patterns ("kcidb.orm.Pattern"
                            instances) matching objects to fetch.
        Returns:
            A dictionary of object type names and lists containing retrieved
            objects of the corresponding type.
        """
        assert LIGHT_ASSERTS or self.is_initialized()
        assert isinstance(pattern_set, set)
        assert all(isinstance(r, kcidb.orm.Pattern) for r in pattern_set)
        LOGGER.debug("OO Query: %r", pattern_set)
        return self.driver.oo_query(pattern_set)

    def load(self, data):
        """
        Load data into the database.

        Args:
            data:   The JSON data to load into the database.
                    Must adhere to a version of I/O schema.
        """
        assert LIGHT_ASSERTS or self.is_initialized()
        assert LIGHT_ASSERTS or io.SCHEMA.is_valid(data)
        self.driver.load(io.SCHEMA.upgrade(data))


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
        for name, driver in DRIVER_TYPES.items():
            print(f"\n{name!r} driver\n" +
                  "-" * (len(name) + 9) + "\n" +
                  driver.DOC)
        parser.exit()


def argparse_add_args(parser, database=None):
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


class ArgumentParser(kcidb.misc.ArgumentParser):
    """
    Command-line argument parser with common database arguments added.
    """

    def __init__(self, *args, database=None, **kwargs):
        """
        Initialize the parser, adding common database arguments.

        Args:
            args:       Positional arguments to initialize ArgumentParser
                        with.
            database:   The default database specification to use, or None to
                        make database specification required.
            kwargs:     Keyword arguments to initialize ArgumentParser with.
        """
        super().__init__(*args, **kwargs)
        argparse_add_args(self, database=database)


class OutputArgumentParser(kcidb.misc.OutputArgumentParser):
    """
    Command-line argument parser for tools outputting JSON,
    with common database arguments added.
    """

    def __init__(self, *args, database=None, **kwargs):
        """
        Initialize the parser, adding JSON output arguments.

        Args:
            args:       Positional arguments to initialize ArgumentParser
                        with.
            database:   The default database specification to use, or None to
                        make database specification required.
            kwargs:     Keyword arguments to initialize ArgumentParser with.
        """
        super().__init__(*args, **kwargs)
        argparse_add_args(self, database=database)


class SplitOutputArgumentParser(kcidb.misc.SplitOutputArgumentParser):
    """
    Command-line argument parser for tools outputting split-report streams,
    with common database arguments added.
    """

    def __init__(self, *args, database=None, **kwargs):
        """
        Initialize the parser, adding split-report output arguments.

        Args:
            args:       Positional arguments to initialize ArgumentParser
                        with.
            database:   The default database specification to use, or None to
                        make database specification required.
            kwargs:     Keyword arguments to initialize ArgumentParser with.
        """
        super().__init__(*args, **kwargs)
        argparse_add_args(self, database=database)


# No, it's OK, pylint: disable=too-many-ancestors
class QueryArgumentParser(SplitOutputArgumentParser):
    """
    Command-line argument parser with common database query arguments added.
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize the parser, adding common database query arguments.

        Args:
            args:   Positional arguments to initialize the parent
                    SplitOutputArgumentParser with.
            kwargs: Keyword arguments to initialize the parent
                    SplitOutputArgumentParser with.
        """
        super().__init__(*args, **kwargs)

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
            '--parents',
            help='Match parents of matching objects',
            action='store_true'
        )
        self.add_argument(
            '--children',
            help='Match children of matching objects',
            action='store_true'
        )


def dump_main():
    """Execute the kcidb-db-dump command-line tool"""
    sys.excepthook = kcidb.misc.log_and_print_excepthook
    description = \
        'kcidb-db-dump - Dump all data from Kernel CI report database'
    parser = SplitOutputArgumentParser(description=description)
    args = parser.parse_args()
    client = Client(args.database)
    if not client.is_initialized():
        raise Exception(f"Database {args.database!r} is not initialized")
    kcidb.misc.json_dump_stream(
        client.dump_iter(args.objects_per_report),
        sys.stdout, indent=args.indent, seq=args.seq
    )


def query_main():
    """Execute the kcidb-db-query command-line tool"""
    sys.excepthook = kcidb.misc.log_and_print_excepthook
    description = \
        "kcidb-db-query - Query objects from Kernel CI report database"
    parser = QueryArgumentParser(description=description)
    args = parser.parse_args()
    client = Client(args.database)
    if not client.is_initialized():
        raise Exception(f"Database {args.database!r} is not initialized")
    query_iter = client.query_iter(
        ids=dict(checkouts=args.checkout_ids,
                 builds=args.build_ids,
                 tests=args.test_ids),
        parents=args.parents,
        children=args.children,
        objects_per_report=args.objects_per_report
    )
    kcidb.misc.json_dump_stream(
        query_iter, sys.stdout, indent=args.indent, seq=args.seq
    )


def load_main():
    """Execute the kcidb-db-load command-line tool"""
    sys.excepthook = kcidb.misc.log_and_print_excepthook
    description = \
        'kcidb-db-load - Load reports into Kernel CI report database'
    parser = ArgumentParser(description=description)
    args = parser.parse_args()
    client = Client(args.database)
    if not client.is_initialized():
        raise Exception(f"Database {args.database!r} is not initialized")
    for data in kcidb.misc.json_load_stream_fd(sys.stdin.fileno()):
        data = io.SCHEMA.upgrade(io.SCHEMA.validate(data), copy=False)
        client.load(data)


def init_main():
    """Execute the kcidb-db-init command-line tool"""
    sys.excepthook = kcidb.misc.log_and_print_excepthook
    description = 'kcidb-db-init - Initialize a Kernel CI report database'
    parser = ArgumentParser(description=description)
    parser.add_argument(
        '--ignore-initialized',
        help='Do not fail if the database is already initialized.',
        action='store_true'
    )
    args = parser.parse_args()
    client = Client(args.database)
    if not client.is_initialized():
        client.init()
    elif not args.ignore_initialized:
        raise Exception(f"Database {args.database!r} is already initialized")


def cleanup_main():
    """Execute the kcidb-db-cleanup command-line tool"""
    sys.excepthook = kcidb.misc.log_and_print_excepthook
    description = 'kcidb-db-cleanup - Cleanup a Kernel CI report database'
    parser = ArgumentParser(description=description)
    parser.add_argument(
        '--ignore-not-initialized',
        help='Do not fail if the database is not initialized.',
        action='store_true'
    )
    parser.add_argument(
        '--ignore-not-found',
        help='Do not fail if the database does not exist.',
        action='store_true'
    )
    args = parser.parse_args()
    try:
        client = Client(args.database)
    except misc.NotFound:
        if args.ignore_not_found:
            return
        raise
    if client.is_initialized():
        client.cleanup()
    elif not args.ignore_not_initialized:
        raise Exception(f"Database {args.database!r} is not initialized")
