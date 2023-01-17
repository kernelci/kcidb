"""Kernel CI report database"""

import sys
import logging
import argparse
import datetime
import kcidb.io as io
import kcidb.orm
import kcidb.misc
from kcidb.misc import LIGHT_ASSERTS
from kcidb.db import abstract, schematic, mux, \
    bigquery, postgresql, sqlite, json, null, misc  # noqa: F401

# Module's logger
LOGGER = logging.getLogger(__name__)


class MuxDriver(mux.Driver):
    """Kernel CI multiplexing database driver"""

    @classmethod
    def get_doc(cls):
        """
        Get driver documentation.

        Returns:
            The driver documentation string.
        """
        return super().get_doc() + \
            "\n            Example: postgresql bigquery:kcidb_01"

    @classmethod
    def get_drivers(cls):
        """
        Retrieve a dictionary of driver names and types available for driver's
        control.

        Returns:
            A driver dictionary.
        """
        return DRIVER_TYPES


# A dictionary of known driver names and types
DRIVER_TYPES = dict(
    bigquery=bigquery.Driver,
    postgresql=postgresql.Driver,
    sqlite=sqlite.Driver,
    json=json.Driver,
    null=null.Driver,
    mux=MuxDriver,
)


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
            UnknownDriver       - an unknown (sub-)driver encountered in the
                                  specification string for a (component)
                                  database
            NotFound            - a database does not exist
            UnsupportedSchema   - a database schema is not supported by a
                                  driver
        """
        assert isinstance(database, str)
        self.driver = misc.instantiate_spec(DRIVER_TYPES, database)
        assert all(io_schema <= io.SCHEMA
                   for io_schema in self.driver.get_schemas().values()), \
            "Driver has I/O schemas newer than the current package I/O schema"

    def is_initialized(self):
        """
        Check if the database is initialized.

        Returns:
            True if the database is initialized, False otherwise.
        """
        return self.driver.is_initialized()

    def init(self, version=None):
        """
        Initialize the driven database. The database must be uninitialized.

        Args:
            version:    A tuple of the major and minor version numbers (both
                        non-negative integers) of the schema to initialize the
                        database to (must be one of the database's available
                        schema versions), an I/O version object to pick the
                        earliest schema supporting it, or None to initialize
                        to the latest schema.
        """
        assert not self.is_initialized()
        schemas = self.get_schemas()
        if version is None:
            version = list(schemas)[-1]
        elif isinstance(version, type) and \
                issubclass(version, io.schema.VA):
            for schema_version, schema_io_version in schemas.items():
                if schema_io_version is version:
                    version = schema_version
                    break
            else:
                assert False, "I/O version not supported by driver schemas"
        assert isinstance(version, tuple) and len(version) == 2
        assert isinstance(version[0], int) and version[0] >= 0
        assert isinstance(version[1], int) and version[1] >= 0
        assert version in schemas, "Schema version is not available"
        self.driver.init(version)

    def cleanup(self):
        """
        Cleanup (deinitialize) the database, removing all data.
        The database must be initialized.
        """
        assert self.is_initialized()
        self.driver.cleanup()

    def empty(self):
        """
        Empty the database, removing all data.
        The database must be initialized.
        """
        assert self.is_initialized()
        self.driver.empty()

    def get_last_modified(self):
        """
        Get the time the data in the connected database was last modified.
        Can return the minimum timestamp constant, if the database is not
        initialized or its data loading interface is not limited in the amount
        of load() method calls.

        Returns:
            A timezone-aware datetime object representing the last
            modification time.
        """
        last_modified = self.driver.get_last_modified()
        assert isinstance(last_modified, datetime.datetime)
        assert last_modified.tzinfo
        return last_modified

    def get_schemas(self):
        """
        Retrieve available database schemas: a dictionary of tuples containing
        major and minor version numbers of the schemas (both non-negative
        integers), and corresponding I/O schemas
        (kcidb_io.schema.abstract.Version instances) supported by them.

        Returns:
            The schema dictionary, sorted by ascending version numbers.
        """
        schemas = self.driver.get_schemas()
        assert isinstance(schemas, dict)
        assert len(schemas) > 0
        prev_version = None
        prev_io_version = None
        for version, io_version in schemas.items():
            assert isinstance(version, tuple) and len(version) == 2
            assert isinstance(version[0], int) and version[0] >= 0
            assert isinstance(version[1], int) and version[1] >= 0
            assert prev_version is None or version > prev_version
            assert isinstance(io_version, type) and \
                issubclass(io_version, io.schema.VA)
            assert prev_io_version is None or io_version > prev_io_version
        return schemas

    def get_schema(self):
        """
        Get a tuple with the driven database schema's major and minor version
        numbers, and the I/O schema supported by it. The database must be
        initialized.

        Returns:
            A tuple of the major and minor version numbers (both non-negative
            integers) of the database schema, and the I/O schema (a
            kcidb_io.schema.abstract.Version) supported by it (allowed for
            loading).
        """
        assert self.is_initialized()
        version, io_version = self.driver.get_schema()
        assert isinstance(version, tuple) and len(version) == 2
        assert isinstance(version[0], int) and version[0] >= 0
        assert isinstance(version[1], int) and version[1] >= 0
        assert isinstance(io_version, type) and \
            issubclass(io_version, io.schema.VA)
        assert (version, io_version) in self.get_schemas().items()
        return version, io_version

    def upgrade(self, target_version=None):
        """
        Upgrade the database to the latest (or specified) schema.
        The database must be initialized.

        Args:
            target_version: A tuple of the major and minor version numbers of
                            the schema to upgrade to (must be one of the
                            database's available schema versions, newer than
                            the current one), an I/O version object to pick
                            the earliest schema supporting it, or None to
                            upgrade to the latest schema.
        """
        assert self.is_initialized()
        schemas = self.get_schemas()
        if target_version is None:
            target_version = list(schemas)[-1]
        elif isinstance(target_version, type) and \
                issubclass(target_version, io.schema.VA):
            for schema_version, schema_io_version in schemas.items():
                if schema_io_version is target_version:
                    target_version = schema_version
                    break
            else:
                assert False, \
                    "Target I/O version not supported by driver schemas"
        assert target_version in schemas, \
            "Target schema version is not available"
        current_version = self.get_schema()[0]
        assert target_version >= current_version, \
            "Target schema is older than the current schema"
        self.driver.upgrade(target_version)

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
            return self.get_schema()[1].new()

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
            return self.get_schema()[1].new()

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
                    Must adhere to the database's supported I/O schema
                    version, or an earlier one.
        """
        assert LIGHT_ASSERTS or self.is_initialized()
        io_schema = self.get_schema()[1]
        assert io_schema.is_compatible_directly(data)
        assert LIGHT_ASSERTS or io_schema.is_valid_exactly(data)
        self.driver.load(data)


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
                  driver.get_doc())
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
    io_schema = client.get_schema()[1]
    for data in kcidb.misc.json_load_stream_fd(sys.stdin.fileno()):
        data = io_schema.upgrade(io_schema.validate(data), copy=False)
        client.load(data)


def schemas_main():
    """Execute the kcidb-db-schemas command-line tool"""
    sys.excepthook = kcidb.misc.log_and_print_excepthook
    description = 'kcidb-db-schemas - List available database schemas ' \
        '(as <DB>: <I/O>)'
    parser = ArgumentParser(description=description)
    args = parser.parse_args()
    client = Client(args.database)
    curr_version = client.get_schema()[0] if client.is_initialized() else None
    lines = []
    widths = [0, 0]
    for version, io_version in client.get_schemas().items():
        marker = "*" if version == curr_version else ""
        lines.append((f"{marker}{version[0]}.{version[1]}",
                      f"{io_version.major}.{io_version.minor}"))
        # pylint: disable=consider-using-enumerate
        for i in range(len(widths)):
            widths[i] = max(widths[i], len(lines[-1][i]))
    for line in lines:
        # Considered, pylint: disable=consider-using-f-string
        print("%*s: %s" % (widths[0], line[0], line[1]))


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
    parser.add_argument(
        '-s', '--schema',
        metavar="VERSION",
        help="Specify database schema VERSION to initialize to "
             "(a first column value from kcidb-db-schemas output). "
             "Default is the latest version.",
        type=kcidb.misc.version
    )
    args = parser.parse_args()
    client = Client(args.database)
    if args.schema is not None:
        if args.schema not in client.get_schemas():
            raise Exception(
                f"Schema version {args.schema[0]}.{args.schema[1]} "
                f"is not available for database {args.database!r}"
            )
    if not client.is_initialized():
        client.init(args.schema)
    elif not args.ignore_initialized:
        raise Exception(f"Database {args.database!r} is already initialized")


def upgrade_main():
    """Execute the kcidb-db-upgrade command-line tool"""
    sys.excepthook = kcidb.misc.log_and_print_excepthook
    description = 'kcidb-db-upgrade - Upgrade database schema'
    parser = ArgumentParser(description=description)
    parser.add_argument(
        '-s', '--schema',
        metavar="VERSION",
        help="Specify database schema VERSION (<major>.<minor> - a left-side "
             "value from kcidb-db-schemas output) to upgrade to. "
             "Default is the latest version. "
             "Increases in the major number introduce "
             "backwards-incompatible changes, in the minor - "
             "backwards-compatible.",
        type=kcidb.misc.version
    )
    args = parser.parse_args()
    client = Client(args.database)
    if not client.is_initialized():
        raise Exception(f"Database {args.database!r} is not initialized")
    if args.schema is not None:
        if args.schema not in client.get_schemas():
            raise Exception(
                f"Schema version {args.schema[0]}.{args.schema[1]} "
                f"is not available for database {args.database!r}"
            )
        curr_schema = client.get_schema()[0]
        if args.schema < curr_schema:
            raise Exception(
                f"Schema version {args.schema[0]}.{args.schema[1]} "
                f"is older than version {curr_schema[0]}.{curr_schema[1]} "
                f"currently used by database {args.database!r}"
            )
    client.upgrade(args.schema)


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


def empty_main():
    """Execute the kcidb-db-empty command-line tool"""
    sys.excepthook = kcidb.misc.log_and_print_excepthook
    description = 'kcidb-db-empty - Remove all data from a ' \
        'Kernel CI report database'
    parser = ArgumentParser(description=description)
    args = parser.parse_args()
    client = Client(args.database)
    if client.is_initialized():
        client.empty()
    else:
        raise Exception(f"Database {args.database!r} is not initialized")
