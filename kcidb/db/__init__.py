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

# It's OK for now, pylint: disable=too-many-lines

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
        self.database = database
        self.driver = None
        self.reset()

    def is_initialized(self):
        """
        Check if the database is initialized.

        Returns:
            True if the database is initialized, False otherwise.
        """
        return self.driver.is_initialized()

    def reset(self):
        """
        Reset the client's database connection, updating
        its knowledge of the database schema
        """
        self.driver = misc.instantiate_spec(DRIVER_TYPES, self.database)
        assert all(io_schema <= io.SCHEMA
                   for io_schema in self.driver.get_schemas().values()), \
            "Driver has I/O schemas newer than the current package I/O schema"

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

    def purge(self, before=None):
        """
        Remove all the data from the database that arrived before the
        specified time, if the database supports that.
        The database must be initialized.

        Args:
            before: An "aware" datetime.datetime object specifying the
                    earliest (database server) time the data to be *preserved*
                    should've arrived. Any other data will be purged.
                    Can be None to have nothing removed. The latter can be
                    used to test if the database supports purging.

        Returns:
            True if the database supports purging, and the requested data was
            purged. False if the database doesn't support purging.
        """
        assert self.is_initialized()
        assert before is None or \
            isinstance(before, datetime.datetime) and before.tzinfo
        return self.driver.purge(before)

    def get_current_time(self):
        """
        Get the current time from the database server.

        Returns:
            A timezone-aware datetime object representing the current
            time on the database server.
        """
        current_time = self.driver.get_current_time()
        assert isinstance(current_time, datetime.datetime)
        assert current_time.tzinfo
        return current_time

    def get_last_modified(self):
        """
        Get the time data has arrived last into the driven database. Can
        return the minimum timestamp constant, if the database is empty.
        The database must be initialized.

        Returns:
            A timezone-aware datetime object representing the last
            data arrival time.

        Raises:
            NoTimestamps    - The database doesn't have row timestamps, and
                              cannot determine the last data arrival time.
        """
        assert self.is_initialized()
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

    def dump_iter(self, objects_per_report=0, with_metadata=True,
                  after=None, until=None):
        """
        Dump all data from the database in object number-limited chunks.

        Args:
            objects_per_report: An integer number of objects per each returned
                                report data, or zero for no limit.
            with_metadata:      True, if metadata fields should be dumped as
                                well. False, if not.
            after:              An "aware" datetime.datetime object specifying
                                the latest (database server) time the data to
                                be excluded from the dump should've arrived.
                                The data after this time will be dumped.
                                Can be None to have no limit on older data.
            until:              An "aware" datetime.datetime object specifying
                                the latest (database server) time the data to
                                be dumped should've arrived.
                                The data after this time will not be dumped.
                                Can be None to have no limit on newer data.

        Returns:
            An iterator returning report JSON data adhering to the current I/O
            schema version, each containing at most the specified number of
            objects.

        Raises:
            NoTimestamps    - Either "after" or "until" are not None, and
                              the database doesn't have row timestamps.
        """
        assert isinstance(objects_per_report, int)
        assert objects_per_report >= 0
        assert isinstance(with_metadata, bool)
        assert after is None or \
            isinstance(after, datetime.datetime) and after.tzinfo
        assert until is None or \
            isinstance(until, datetime.datetime) and until.tzinfo
        assert self.is_initialized()
        yield from self.driver.dump_iter(
            objects_per_report=objects_per_report,
            with_metadata=with_metadata,
            after=after, until=until
        )

    def dump(self, with_metadata=True, after=None, until=None):
        """
        Dump all data from the database.

        Args:
            with_metadata:      True, if metadata fields should be dumped as
                                well. False, if not.
            after:              An "aware" datetime.datetime object specifying
                                the latest (database server) time the data to
                                be excluded from the dump should've arrived.
                                The data after this time will be dumped.
                                Can be None to have no limit on older data.
            until:              An "aware" datetime.datetime object specifying
                                the latest (database server) time the data to
                                be dumped should've arrived.
                                The data after this time will not be dumped.
                                Can be None to have no limit on newer data.

        Returns:
            The JSON data from the database adhering to the current I/O schema
            version.

        Raises:
            NoTimestamps    - Either "after" or "until" are not None, and
                              the database doesn't have row timestamps.
        """
        assert isinstance(with_metadata, bool)
        assert after is None or \
            isinstance(after, datetime.datetime) and after.tzinfo
        assert until is None or \
            isinstance(until, datetime.datetime) and until.tzinfo
        assert self.is_initialized()
        try:
            return next(self.dump_iter(objects_per_report=0,
                                       with_metadata=with_metadata,
                                       after=after, until=until))
        except StopIteration:
            return self.get_schema()[1].new()

    # No, it's not, pylint: disable=too-many-return-statements
    def query_ids_are_valid(self, ids):
        """
        Verify the IDs to be queried are valid according to the I/O version
        supported by the database. The database must be initialized.

        Args:
            ids:    A dictionary of object list names, and lists of IDs of
                    objects to match. None means empty dictionary. Each ID is
                    either a tuple of values or a single value (equivalent to
                    a single-value tuple). The values should match the types,
                    the order, and the number of the object's ID fields as
                    described by the database's I/O schema (the "id_fields"
                    attribute).

        Returns:
            True if the IDs are valid, false otherwise.
        """
        assert LIGHT_ASSERTS or self.is_initialized()
        id_fields = self.get_schema()[1].id_fields

        if ids is None:
            return True
        if not isinstance(ids, dict):
            return False
        for obj_list_name, values_list in ids.items():
            if obj_list_name not in id_fields:
                return False
            obj_id_fields = id_fields[obj_list_name]
            if not isinstance(values_list, list):
                return False
            for values in values_list:
                if not isinstance(values, tuple):
                    values = (values,)
                if len(values) != len(obj_id_fields):
                    return False
                for value, type in zip(values, obj_id_fields.values()):
                    if not isinstance(value, type):
                        return False
        return True

    def query_ids_normalize(self, ids):
        """
        Normalize the IDs to be queried to always be a dictionary of object
        list names and lists of IDs, where each ID is a tuple with ID field
        values for the corresponding object type.

        Args:
            ids:    A dictionary of object list names, and lists of IDs of
                    objects to match. None means empty dictionary. Each ID is
                    either a tuple of values or a single value (equivalent to
                    a single-value tuple). The values should match the types,
                    the order, and the number of the object's ID fields as
                    described by the database's I/O schema (the "id_fields"
                    attribute).

        Returns:
            The normalized IDs: a dictionary of object list names, and lists
            of IDs of objects to match. Each ID is a tuple of values. The
            values should match the types, the order, and the number of the
            object's ID fields as described by the database's I/O schema (the
            "id_fields" attribute).
        """
        assert self.query_ids_are_valid(ids)
        new_ids = {
            obj_list_name: [
                values if isinstance(values, tuple) else (values,)
                for values in values_list
            ]
            for obj_list_name, values_list in (ids or {}).items()
        }
        assert self.query_ids_are_valid(new_ids)
        return new_ids

    # We can live with this for now, pylint: disable=too-many-arguments
    # Or if you prefer, pylint: disable=too-many-positional-arguments
    def query_iter(self, ids=None,
                   children=False, parents=False,
                   objects_per_report=0, with_metadata=False):
        """
        Match and fetch objects from the database, in object number-limited
        chunks.

        Args:
            ids:                A dictionary of object list names, and lists
                                of IDs of objects to match. None means empty
                                dictionary. Each ID is either a tuple of
                                values or a single value (equivalent to a
                                single-value tuple). The values should match
                                the types, the order, and the number of the
                                object's ID fields as described by the
                                database's I/O schema (the "id_fields"
                                attribute).
            children:           True if children of matched objects should be
                                matched as well.
            parents:            True if parents of matched objects should be
                                matched as well.
            objects_per_report: An integer number of objects per each returned
                                report data, or zero for no limit.
            with_metadata:      True, if metadata fields should be fetched as
                                well. False, if not.

        Returns:
            An iterator returning report JSON data adhering to the current I/O
            schema version, each containing at most the specified number of
            objects.
        """
        assert LIGHT_ASSERTS or self.is_initialized()
        assert self.query_ids_are_valid(ids)
        assert isinstance(objects_per_report, int)
        assert objects_per_report >= 0
        assert isinstance(with_metadata, bool)
        yield from self.driver.query_iter(
            ids=self.query_ids_normalize(ids),
            children=children, parents=parents,
            objects_per_report=objects_per_report,
            with_metadata=with_metadata
        )

    def query(self, ids=None, children=False, parents=False,
              with_metadata=False):
        """
        Match and fetch objects from the database.

        Args:
            ids:            A dictionary of object list names, and lists of
                            IDs of objects to match. None means empty
                            dictionary. Each ID is either a tuple of values or
                            a single value (equivalent to a single-value
                            tuple). The values should match the types, the
                            order, and the number of the object's ID fields as
                            described by the database's I/O schema (the
                            "id_fields" attribute).
            children:       True if children of matched objects should be
                            matched as well.
            parents:        True if parents of matched objects should be
                            matched as well.
            with_metadata:  True, if metadata fields should be fetched as
                            well. False, if not.

        Returns:
            The JSON data from the database adhering to the current I/O schema
            version.
        """
        assert LIGHT_ASSERTS or self.is_initialized()
        assert self.query_ids_are_valid(ids)
        assert isinstance(with_metadata, bool)
        try:
            return next(self.query_iter(ids=ids,
                                        children=children, parents=parents,
                                        objects_per_report=0,
                                        with_metadata=with_metadata))
        except StopIteration:
            return self.get_schema()[1].new()

    def oo_query(self, pattern_set):
        """
        Query raw object-oriented data from the database.

        Args:
            pattern_set:    A set of patterns ("kcidb.orm.query.Pattern"
                            instances) matching objects to fetch.
        Returns:
            A dictionary of object type names and lists containing retrieved
            objects of the corresponding type.
        """
        assert LIGHT_ASSERTS or self.is_initialized()
        assert isinstance(pattern_set, set)
        assert all(isinstance(r, kcidb.orm.query.Pattern)
                   for r in pattern_set)
        LOGGER.debug("OO Query: %r", pattern_set)
        return self.driver.oo_query(pattern_set)

    def load(self, data, with_metadata=False):
        """
        Load data into the database.

        Args:
            data:           The JSON data to load into the database.
                            Must adhere to the database's supported I/O schema
                            version, or an earlier one.
            with_metadata:  True if any metadata in the data should
                            also be loaded into the database. False if it
                            should be discarded and the database should
                            generate its metadata itself.
        """
        assert LIGHT_ASSERTS or self.is_initialized()
        io_schema = self.get_schema()[1]
        assert io_schema.is_compatible_directly(data)
        assert LIGHT_ASSERTS or io_schema.is_valid_exactly(data)
        assert isinstance(with_metadata, bool)
        self.driver.load(data, with_metadata=with_metadata)


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
               else f" Default is {database!r}.")),
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


class InputArgumentParser(kcidb.misc.InputArgumentParser):
    """
    Command-line argument parser with common database arguments added, for
    tools inputting JSON.
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
            '-i', '--issue-id',
            metavar="ID",
            default=[],
            help='ID of an issue to match',
            dest="issue_ids",
            action='append',
        )
        self.add_argument(
            '--issue-version', '--iv',
            metavar="VERSION",
            type=int,
            default=[],
            help='Version of an issue to match',
            dest="issue_versions",
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
        self.add_argument(
            '--with-metadata',
            help='Fetch metadata fields as well',
            action='store_true'
        )

    def parse_args(self, args=None, namespace=None):
        """
        Parse (and validate) command-line arguments.

        Args:
            args:       List of argument strings to parse.
                        The sys.argv is used, if None.
            namespace:  The object to take the parsed attributes. A new empty
                        argparse.Namespace object is used, if None.
        Returns:
            The populated namespace.
        """
        namespace = super().parse_args(args, namespace)
        if len(namespace.issue_ids) != len(namespace.issue_versions):
            self.error("Mismatching number of issue IDs vs. issue versions "
                       "(--issue-id vs. --issue-version options)")
        return namespace


def dump_main():
    """Execute the kcidb-db-dump command-line tool"""
    sys.excepthook = kcidb.misc.log_and_print_excepthook
    description = \
        'kcidb-db-dump - Dump all data from Kernel CI report database'
    parser = SplitOutputArgumentParser(description=description)
    parser.add_argument(
        '--without-metadata',
        help='Do not dump metadata fields',
        action='store_true'
    )
    parser.add_argument(
        '--after',
        metavar='AFTER',
        type=kcidb.misc.iso_timestamp,
        help="An ISO-8601 timestamp specifying the latest time the data to "
        "be *excluded* from the dump should've arrived."
    )
    parser.add_argument(
        '--until',
        metavar='UNTIL',
        type=kcidb.misc.iso_timestamp,
        help="An ISO-8601 timestamp specifying the latest time the data to "
        "be *included* into the dump should've arrived."
    )
    args = parser.parse_args()
    client = Client(args.database)
    if not client.is_initialized():
        raise Exception(f"Database {args.database!r} is not initialized")
    kcidb.misc.json_dump_stream(
        client.dump_iter(objects_per_report=args.objects_per_report,
                         with_metadata=not args.without_metadata,
                         after=args.after, until=args.until),
        sys.stdout, indent=args.indent, seq=args.seq_out
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
                 tests=args.test_ids,
                 issues=list(zip(args.issue_ids, args.issue_versions)),
                 incidents=args.incident_ids),
        parents=args.parents,
        children=args.children,
        objects_per_report=args.objects_per_report,
        with_metadata=args.with_metadata
    )
    kcidb.misc.json_dump_stream(
        query_iter, sys.stdout, indent=args.indent, seq=args.seq_out
    )


def load_main():
    """Execute the kcidb-db-load command-line tool"""
    sys.excepthook = kcidb.misc.log_and_print_excepthook
    description = \
        'kcidb-db-load - Load reports into Kernel CI report database'
    parser = InputArgumentParser(description=description)
    parser.add_argument(
        '--with-metadata',
        help='Load metadata fields as well',
        action='store_true'
    )
    args = parser.parse_args()
    client = Client(args.database)
    if not client.is_initialized():
        raise Exception(f"Database {args.database!r} is not initialized")
    io_schema = client.get_schema()[1]
    for data in kcidb.misc.json_load_stream_fd(sys.stdin.fileno(),
                                               seq=args.seq_in):
        data = io_schema.upgrade(io_schema.validate(data), copy=False)
        client.load(data, with_metadata=args.with_metadata)


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


def purge_main():
    """Execute the kcidb-db-purge command-line tool"""
    sys.excepthook = kcidb.misc.log_and_print_excepthook
    description = 'kcidb-db-purge - Try removing all data from a ' \
        'Kernel CI report database that arrived before a certain time. ' \
        'Exit with status 2, if not supported by database.'
    parser = ArgumentParser(description=description)
    parser.add_argument(
        'before',
        metavar='BEFORE',
        type=kcidb.misc.iso_timestamp,
        nargs='?',
        help="An ISO-8601 timestamp specifying the earliest time the data to "
        "be *preserved* should've arrived. "
        "No data is removed if not specified."
    )
    args = parser.parse_args()
    client = Client(args.database)
    if not client.is_initialized():
        raise Exception(f"Database {args.database!r} is not initialized")
    return 0 if client.purge(before=args.before) else 2
