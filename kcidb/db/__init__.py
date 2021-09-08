"""Kernel CI report database"""

import sys
import logging
import kcidb_io as io
import kcidb.orm
import kcidb.misc
from kcidb.misc import LIGHT_ASSERTS
from kcidb.db import bigquery, sqlite, json, null

# Module's logger
LOGGER = logging.getLogger(__name__)


class IncompatibleSchema(Exception):
    """Database schema is incompatible with the latest I/O schema"""

    def __init__(self, db_major, db_minor):
        """
        Initialize the exception.

        Args:
            db_major:   Database schema major version number
            db_minor:   Database schema minor version number
        """
        super().__init__(f"Database schema {db_major}.{db_minor} "
                         f"is incompatible with I/O schema "
                         f"{io.schema.LATEST.major}."
                         f"{io.schema.LATEST.minor}")


# A dictionary of known driver names and types
DRIVER_TYPES = dict(
    bigquery=bigquery.Driver,
    sqlite=sqlite.Driver,
    json=json.Driver,
    null=null.Driver,
)


class Client(kcidb.orm.Source):
    """Kernel CI report database client"""

    def __init__(self, database):
        """
        Initialize a Kernel CI report database client.

        Args:
            driver_params:  A string specifying the database to access,
                            formatted as "<DRIVER>:<PARAMS>" or just
                            "<DRIVER>". Where "<DRIVER>" is the driver name,
                            and "<PARAMS>" is the optional driver-specific
                            database parameter string.

        Raises:
            `IncompatibleSchema` if the database is not empty and its schema
            is incompatible with the latest I/O schema.
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
            self.driver = driver_type(driver_params)
        except Exception as exc:
            raise Exception(
                f"Failed connecting to {driver_name!r} database"
            ) from exc
        if self.driver.is_initialized():
            major, minor = self.driver.get_schema_version()
            if major != io.schema.LATEST.major:
                raise IncompatibleSchema(major, minor)

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
        return self.driver.get_last_modified()

    def dump_iter(self, objects_per_report=0):
        """
        Dump all data from the database in object number-limited chunks.

        Args:
            objects_per_report: An integer number of objects per each returned
                                report data, or zero for no limit.

        Returns:
            An iterator returning report JSON data adhering to the latest I/O
            schema version, each containing at most the specified number of
            objects.
        """
        assert self.is_initialized()
        assert isinstance(objects_per_report, int)
        assert objects_per_report >= 0
        return self.driver.dump_iter(objects_per_report=objects_per_report)

    def dump(self):
        """
        Dump all data from the database.

        Returns:
            The JSON data from the database adhering to the latest I/O schema
            version.
        """
        assert self.is_initialized()
        try:
            return next(self.dump_iter(objects_per_report=0))
        except StopIteration:
            return io.new()

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
            An iterator returning report JSON data adhering to the latest I/O
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
        return self.driver.query_iter(ids=ids,
                                      children=children, parents=parents,
                                      objects_per_report=objects_per_report)

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
            The JSON data from the database adhering to the latest I/O schema
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
            return io.new()

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
        assert LIGHT_ASSERTS or io.schema.is_valid(data)
        self.driver.load(io.schema.upgrade(data))

    def complement(self, data):
        """
        Given I/O data, return its complement. I.e. the same data, but with
        all objects from the database it references. E.g. for each checkout
        load all its builds, for each build load all its tests. And vice
        versa: for each test load its build, and for each build load its
        checkout.

        Args:
            data:   The JSON data to complement from the database.
                    Must adhere to a version of I/O schema
                    Will not be modified.

        Returns:
            The complemented JSON data from the database adhering to the
            latest version of I/O schema.
        """
        assert LIGHT_ASSERTS or self.is_initialized()
        assert LIGHT_ASSERTS or io.schema.is_valid(data)
        data = io.schema.upgrade(data)

        # Collect IDs of all supplied objects
        ids = {
            obj_list_name: [obj["id"] for obj in data.get(obj_list_name, [])]
            for obj_list_name in io.schema.LATEST.tree if obj_list_name
        }

        # Query the objects along with parents and children
        return self.query(ids=ids, children=True, parents=True)


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
        help=('Database specification' if database is None
              else f"Database specification. Default is {database!r}."),
        default=database,
        required=(database is None)
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


def complement_main():
    """Execute the kcidb-db-complement command-line tool"""
    sys.excepthook = kcidb.misc.log_and_print_excepthook
    description = \
        'kcidb-db-complement - Complement reports from database'
    parser = OutputArgumentParser(description=description)
    args = parser.parse_args()
    client = Client(args.database)
    if not client.is_initialized():
        raise Exception(f"Database {args.database!r} is not initialized")
    kcidb.misc.json_dump_stream(
        (
            client.complement(
                io.schema.upgrade(io.schema.validate(data), copy=False)
            )
            for data in kcidb.misc.json_load_stream_fd(sys.stdin.fileno())
        ),
        sys.stdout, indent=args.indent, seq=args.seq
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
        data = io.schema.upgrade(io.schema.validate(data), copy=False)
        client.load(data)


def init_main():
    """Execute the kcidb-db-init command-line tool"""
    sys.excepthook = kcidb.misc.log_and_print_excepthook
    description = 'kcidb-db-init - Initialize a Kernel CI report database'
    parser = ArgumentParser(description=description)
    args = parser.parse_args()
    client = Client(args.database)
    if client.is_initialized():
        raise Exception(f"Database {args.database!r} is already initialized")
    client.init()


def cleanup_main():
    """Execute the kcidb-db-cleanup command-line tool"""
    sys.excepthook = kcidb.misc.log_and_print_excepthook
    description = 'kcidb-db-cleanup - Cleanup a Kernel CI report database'
    parser = ArgumentParser(description=description)
    args = parser.parse_args()
    client = Client(args.database)
    if not client.is_initialized():
        raise Exception(f"Database {args.database!r} is not initialized")
    client.cleanup()
