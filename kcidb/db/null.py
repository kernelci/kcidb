"""Kernel CI report database - null driver"""

import textwrap
import datetime
import kcidb.io as io
from kcidb.misc import LIGHT_ASSERTS
from kcidb.db.abstract import Driver as AbstractDriver


class Driver(AbstractDriver):
    """Kernel CI null database driver"""

    @classmethod
    def get_doc(cls):
        """
        Get driver documentation.

        Returns:
            The driver documentation string.
        """
        return textwrap.dedent("""\
            The null driver discards any loaded data and returns nothing for
            any query. This driver does not take parameters.
        """)

    # Yes, it's an abstract class, pylint: disable=super-init-not-called
    def __init__(self, params):
        """
        Initialize the driver.

        Args:
            params:        Must be None (not specified).
        """
        assert params is None or isinstance(params, str)
        if params is not None:
            raise Exception("Database parameters are not accepted")

    def is_initialized(self):
        """
        Check if the database is initialized.

        Returns:
            True if the database is initialized, False otherwise.
        """
        return True

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
        return (0, 0), io.SCHEMA

    def get_schemas(self):
        """
        Retrieve available database schemas: a dictionary of tuples containing
        major and minor version numbers of the schemas (both non-negative
        integers), and corresponding I/O schemas
        (kcidb_io.schema.abstract.Version instances) supported by them.

        Returns:
            The schema dictionary, sorted by ascending version numbers.
        """
        return dict((self.get_schema(),))

    def upgrade(self, target_version):
        """
        Upgrade the database to the specified schema.
        The database must be initialized.

        Args:
            target_version: A tuple of the major and minor version numbers of
                            the schema to upgrade to (must be one of the
                            database's available schema versions, newer than
                            the current one).
        """
        assert target_version == self.get_schema()[0]

    def init(self, version):
        """
        Initialize the driven database. The database must be uninitialized.

        Args:
            version:    A tuple of the major and minor version numbers (both
                        non-negative integers) of the schema to initialize the
                        database to (must be one of the driver's available
                        schema versions)
        """
        assert version == self.get_schema()[0]

    def cleanup(self):
        """
        Cleanup (deinitialize) the database, removing all data.
        The database must be initialized.
        """

    def empty(self):
        """
        Empty the driven database, removing all data.
        The database must be initialized.
        """

    def get_current_time(self):
        """
        Get the current time from the database server.

        Returns:
            A timezone-aware datetime object representing the current
            time on the database server.
        """
        return datetime.datetime.now(datetime.timezone.utc)

    def get_first_modified(self):
        """
        Get the time data has arrived first into the driven database.
        The database must be initialized.

        Returns:
            A dictionary of names of I/O object types (list names), which have
            objects in the database, and timezone-aware datetime objects
            representing the time the first one has arrived into the database.

        Raises:
            NoTimestamps    - The database doesn't have row timestamps, and
                              cannot determine data arrival time.
        """
        return {}

    def get_last_modified(self):
        """
        Get the time data has arrived last into the driven database.
        The database must be initialized.

        Returns:
            A dictionary of names of I/O object types (list names), which have
            objects in the database, and timezone-aware datetime objects
            representing the time the last one has arrived into the database.

        Raises:
            NoTimestamps    - The database doesn't have row timestamps, and
                              cannot determine data arrival time.
        """
        return {}

    def dump_iter(self, objects_per_report, with_metadata, after, until):
        """
        Dump all data from the database in object number-limited chunks.

        Args:
            objects_per_report: An integer number of objects per each returned
                                report data, or zero for no limit.
            with_metadata:      True, if metadata fields should be dumped as
                                well. False, if not.
            after:              A dictionary of names of I/O object types
                                (list names) and timezone-aware datetime
                                objects specifying the latest time the
                                corresponding objects should've arrived to be
                                *excluded* from the dump. Any objects which
                                arrived later will be *eligible* for dumping.
                                Object types missing from this dictionary will
                                not be limited.
            until:              A dictionary of names of I/O object types
                                (list names) and timezone-aware datetime
                                objects specifying the latest time the
                                corresponding objects should've arrived to be
                                *included* into the dump. Any objects which
                                arrived later will be *ineligible* for
                                dumping. Object types missing from this
                                dictionary will not be limited.

        Returns:
            An iterator returning report JSON data adhering to the I/O
            version of the database schema, each containing at most the
            specified number of objects.

        Raises:
            NoTimestamps    - Either "after" or "until" are not empty, and
                              the database doesn't have row timestamps.
        """
        del objects_per_report
        del with_metadata
        del after
        del until
        yield io.SCHEMA.new()

    # We can live with this for now, pylint: disable=too-many-arguments
    # Or if you prefer, pylint: disable=too-many-positional-arguments
    def query_iter(self, ids, children, parents, objects_per_report,
                   with_metadata):
        """
        Match and fetch objects from the database, in object number-limited
        chunks.

        Args:
            ids:                A dictionary of object list names, and lists
                                of IDs of objects to match. Each ID is a tuple
                                of values. The values should match the types,
                                the order, and the number of the object's ID
                                fields as described by the database's I/O
                                schema (the "id_fields" attribute).
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
        del ids, children, parents, objects_per_report, with_metadata
        yield io.SCHEMA.new()

    def oo_query(self, pattern_set):
        """
        Query raw object-oriented data from the database.

        Args:
            pattern_set:    A set of patterns ("kcidb.oo.data.Pattern"
                            instances) matching objects to fetch.
        Returns:
            A dictionary of object type names and lists containing retrieved
            objects of the corresponding type.
        """
        del pattern_set
        return {}

    def load_iter(self, data_iter, with_metadata, copy):
        """
        Load an iterable of datasets into the database,
        at least per-table atomically.

        Args:
            data_iter:      The iterable of JSON datasets to load into the
                            database. Each dataset must adhere to the I/O
                            version of the database schema, and will be
                            modified, if "copy" is False.
            with_metadata:  True if any metadata in the datasets should
                            also be loaded into the database. False if it
                            should be discarded and the database should
                            generate its metadata itself.
            copy:           True, if the loaded data should be copied before
                            packing. False, if the loaded data should be
                            packed in-place.
        """
        assert self.is_initialized()
        assert isinstance(with_metadata, bool)
        assert isinstance(copy, bool)
        io_schema = self.get_schema()[1]
        # Exhaust generators
        for data in data_iter:
            assert io_schema.is_compatible_directly(data)
            assert LIGHT_ASSERTS or io_schema.is_valid_exactly(data)
