"""Kernel CI report database - null driver."""


import textwrap
import datetime
import kcidb.io as io
from kcidb.db.abstract import Driver as AbstractDriver


class Driver(AbstractDriver):
    """Kernel CI null database driver."""

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
        Get driven database schema version and supported I/O schema.

        Returns:
            A tuple of the major and minor version numbers (both non-negative
            integers) of the database schema, and the I/O schema (a
            kcidb_io.schema.abstract.Version) supported by it (allowed for
            loading).
        """
        return (0, 0), io.SCHEMA

    def get_schemas(self):
        """
        Get available database schemas and their corresponding I/O schemas.

        Returns:
            The schema dictionary, sorted by ascending version numbers.
        """
        return dict((self.get_schema(),))

    def upgrade(self, target_version):
        """
        Upgrade initialized database to specified schema.

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
        """Truncate all data and deinitialize the database."""

    def empty(self):
        """Clear all data from driven database(Requires initialization)."""

    def get_last_modified(self):
        """
        Retrieve the last modified time of the connected database.

        Returns:
            A timezone-aware datetime object representing the last
            modification time.
        """
        return datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)

    def dump_iter(self, objects_per_report):
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
        del objects_per_report
        yield io.SCHEMA.new()

    # We can live with this for now, pylint: disable=too-many-arguments
    def query_iter(self, ids, children, parents, objects_per_report):
        """
        Match and fetch objects from db, in object number-limited chunks.

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
        del ids, children, parents, objects_per_report
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

    def load(self, data):
        """
        Load data into the database.

        Args:
            data:   The JSON data to load into the database.
                    Must adhere to the current version of I/O schema.
        """
