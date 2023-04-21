"""Kernel CI reporting database - abstract database definitions."""

from abc import ABC, abstractmethod
import kcidb.orm as orm
from kcidb.misc import LIGHT_ASSERTS


class Driver(ABC):
    """An abstract driver."""

    @classmethod
    @abstractmethod
    def get_doc(cls):
        """
        Get driver documentation.

        Returns:
            The driver documentation string.
        """

    @abstractmethod
    def __init__(self, params):
        """
        Initialize a database driver instance.

        Args:
            params: A string containing parameters for accessing a database,
                    or None, if not required by the particular driver.

        Raises:
            UnknownDriver       - an unknown sub-driver encountered in the
                                  specification string for a component
                                  database
            NotFound            - a database does not exist
            UnsupportedSchema   - a database schema is not supported by a
                                  driver
        """
        assert params is None or isinstance(params, str)

    @abstractmethod
    def is_initialized(self):
        """
        Check if the driven database is initialized.

        Returns:
            True if the database is initialized, False if not.
        """

    @abstractmethod
    def init(self, version):
        """
        Initialize the driven database. The database must be uninitialized.

        Args:
            version:    A tuple of the major and minor version numbers (both
                        non-negative integers) of the schema to initialize the
                        database to (must be one of the driver's available
                        schema versions)
        """
        assert not self.is_initialized()
        assert version in self.get_schemas(), \
            "Schema version is not available for the driver"

    @abstractmethod
    def cleanup(self):
        """Database cleanup: remove all data and deinitialize."""
        assert self.is_initialized()

    @abstractmethod
    def empty(self):
        """Clear all data from the initialized database."""
        assert self.is_initialized()

    @abstractmethod
    def get_last_modified(self):
        """
        Get last modified time of initialized database.

        Returns:
            A timezone-aware datetime object representing the last
            modification time.
        """

    @abstractmethod
    def get_schemas(self):
        """
        Get available db schema versions and their corresponding I/O schemas.

        Returns:
            The schema dictionary, sorted by ascending version numbers.
        """

    @abstractmethod
    def get_schema(self):
        """
        Get database schema version and I/O schema (Initialized DB).

        Returns:
            A tuple of the major and minor version numbers (both non-negative
            integers) of the database schema, and the I/O schema (a
            kcidb_io.schema.abstract.Version) supported by it (allowed for
            loading).
        """
        assert self.is_initialized()

    @abstractmethod
    def upgrade(self, target_version):
        """
        Upgrade initialized database to schema.

        Args:
            target_version: A tuple of the major and minor version numbers of
                            the schema to upgrade to (must be one of the
                            database's available schema versions, newer than
                            the current one).
        """
        assert self.is_initialized()
        assert target_version in self.get_schemas(), \
            "Target schema version is not available for the driver"
        current_version = self.get_schema()[0]
        assert target_version >= current_version, \
            "Target schema is older than the current schema"

    @abstractmethod
    def dump_iter(self, objects_per_report):
        """
        Dump all data from initialized database in chunks.

        Args:
            objects_per_report: An integer number of objects per each returned
                                report data, or zero for no limit.

        Returns:
            An iterator returning report JSON data adhering to the current
            database schema's I/O schema version, each containing at most the
            specified number of objects.
        """
        assert isinstance(objects_per_report, int)
        assert objects_per_report >= 0
        assert self.is_initialized()

    @abstractmethod
    def query_iter(self, ids, children, parents, objects_per_report):
        """
        Fetch objects from database in chunks by object number limit.

        Args:
            ids:                A dictionary of object list names, and lists
                                of IDs of objects to match.
            children:           True if children of matched objects should be
                                matched as well.
            parents:            True if parents of matched objects should be
                                matched as well.
            objects_per_report: An integer number of objects per each returned
                                report data, or zero for no limit.

        Returns:
            An iterator returning report JSON data adhering to the current
            database schema's I/O schema version, each containing at most the
            specified number of objects.
        """
        assert isinstance(ids, dict)
        assert all(isinstance(k, str) and isinstance(v, list) and
                   all(isinstance(e, str) for e in v)
                   for k, v in ids.items())
        assert isinstance(objects_per_report, int)
        assert objects_per_report >= 0
        assert self.is_initialized()

    @abstractmethod
    def oo_query(self, pattern_set):
        """
        Retrieve raw object-oriented data from the database.

        Args:
            pattern_set:    A set of patterns ("kcidb.orm.query.Pattern"
                            instances) matching objects to fetch.
        Returns:
            A dictionary of object type names and lists containing retrieved
            objects of the corresponding type.
        """
        assert isinstance(pattern_set, set)
        assert all(isinstance(r, orm.query.Pattern)
                   for r in pattern_set)
        assert self.is_initialized()

    @abstractmethod
    def load(self, data):
        """
        Load data into the database(required initialized).

        Args:
            data:   The JSON data to load into the database.
                    Must adhere to the current database schema's version of
                    the I/O schema.
        """
        assert self.is_initialized()
        io_schema = self.get_schema()[1]
        assert io_schema.is_compatible_directly(data)
        assert LIGHT_ASSERTS or io_schema.is_valid_exactly(data)
