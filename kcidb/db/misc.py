"""Kernel CI reporting database - misc definitions"""

from abc import ABC, abstractmethod
import kcidb.io as io
import kcidb.orm
from kcidb.misc import LIGHT_ASSERTS


class Error(Exception):
    """An abstract error"""


class NotFound(Error):
    """A database doesn't exist"""

    def __init__(self, database):
        """Initialize the exception."""
        assert isinstance(database, str)
        super().__init__(f"Database {database!r} not found")


class IncompatibleSchema(Error):
    """Database schema is incompatible with the current I/O schema"""

    def __init__(self, db_major, db_minor):
        """
        Initialize the exception.

        Args:
            db_major:   Database schema major version number
            db_minor:   Database schema minor version number
        """
        super().__init__(f"Database schema {db_major}.{db_minor} "
                         f"is incompatible with I/O schema "
                         f"{io.SCHEMA.major}."
                         f"{io.SCHEMA.minor}")


class Driver(ABC):
    """
    An abstract Kernel CI report database driver.

    A driver doesn't have to be responsible for creating or removing a
    database, but if it is, it must do so implicitly, when initializing itself
    or the database.

    If the driver doesn't handle creating the database, it should raise the
    NotFound exception when initializing.
    """

    # Calm down, we're abstract, pylint: disable=no-self-use

    # A (multiline) string describing the driver
    # and documenting the format of its parameters
    DOC = None

    @abstractmethod
    def __init__(self, params):
        """
        Initialize the Kernel CI report database driver.

        Args:
            params: A string containing parameters for accessing a database.
                    Or None, if not specified.

        Raises:
            NotFound        - the database does not exist,
        """
        assert params is None or isinstance(params, str)

    @abstractmethod
    def is_initialized(self):
        """
        Check if the database is initialized (not empty).

        Returns:
            True if the database is initialized, False otherwise.
        """

    @abstractmethod
    def get_schema_version(self):
        """
        Get the version of the I/O schema the database schema corresponds to,
        if any. Assumes the database is initialized.

        Returns:
            Major and minor version numbers.
        """

    @abstractmethod
    def init(self):
        """
        Initialize the database. The database must be empty (uninitialized).
        """

    @abstractmethod
    def cleanup(self):
        """
        Cleanup (empty) the database, removing all data.
        The database must be initialized.
        """

    @abstractmethod
    def get_last_modified(self):
        """
        Get the time the data in the database was last modified.
        The database must be initialized.

        Returns:
            The datetime object representing the last modification time.
        """

    @abstractmethod
    def dump_iter(self, objects_per_report):
        """
        Dump all data from the database in object number-limited chunks.
        The database must be initialized.

        Args:
            objects_per_report: An integer number of objects per each returned
                                report data, or zero for no limit.

        Returns:
            An iterator returning report JSON data adhering to the current I/O
            schema version, each containing at most the specified number of
            objects.
        """
        assert isinstance(objects_per_report, int)
        assert objects_per_report >= 0

    @abstractmethod
    def query_iter(self, ids, children, parents, objects_per_report):
        """
        Match and fetch objects from the database, in object number-limited
        chunks. The database must be initialized.

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
            An iterator returning report JSON data adhering to the current I/O
            schema version, each containing at most the specified number of
            objects.
        """
        assert isinstance(ids, dict)
        assert all(isinstance(k, str) and isinstance(v, list) and
                   all(isinstance(e, str) for e in v)
                   for k, v in ids.items())
        del children
        del parents
        assert isinstance(objects_per_report, int)
        assert objects_per_report >= 0

    @abstractmethod
    def oo_query(self, pattern_set):
        """
        Query raw object-oriented data from the database.
        The database must be initialized.

        Args:
            pattern_set:    A set of patterns ("kcidb.orm.Pattern"
                            instances) matching objects to fetch.
        Returns:
            A dictionary of object type names and lists containing retrieved
            objects of the corresponding type.
        """
        assert isinstance(pattern_set, set)
        assert all(isinstance(r, kcidb.orm.Pattern) for r in pattern_set)

    @abstractmethod
    def load(self, data):
        """
        Load data into the database.
        The database must be initialized.

        Args:
            data:   The JSON data to load into the database.
                    Must adhere to the current version of the I/O schema.
        """
        assert LIGHT_ASSERTS or io.SCHEMA.is_valid_exactly(data)
