"""Kernel CI report database - null driver"""

import textwrap
import datetime
import kcidb.io as io
from kcidb.db.misc import Driver as AbstractDriver


class Driver(AbstractDriver):
    """Kernel CI null database driver"""

    DOC = textwrap.dedent("""\
        The null driver discards any loaded data and returns nothing for
        any query. This driver does not take parameters.
    """)

    # Yes, it's a special driver, pylint: disable=no-self-use
    # Yes, it's an abstract class, pylint: disable=super-init-not-called
    def __init__(self, params):
        """
        Initialize the BigQuery driver.

        Args:
            params:        Must be None (not specified).
        """
        assert params is None or isinstance(params, str)
        if params is not None:
            raise Exception("Database parameters are not accepted")

    def is_initialized(self):
        """
        Check if the database is initialized (not empty).

        Returns:
            True if the database is initialized, False otherwise.
        """
        return True

    def get_schema_version(self):
        """
        Get the version of the I/O schema the dataset schema corresponds to.

        Returns:
            Major and minor version numbers.
        """
        return io.SCHEMA.major, io.SCHEMA.minor

    def init(self):
        """
        Initialize the database.
        The database must be empty (uninitialized).
        """

    def cleanup(self):
        """
        Cleanup (empty) the database, removing all data.
        The database must be initialized (not empty).
        """

    def get_last_modified(self):
        """
        Get the time the data in the database was last modified.
        The database must be initialized (not empty).

        Returns:
            The datetime object representing the last modification time.
        """
        return datetime.datetime.min

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
        yield io.SCHEMA.new()

    # We can live with this for now, pylint: disable=too-many-arguments
    def query_iter(self, ids, children, parents, objects_per_report):
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
        return {}

    def load(self, data):
        """
        Load data into the database.

        Args:
            data:   The JSON data to load into the database.
                    Must adhere to a version of I/O schema.
        """
