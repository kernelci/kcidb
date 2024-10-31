"""Kernel CI reporting database - abstract database definitions"""

from abc import ABC, abstractmethod
import datetime
import kcidb.orm as orm
from kcidb.misc import LIGHT_ASSERTS


class Driver(ABC):
    """An abstract driver"""

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
        """
        Cleanup (deinitialize) the driven database, removing all data.
        The database must be initialized.
        """
        assert self.is_initialized()

    @abstractmethod
    def empty(self):
        """
        Empty the driven database, removing all data.
        The database must be initialized.
        """
        assert self.is_initialized()

    def purge(self, before):
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
        return False

    @abstractmethod
    def get_current_time(self):
        """
        Get the current time from the database server.

        Returns:
            A timezone-aware datetime object representing the current
            time on the database server.
        """

    @abstractmethod
    def get_last_modified(self):
        """
        Get the time the data in the driven database was last modified.
        Can return the minimum timestamp constant, if the database is not
        initialized, or its data loading interface is not limited in the
        amount of load() method calls.

        Returns:
            A timezone-aware datetime object representing the last
            modification time.
        """

    @abstractmethod
    def get_schemas(self):
        """
        Retrieve available database schemas: a dictionary of tuples containing
        major and minor version numbers of the schemas (both non-negative
        integers), and corresponding I/O schemas
        (kcidb_io.schema.abstract.Version instances) supported by them.

        Returns:
            The schema dictionary, sorted by ascending version numbers.
        """

    @abstractmethod
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

    @abstractmethod
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
        assert self.is_initialized()
        assert target_version in self.get_schemas(), \
            "Target schema version is not available for the driver"
        current_version = self.get_schema()[0]
        assert target_version >= current_version, \
            "Target schema is older than the current schema"

    @abstractmethod
    def dump_iter(self, objects_per_report, with_metadata):
        """
        Dump all data from the database in object number-limited chunks.
        The database must be initialized.

        Args:
            objects_per_report: An integer number of objects per each returned
                                report data, or zero for no limit.
            with_metadata:      True, if metadata fields should be dumped as
                                well. False, if not.

        Returns:
            An iterator returning report JSON data adhering to the current
            database schema's I/O schema version, each containing at most the
            specified number of objects.
        """
        assert isinstance(objects_per_report, int)
        assert objects_per_report >= 0
        assert isinstance(with_metadata, bool)
        assert self.is_initialized()

    # No, it's not, pylint: disable=too-many-return-statements
    def query_ids_are_valid(self, ids):
        """
        Verify the IDs to be queried are valid according to the I/O version
        supported by the database. The database must be initialized.

        Args:
            ids:    A dictionary of object list names, and lists of IDs of
                    objects to match. Each ID is a tuple of values. The values
                    should match the types, the order, and the number of the
                    object's ID fields as described by the database's I/O
                    schema (the "id_fields" attribute).

        Returns:
            True if the IDs are valid, false otherwise.
        """
        assert LIGHT_ASSERTS or self.is_initialized()
        id_fields = self.get_schema()[1].id_fields
        if not isinstance(ids, dict):
            return False
        for obj_list_name, values_list in ids.items():
            if obj_list_name not in id_fields:
                return False
            obj_id_fields = id_fields[obj_list_name]
            if not isinstance(values_list, list):
                return False
            for values in values_list:
                if not isinstance(values, tuple) or \
                   len(values) != len(obj_id_fields):
                    return False
                for value, type in zip(values, obj_id_fields.values()):
                    if not isinstance(value, type):
                        return False
        return True

    # We can live with this for now, pylint: disable=too-many-arguments
    # Or if you prefer, pylint: disable=too-many-positional-arguments
    @abstractmethod
    def query_iter(self, ids, children, parents, objects_per_report,
                   with_metadata):
        """
        Match and fetch objects from the database, in object number-limited
        chunks. The database must be initialized.

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
            An iterator returning report JSON data adhering to the current
            database schema's I/O schema version, each containing at most the
            specified number of objects.
        """
        assert self.is_initialized()
        assert self.query_ids_are_valid(ids)
        assert isinstance(objects_per_report, int)
        assert objects_per_report >= 0
        assert isinstance(with_metadata, bool)

    @abstractmethod
    def oo_query(self, pattern_set):
        """
        Query raw object-oriented data from the database.
        The database must be initialized.

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
    def load(self, data, with_metadata):
        """
        Load data into the database.
        The database must be initialized.

        Args:
            data:           The JSON data to load into the database.
                            Must adhere to the current database schema's
                            version of the I/O schema.
            with_metadata:  True if any metadata in the data should
                            also be loaded into the database. False if it
                            should be discarded and the database should
                            generate its metadata itself.
        """
        assert self.is_initialized()
        io_schema = self.get_schema()[1]
        assert io_schema.is_compatible_directly(data)
        assert LIGHT_ASSERTS or io_schema.is_valid_exactly(data)
        assert isinstance(with_metadata, bool)
