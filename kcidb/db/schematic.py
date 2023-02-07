"""Kernel CI reporting database - driver with discrete schemas"""

import inspect
from abc import ABCMeta, ABC, abstractmethod
import kcidb.io as io
import kcidb.orm as orm
from kcidb.db.misc import UnsupportedSchema
from kcidb.misc import LIGHT_ASSERTS
from kcidb.db.abstract import Driver as AbstractDriver


class MetaConnection(ABCMeta):
    """Connection metaclass"""

    def __init__(cls, name, bases, namespace, **kwargs):
        """
        Initialize a connection class.

        Args:
            cls:        The class to be initialized.
            name:       The name of the class being initialized.
            bases:      A list of base classes for the initialized class.
            namespace:  The class namespace.
            kwargs:     Other (opaque) metaclass arguments.
        """
        super().__init__(name, bases, namespace, **kwargs)
        if bases == (ABC,):
            return
        assert isinstance(cls._PARAMS_DOC, str), \
            "Connection subclass has no _PARAMS_DOC set"


class Connection(ABC, metaclass=MetaConnection):
    """An abstract database connection, to be customized for a schema"""

    # Documentation of the connection parameters
    _PARAMS_DOC = None

    @abstractmethod
    def __init__(self, params):
        """
        Initialize a database connection.

        Args:
            params: A string containing parameters for accessing a database,
                    or None, if not required by the particular connection.

        Raises:
            NotFound        - the database does not exist,
        """
        assert params is None or isinstance(params, str)

    @abstractmethod
    def set_schema_version(self, version):
        """
        Set the schema version of the connected database (or remove it) in a
        separate transaction. Does not modify the data or upgrade its actual
        schema.

        Args:
            version:    A tuple of (major, minor) schema version numbers (both
                        non-negative integers) to set. None to remove the
                        version.
        """
        assert version is None or \
            isinstance(version, tuple) and len(version) == 2 and \
            isinstance(version[0], int) and version[0] >= 0 and \
            isinstance(version[1], int) and version[1] >= 0

    @abstractmethod
    def get_schema_version(self):
        """
        Retrieve the schema version of the connected database, in a separate
        transaction.

        Returns:
            The major and the minor version numbers of the database schema,
            or None, if not initialized.
        """

    @abstractmethod
    def get_last_modified(self):
        """
        Get the time the data in the connected database was last modified.
        Can return the minimum timestamp constant, if the database is not
        initialized, or its data loading interface is not limited in the
        amount of load() method calls.

        Returns:
            A timezone-aware datetime object representing the last
            modification time.
        """

    def is_initialized(self):
        """
        Check if the connected database is initialized.

        Returns:
            True if the database is initialized, False otherwise.
        """
        return self.get_schema_version() is not None


class MetaSchema(ABCMeta):
    """Schema metaclass"""

    def __init__(cls, name, bases, namespace, **kwargs):
        """
        Initialize a schema class.

        Args:
            cls:        The class to be initialized.
            name:       The name of the class being initialized.
            bases:      A list of base classes for the initialized class.
            namespace:  The class namespace.
            kwargs:     Other (opaque) metaclass arguments.
        """
        assert len(bases) == 1
        super().__init__(name, bases, namespace, **kwargs)
        base = bases[0]

        # If this is the abstract schema
        if base is ABC:
            return

        assert issubclass(cls.Connection, base.Connection), \
            "Schema's connection is not derived from the parent's"

        assert "version" in namespace, \
            "Schema doesn't have its own version"
        assert isinstance(cls.version, tuple) and len(cls.version) == 2 and \
            isinstance(cls.version[0], int) and cls.version[0] >= 0 and \
            isinstance(cls.version[1], int) and cls.version[1] >= 0, \
            f"Schema has invalid version {cls.version!r}"

        assert issubclass(cls.io, io.schema.VA), \
            "Schema has invalid I/O version"

        # If this is is based on the abstract schema
        if base.__bases__[0] is ABC:
            assert "_inherit" not in namespace, \
                "Schema has an _inherit member but no parent"
            return

        assert cls.io >= base.io, \
            "Schema's I/O version is older than the parent's"
        assert cls.version > base.version, \
            "Schema's version is lower than the parent's"
        assert "_inherit" in namespace, \
            "Schema doesn't have its own _inherit() method"
        assert inspect.ismethod(cls._inherit), \
            "Schema has invalid _inherit() method"

    @property
    def lineage(cls):
        """
        A generator returning every schema version in (reverse order of)
        history, starting with this one and ending with the first one (the
        direct child of the abstract one).
        """
        while cls.__bases__[0] is not ABC:
            yield cls
            # Piss off, pylint: disable=self-cls-assignment
            cls = cls.__bases__[0]

    @property
    def history(cls):
        """
        A tuple containing every schema version in history, starting with the
        first version (the direct child of the abstract one) and ending with
        this one.
        """
        return tuple(reversed(tuple(cls.lineage)))


class Schema(ABC, metaclass=MetaSchema):
    """An abstract schema for a database driver"""

    # The connection class to use for talking to the database.
    # Must be a subclass of kcidb.db.schematic.Connection and a subclass of the
    # parent's connection class.
    Connection = Connection

    # The schema's version: a tuple of major and minor version numbers (both
    # non-negative integers).
    #
    # Increases in the major number signify backwards-incompatible changes,
    # such as adding a required table/column, or restricting an existing
    # table/column.
    #
    # Increases in the minor number signify backwards-compatible changes, such
    # as adding an optional table/column, or relaxing an existing table/column
    #
    # Must be larger than the parent's major number.
    version = None

    # The I/O schema the database schema supports
    # Must be a subclass of the parent's version.
    io = None

    # Not abstract to allow first version to instantiate
    @classmethod
    def _inherit(cls, conn):
        """
        Inerit the database data from the previous schema version (if any).

        Args:
            conn:   Connection to the database to inherit. The database must
                    comply with the previous version of the schema.
        """
        assert isinstance(conn, cls.Connection)

    def __init__(self, conn):
        """
        Initialize a database driver schema.

        Args:
            conn:   An object representing the connection to the database to
                    access, corresponding to the schema's driver.
        """
        assert isinstance(conn, self.Connection)
        self.conn = conn

    @abstractmethod
    def init(self):
        """
        Initialize the database. The database must be uninitialized.
        """

    @abstractmethod
    def cleanup(self):
        """
        Cleanup (deinitialize) the database, removing all data.
        The database must be initialized.
        """

    @abstractmethod
    def empty(self):
        """
        Empty the database, removing all data.
        The database must be initialized.
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
            An iterator returning report JSON data adhering to the current
            database schema's I/O schema version, each containing at most the
            specified number of objects.
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
            An iterator returning report JSON data adhering to the current
            database schema's I/O schema version, each containing at most the
            specified number of objects.
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
        assert all(isinstance(r, orm.Pattern) for r in pattern_set)

    @abstractmethod
    def load(self, data):
        """
        Load data into the database.
        The database must be initialized.

        Args:
            data:   The JSON data to load into the database.
                    Must adhere to the schema's version of the I/O schema.
        """
        # Relying on the driver to check compatibility/validity


class MetaDriver(ABCMeta):
    """A schematic metadriver"""

    def __init__(cls, name, bases, namespace, **kwargs):
        """
        Initialize a driver class.

        Args:
            cls:        The class to be initialized.
            name:       The name of the class being initialized.
            bases:      A list of base classes for the initialized class.
            namespace:  The class namespace.
            kwargs:     Other (opaque) metaclass arguments.
        """
        assert len(bases) == 1
        super().__init__(name, bases, namespace, **kwargs)
        base = bases[0]
        if base is AbstractDriver:
            return
        assert issubclass(cls.LatestSchema, base.LatestSchema), \
            "Driver's latest schema is not derived from the parent's"
        assert isinstance(cls._DOC, str), \
            "Driver subclass has no _DOC set"


class Driver(AbstractDriver, metaclass=MetaDriver):
    """An abstract driver with discreetly-defined schemas"""

    # A class representing the latest database schema recognized by the
    # driver. A subclass of kcidb.db.schematic.Schema, and of the parent
    # driver's latest schema.
    LatestSchema = Schema

    # Driver documentation
    _DOC = None

    @classmethod
    def get_doc(cls):
        """
        Get driver documentation.

        Returns:
            The driver documentation string.
        """
        # It's OK, we're friends, pylint: disable=protected-access
        return cls._DOC + "\n" + cls.LatestSchema.Connection._PARAMS_DOC

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
            NotFound            - the database does not exist
            UnsupportedSchema   - the database schema is not supported by the
                                  driver
        """
        assert params is None or isinstance(params, str)
        super().__init__(params)
        self.conn = self.LatestSchema.Connection(params)
        version = self.conn.get_schema_version()
        # If the database is not initialized with a schema
        if version is None:
            self.schema = None
        else:
            # Use the latest matching schema
            for schema in self.LatestSchema.lineage:
                if schema.version[0] == version[0] and \
                        schema.version[1] <= version[1]:
                    break
            else:
                raise UnsupportedSchema(
                        f"Database schema v{version[0]}.{version[1]} "
                        f"is unsupported"
                )
            self.schema = schema(self.conn)

    def is_initialized(self):
        """
        Check if the driven database is initialized.

        Returns:
            True if the database is initialized, False if not.
        """
        return self.schema is not None

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
        for schema in self.LatestSchema.lineage:
            if schema.version == version:
                break
        else:
            raise Exception("Schema version {version!r} is not available")
        self.schema = schema(self.conn)
        self.schema.init()
        self.conn.set_schema_version(version)

    def cleanup(self):
        """
        Cleanup (deinitialize) the driven database, removing all data.
        The database must be initialized.
        """
        assert self.is_initialized()
        self.schema.conn.set_schema_version(None)
        self.schema.cleanup()
        self.schema = None

    def empty(self):
        """
        Empty the driven database, removing all data.
        The database must be initialized.
        """
        assert self.is_initialized()
        self.schema.empty()

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
        return self.conn.get_last_modified()

    def get_schemas(self):
        """
        Retrieve available database schemas: a dictionary of tuples containing
        major and minor version numbers of the schemas (both non-negative
        integers), and corresponding I/O schemas
        (kcidb_io.schema.abstract.Version instances) supported by them.

        Returns:
            The schema dictionary, sorted by ascending version numbers.
        """
        return {
            schema.version: schema.io
            for schema in self.LatestSchema.history
        }

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
        return self.schema.version, self.schema.io

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
        current_version = self.schema.version
        assert target_version >= current_version, \
            "Target schema is older than the current schema"

        # Collect newer schemas
        newer_schemas = []
        for schema in self.LatestSchema.lineage:
            if schema.version == current_version:
                break
            if schema.version <= target_version:
                newer_schemas.insert(0, schema)
        else:
            raise Exception("Target schema is not a driver's newer schema")

        # Inherit data through all newer versions up to the target one
        for schema in newer_schemas:
            # The metaclass makes sure each schema has its own _inherit()
            # It's OK, we're friends, pylint: disable=protected-access
            schema._inherit(self.conn)
            self.conn.set_schema_version(schema.version)
            self.schema = schema(self.conn)

    def dump_iter(self, objects_per_report):
        """
        Dump all data from the database in object number-limited chunks.
        The database must be initialized.

        Args:
            objects_per_report: An integer number of objects per each returned
                                report data, or zero for no limit.

        Returns:
            An iterator returning report JSON data adhering to the schema's
            I/O schema version, each containing at most the specified number
            of objects.
        """
        assert isinstance(objects_per_report, int)
        assert objects_per_report >= 0
        assert self.is_initialized()
        return self.schema.dump_iter(objects_per_report)

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
            An iterator returning report JSON data adhering to the schema's
            I/O schema version, each containing at most the specified number
            of objects.
        """
        assert isinstance(ids, dict)
        assert all(isinstance(k, str) and isinstance(v, list) and
                   all(isinstance(e, str) for e in v)
                   for k, v in ids.items())
        assert isinstance(objects_per_report, int)
        assert objects_per_report >= 0
        assert self.is_initialized()
        return self.schema.query_iter(
            ids, children, parents, objects_per_report
        )

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
        assert all(isinstance(r, orm.Pattern) for r in pattern_set)
        assert self.is_initialized()
        return self.schema.oo_query(pattern_set)

    def load(self, data):
        """
        Load data into the database.
        The database must be initialized.

        Args:
            data:   The JSON data to load into the database.
                    Must adhere to the schema's version of the I/O schema.
        """
        assert self.is_initialized()
        assert self.schema.io.is_compatible_directly(data)
        assert LIGHT_ASSERTS or self.schema.io.is_valid_exactly(data)
        self.schema.load(data)
