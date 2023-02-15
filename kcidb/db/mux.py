"""Kernel CI reporting database - multiplexing"""

import textwrap
from abc import abstractmethod
import kcidb.io as io
import kcidb.db.misc
from kcidb.db.abstract import Driver as AbstractDriver


class Driver(AbstractDriver):
    """Abstract multiplexing driver"""

    @classmethod
    @abstractmethod
    def get_drivers(cls):
        """
        Retrieve a dictionary of driver names and types available for driver's
        control.

        Returns:
            A driver dictionary.
        """

    @classmethod
    def get_doc(cls):
        """
        Get driver documentation.

        Returns:
            The driver documentation string.
        """
        return textwrap.dedent("""\
            The mux driver allows loading data into multiple databases at
            once, and querying one of them.

            Parameters: <DATABASES>

            <DATABASES> A whitespace-separated list of strings describing the
                        multiplexed databases (<DRIVER>[:<PARAMS>] pairs). Any
                        spaces or backslashes in database strings need to be
                        escaped with backslashes. All databases have to either
                        be initialized or not, at the same time. Each database
                        will receive the loaded data, but only the first one
                        will be queried.
        """)

    @staticmethod
    def _drivers_are_initialized(drivers):
        """
        Check if all databases driven by a list of drivers are initialized.

        Args:
            drivers:    The drivers for the driven databases to check.

        Returns:
            True if all the driven databases are initialized,
            False if none of the driven databases are initialized,
            None if some of the driven databases are initialized and some are
            not, or there are no driven databases.
        """
        results = set(
            driver.is_initialized()
            for driver in drivers
        )
        return bool(results.pop()) if len(results) == 1 else None

    @staticmethod
    def _drivers_get_schemas(drivers):
        """
        Generate a dictionary of the driver's schema version numbers and
        corresponding I/O schema and schema version numbers of specified member
        drivers. All databases driven by the drivers should either be
        initialized, or not initialized, at the same time.

        The driver's schema version numbers start with 0.0, corresponding to
        the schema versions currently used by the drivers, if all databases
        are initialized, or to the minimum schema versions, if all databases
        are not initialized.

        Args:
            drivers:    The member drivers to generate the dictionary for.

        Returns:
            A dictionary of tuples, containing major and minor version numbers
            of the driver's schema versions, and tuples containing the
            supported I/O schema version, and a dictionary of member drivers
            and their (major, minor) schema version tuples to be used.
        """
        initialized = Driver._drivers_are_initialized(drivers)
        assert initialized is not None

        # A dictionary of drivers and lists of tuples, each containing a
        # driver's (major, minor) version tuple and the corresponding I/O
        # schema, sorted lowest driver versions first
        driver_schemas = {
            driver: list(driver.get_schemas().items())
            for driver in drivers
        }
        # A dictionary of drivers and indices into driver_schemas lists
        # representing the currently-processed schema combination,
        # initialized to the currently-used schemas, or to the first one, if
        # none
        driver_indices = {
            driver: driver_schemas[driver].index(driver.get_schema())
            if driver.is_initialized() else 0
            for driver in drivers
        }
        # A list of tuples, each containing an I/O version supported by the
        # mux driver, and a dictionary of member drivers and their schema
        # (major, minor) version tuples to be used.
        schemas = []

        def add_indexed_schema():
            """
            Add a version of a schema described by the current value of
            driver_indices to the "schemas" list.
            """
            schemas.append((
                # Record the minimum supported I/O version across
                # currently-selected schemas of all drivers
                min((
                    driver_schemas[driver][index][1]
                    for driver, index in driver_indices.items()
                )),
                # Record currently-selected (major, minor) schema versions for
                # each driver
                {
                    driver: driver_schemas[driver][index][0]
                    for driver, index in driver_indices.items()
                }
            ))

        # Generate a list of available schema versions - combinations of
        # member driver schema versions, sorted in the order of increasing I/O
        # version number support (and thus driver schema version order too)

        # From oldest to newest I/O version
        for io_schema in io.schema.LATEST.history:
            # For each driver
            for driver in driver_indices:
                # For each driver schema using the current I/O version
                while driver_indices[driver] < \
                        len(driver_schemas[driver]) - 1 and \
                        driver_schemas[driver][driver_indices[driver]][1] \
                        is io_schema:
                    add_indexed_schema()
                    driver_indices[driver] += 1
        add_indexed_schema()

        # A dictionary of tuples, containing major and minor version numbers
        # of the mux driver's schema versions, and tuples containing the
        # supported I/O schema version, and a dictionary of member drivers and
        # their (major, minor) schema version tuples to be used.
        versioned_schemas = {}
        major = 0
        minor = 0
        for i, (io_version, driver_versions) in enumerate(schemas):
            if i > 0:
                # For each driver and its version in the previous schema
                for driver, prev_version in schemas[i - 1][1].items():
                    # If at least one driver increased its major version
                    if driver_versions[driver][0] > prev_version[0]:
                        major += 1
                        minor = 0
            versioned_schemas[(major, minor)] = (io_version, driver_versions)
            minor += 1

        return versioned_schemas

    def __init__(self, params):
        """
        Initialize the multiplexing driver.

        Args:
            params: A parameter string describing the databases to
                    access. See MuxDriver.DOC for documentation.
                    Cannot be None (must be specified).

        Raises:
            UnknownDriver       - an unknown sub-driver encountered in the
                                  specification string for a component
                                  database
            NotFound            - a database does not exist
            UnsupportedSchema   - a database schema is not supported by a
                                  driver, or not all databases are
                                  (not)initialized at the same time.
        """
        assert params is None or isinstance(params, str)
        super().__init__(params)
        if params is None:
            raise Exception("Database parameters must be specified\n\n" +
                            self.get_doc())
        self.drivers = list(
            kcidb.db.misc.instantiate_spec_list(self.get_drivers(), params)
        )
        initialized = Driver._drivers_are_initialized(self.drivers)
        if initialized is None:
            raise kcidb.db.misc.UnsupportedSchema(
                f"Initialization status is inconsistent for {params!r}"
            )
        # The driver's available schemas, starting with the currently-used
        # by the drivers, or with the minimum ones, if none,
        # pointed to by v0.0
        self.schemas = Driver._drivers_get_schemas(self.drivers)
        # The current (first) schema version
        self.version = (0, 0) if initialized else None

    def is_initialized(self):
        """
        Check if all member databases are initialized.

        Returns:
            True if all member database are initialized,
            False if all are not.
        """
        return self.version is not None

    def init(self, version):
        """
        Initialize the member databases.
        All the databases must be uninitialized.

        Args:
            version:    A tuple of the major and minor version numbers (both
                        non-negative integers) of the schema to initialize the
                        database to (must be one of the database's available
                        schema versions).
        """
        assert not self.is_initialized()
        assert version in self.schemas, "Schema version is not available"
        schema = self.schemas[version]
        for driver in self.drivers:
            driver.init(version=schema[1][driver])
        self.version = version

    def cleanup(self):
        """
        Cleanup (deinitialize) the databases, removing all data.
        All the databases must be initialized.
        """
        for driver in self.drivers:
            driver.cleanup()
        self.version = None

    def empty(self):
        """
        Empty the driven databases, removing all data.
        All the databases must be initialized.
        """
        for driver in self.drivers:
            driver.empty()

    def get_last_modified(self):
        """
        Get the time the data in the driven databases was last modified.
        Can return the minimum timestamp constant, if the databases are not
        initialized, or their data loading interface is not limited in the
        amount of load() method calls.

        Returns:
            A timezone-aware datetime object representing the last
            modification time.
        """
        return max(driver.get_last_modified() for driver in self.drivers)

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
            version: io_version
            for version, (io_version, _) in self.schemas.items()
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
        return self.version, self.schemas[self.version][0]

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
        assert target_version in self.schemas, \
            "Target schema version is not available"
        assert target_version >= self.version, \
            "Target schema is older than the current schema"
        for version, (_, driver_versions) in self.schemas.items():
            if version > self.version:
                if version > target_version:
                    break
                for driver, driver_version in driver_versions.items():
                    driver.upgrade(driver_version)
                self.version = version

    def dump_iter(self, objects_per_report):
        """
        Dump all data from the first database in object number-limited chunks.

        Args:
            objects_per_report: An integer number of objects per each returned
                                report data, or zero for no limit.

        Returns:
            An iterator returning report JSON data adhering to the current I/O
            schema version, each containing at most the specified number of
            objects.
        """
        yield from self.drivers[0].dump_iter(objects_per_report)

    # We can live with this for now, pylint: disable=too-many-arguments
    def query_iter(self, ids, children, parents, objects_per_report):
        """
        Match and fetch objects from the first database, in object
        number-limited chunks.

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
        yield from self.drivers[0].query_iter(
            ids, children, parents, objects_per_report
        )

    def oo_query(self, pattern_set):
        """
        Query raw object-oriented data from the first database.

        Args:
            pattern_set:    A set of patterns ("kcidb.oo.data.Pattern"
                            instances) matching objects to fetch.
        Returns:
            A dictionary of object type names and lists containing retrieved
            objects of the corresponding type.
        """
        return self.drivers[0].oo_query(pattern_set)

    def load(self, data):
        """
        Load data into the databases.

        Args:
            data:   The JSON data to load into the databases.
                    Must adhere to the current schema's version of the I/O
                    schema.
        """
        # The mux driver I/O schema is the oldest across member drivers
        io_schema = self.get_schema()[1]
        assert io_schema.is_compatible_directly(data)
        # Load data into every driver
        for driver in self.drivers:
            # Only copy if we need to upgrade
            driver_io_schema = driver.get_schema()[1]
            driver.load(
                driver_io_schema.upgrade(data)
                if driver_io_schema != io_schema else data
            )
