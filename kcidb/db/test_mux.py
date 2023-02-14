"""kcdib.db.mux module tests"""

from itertools import zip_longest
import textwrap
import pytest
from kcidb_io.schema import V1_1, V2_0, V3_0, V4_0, V4_1
from kcidb.db.mux import Driver as MuxDriver
from kcidb.db.null import Driver as NullDriver
from kcidb.db.misc import UnsupportedSchema


class DummyDriver(NullDriver):
    """A dummy driver with configurable number of schemas"""

    @classmethod
    def get_doc(cls):
        """
        Get driver documentation.

        Returns:
            The driver documentation string.
        """
        return textwrap.dedent("""\
            The dummy driver allows creating null drivers supporting
            specifying minimum and maximum I/O schema major version numbers,
            number of major schema versions per I/O schema, major schema
            version number increment, initial major schema version, initial
            minor schema version, and number of minor versions per major
            version. The initial major schema version can be specified
            negative to have the emulated database uninitialized (and the
            initial minor schema version ignored).

            Parameters: <MIN_IO_MAJOR>:
                        <MAX_IO_MAJOR>:
                        <MAJORS_PER_IO_SCHEMA>:
                        <MAJOR_STEP>:
                        <MAJOR>:
                        <MINOR>:
                        <MINORS_PER_MAJOR>

                        All optional, default being "1:4:1:1:0:0:1".
        """)

    def __init__(self, params):
        """
        Initialize the driver.

        Args:
            params: A string describing how many and which schema versions the
                    driver should have. See the return value of get_doc() for
                    details.
        """
        assert params is None or isinstance(params, str)
        super().__init__(None)

        min_io_major, max_io_major, \
            majors_per_io_schema, self.major_step, \
            self.major, self.minor, self.minors_per_major = (
                default if param is None else param
                for param, default in zip_longest(
                    (int(number) for number in (
                        params.split(":") if params else []
                    )),
                    (1, 4, 1, 1, 0, 0, 1)
                )
            )
        if self.major < 0:
            self.major = None
            self.minor = None
        assert min_io_major <= max_io_major
        assert majors_per_io_schema >= 1
        assert self.major_step >= 1
        assert (self.major is None) == (self.minor is None)
        assert self.major is None or self.major >= 0
        assert self.major is None or self.major % self.major_step == 0
        assert self.major is None or self.minor >= 0
        assert self.minors_per_major >= 1
        assert self.major is None or self.minor < self.minors_per_major
        io_schemas = filter(
            lambda x: min_io_major <= x.major <= max_io_major,
            V4_1.history
        )
        self.schemas = [
            io_schema
            for io_schema in io_schemas
            for i in range(0, majors_per_io_schema * self.minors_per_major)
        ]
        assert self.major is None or \
            0 <= (self.major / self.major_step * self.minors_per_major +
                  self.minor) < len(self.schemas)
        self.params = params

    def __repr__(self):
        return f"Dummy<{self.params}>"

    def get_schemas(self):
        """
        Retrieve available driver schemas: a dictionary of major and minor
        version numbers of the driver's schemas (non-negative integers), and
        corresponding I/O schemas (kcidb_io.schema.abstract.Version instances)
        supported by them.

        Returns:
            The schema dictionary.
        """
        return {
            (int(index / self.minors_per_major * self.major_step),
             index % self.minors_per_major): io_schema
            for index, io_schema in enumerate(self.schemas)
        }

    def is_initialized(self):
        """
        Check if the database is initialized.

        Returns:
            True if the database is initialized, False otherwise.
        """
        return self.major is not None

    def init(self, version):
        """
        Initialize the driven database. The database must be uninitialized.

        Args:
            version:    A tuple of the major and minor version numbers (both
                        non-negative integers) of the schema to initialize the
                        database to (must be one of the driver's available
                        schema versions)
        """
        assert version in self.get_schemas()
        (self.major, self.minor) = version

    def get_schema(self):
        """
        Get the driven database schema's major version number and the I/O
        schema supported by it. The database must be initialized.

        Returns:
            The major version number (a non-negative integer) of the database
            schema and the I/O schema (a kcidb_io.schema.abstract.Version)
            supported by it.
        """
        return \
            (self.major, self.minor), \
            self.schemas[int(self.major / self.major_step) *
                         self.minors_per_major]

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
        assert isinstance(target_version, tuple)
        assert len(target_version) == 2
        assert target_version in self.get_schemas(), \
            "Target schema version is not available for the driver"
        assert target_version >= (self.major, self.minor), \
            "Target schema is older than the current schema"
        (self.major, self.minor) = target_version


class DummyMuxDriver(MuxDriver):
    """A driver muxing dummy drivers"""

    @classmethod
    def get_drivers(cls):
        """
        Retrieve a dictionary of driver names and types available for driver's
        control.

        Returns:
            A driver dictionary.
        """
        return dict(dummy=DummyDriver)


def test_param_parsing():
    """Check that parameters are parsed correctly"""
    # Single driver without parameters
    driver = DummyMuxDriver("dummy")
    assert driver.get_schemas() == {
        (0, 0): V1_1, (1, 0): V2_0, (2, 0): V3_0, (3, 0): V4_0,
        (4, 0): V4_1
    }
    # Two drivers without parameters
    driver = DummyMuxDriver("dummy dummy")
    assert driver.get_schemas() == {
        (0, 0): V1_1, (1, 0): V1_1, (2, 0): V2_0, (3, 0): V2_0,
        (4, 0): V3_0, (5, 0): V3_0, (6, 0): V4_0, (7, 0): V4_0,
        (8, 0): V4_1
    }
    # Single driver with parameters
    driver = DummyMuxDriver("dummy:1:3")
    assert driver.get_schemas() == {
        (0, 0): V1_1, (1, 0): V2_0, (2, 0): V3_0
    }
    # Two drivers with parameters
    driver = DummyMuxDriver("dummy:1:3 dummy:1:3")
    assert driver.get_schemas() == {
        (0, 0): V1_1, (1, 0): V1_1, (2, 0): V2_0, (3, 0): V2_0,
        (4, 0): V3_0
    }
    # First driver with parameters
    driver = DummyMuxDriver("dummy:1:3 dummy")
    assert driver.get_schemas() == {
        (0, 0): V1_1, (1, 0): V1_1, (2, 0): V2_0, (3, 0): V2_0,
        (4, 0): V3_0, (5, 0): V3_0, (6, 0): V3_0
    }
    # Second driver with parameters
    driver = DummyMuxDriver("dummy dummy:1:3")
    assert driver.get_schemas() == {
        (0, 0): V1_1, (1, 0): V1_1, (2, 0): V2_0, (3, 0): V2_0,
        (4, 0): V3_0, (5, 0): V3_0, (6, 0): V3_0
    }

    # Newline separation
    driver = DummyMuxDriver("dummy\ndummy")
    assert driver.get_schemas() == {
        (0, 0): V1_1, (1, 0): V1_1, (2, 0): V2_0, (3, 0): V2_0,
        (4, 0): V3_0, (5, 0): V3_0, (6, 0): V4_0, (7, 0): V4_0,
        (8, 0): V4_1
    }

    # Long separation
    driver = DummyMuxDriver("dummy \r\n\t\vdummy")
    assert driver.get_schemas() == {
        (0, 0): V1_1, (1, 0): V1_1, (2, 0): V2_0, (3, 0): V2_0,
        (4, 0): V3_0, (5, 0): V3_0, (6, 0): V4_0, (7, 0): V4_0,
        (8, 0): V4_1
    }


def test_initialized():
    """Check that initialization status is handled correctly"""

    # All databases initialized
    assert DummyMuxDriver("dummy").is_initialized()
    assert DummyMuxDriver("""
        dummy
        dummy
    """).is_initialized()

    # All databases uninitialized
    assert not DummyMuxDriver("dummy:1:4:1:1:-1").is_initialized()
    assert not DummyMuxDriver("""
        dummy:1:4:1:1:-1
        dummy:1:4:1:1:-1
    """).is_initialized()

    # Some databases initialized, some not
    with pytest.raises(UnsupportedSchema):
        DummyMuxDriver("""
            dummy:1:4:1:1:0
            dummy:1:4:1:1:-1
        """)

    # Get schemas of a single uninitialized member driver
    driver = DummyMuxDriver("""
        dummy:1:4:1:1:-1
    """)
    assert not driver.is_initialized()
    assert driver.get_schemas() == {
        (0, 0): V1_1, (1, 0): V2_0, (2, 0): V3_0, (3, 0): V4_0,
        (4, 0): V4_1,
    }
    # Initialize a single-member driver to the first version
    driver.init((0, 0))
    assert driver.is_initialized()
    assert driver.get_schema() == ((0, 0), V1_1)

    # Initialize a single-member driver to the last version
    driver = DummyMuxDriver("""
        dummy:1:4:1:1:-1
    """)
    driver.init((4, 0))
    assert driver.is_initialized()
    assert driver.get_schema() == ((4, 0), V4_1)

    # Get schemas of an uninitialized two-member driver
    driver = DummyMuxDriver("""
        dummy:1:4:1:1:-1
        dummy:1:4:1:1:-1
    """)
    assert not driver.is_initialized()
    assert driver.get_schemas() == {
        (0, 0): V1_1, (1, 0): V1_1, (2, 0): V2_0, (3, 0): V2_0,
        (4, 0): V3_0, (5, 0): V3_0, (6, 0): V4_0, (7, 0): V4_0,
        (8, 0): V4_1,
    }
    # Initialize a two-member driver to the first version
    driver.init((0, 0))
    assert driver.is_initialized()
    assert driver.get_schema() == ((0, 0), V1_1)

    # Initialize a two-member driver to the last version
    driver = DummyMuxDriver("""
        dummy:1:4:1:1:-1
        dummy:1:4:1:1:-1
    """)
    driver.init((8, 0))
    assert driver.is_initialized()
    assert driver.get_schema() == ((8, 0), V4_1)

    # Initialize a two-member driver to a middle version
    driver = DummyMuxDriver("""
        dummy:1:4:1:1:-1
        dummy:1:4:1:1:-1
    """)
    driver.init((4, 0))
    assert driver.is_initialized()
    assert driver.get_schema() == ((4, 0), V3_0)


def test_schemas():  # It's OK, pylint: disable=too-many-branches
    """Check that schemas are enumerated and are upgradable"""

    # Single driver with simple version history
    driver = DummyMuxDriver("dummy")
    assert driver.get_schema() == ((0, 0), V1_1)
    assert driver.get_schemas() == {
        (0, 0): V1_1, (1, 0): V2_0, (2, 0): V3_0, (3, 0): V4_0,
        (4, 0): V4_1,
    }
    for version in driver.get_schemas():
        if version > driver.get_schema()[0]:
            driver.upgrade(version)
    assert driver.get_schema() == ((4, 0), V4_1)

    # Multiple drivers starting with different I/O versions
    driver = DummyMuxDriver("""
        dummy:1:5:1:1:0
        dummy:1:5:2:1:2
        dummy:1:5:3:1:6
    """)
    assert driver.get_schema() == ((0, 0), V1_1)
    assert driver.get_schemas() == {
        (0, 0): V1_1, (1, 0): V2_0, (2, 0): V2_0, (3, 0): V2_0,
        (4, 0): V3_0, (5, 0): V3_0, (6, 0): V3_0, (7, 0): V3_0,
        (8, 0): V3_0, (9, 0): V3_0, (10, 0): V4_0, (11, 0): V4_0,
        (12, 0): V4_0, (13, 0): V4_0, (14, 0): V4_0, (15, 0): V4_0,
        (16, 0): V4_1, (17, 0): V4_1, (18, 0): V4_1, (19, 0): V4_1,
    }
    for version in driver.get_schemas():
        if version > driver.get_schema()[0]:
            driver.upgrade(version)
    assert driver.get_schema() == ((19, 0), V4_1)

    # Staggered driver schema version numbers
    driver = DummyMuxDriver("""
        dummy:1:5:1:3:0
        dummy:1:5:2:2:4
        dummy:1:5:3:1:6
    """)
    assert driver.get_schema() == ((0, 0), V1_1)
    assert driver.get_schemas() == {
        (0, 0): V1_1, (1, 0): V2_0, (2, 0): V2_0, (3, 0): V2_0,
        (4, 0): V3_0, (5, 0): V3_0, (6, 0): V3_0, (7, 0): V3_0,
        (8, 0): V3_0, (9, 0): V3_0, (10, 0): V4_0, (11, 0): V4_0,
        (12, 0): V4_0, (13, 0): V4_0, (14, 0): V4_0, (15, 0): V4_0,
        (16, 0): V4_1, (17, 0): V4_1, (18, 0): V4_1, (19, 0): V4_1,
    }
    for version in driver.get_schemas():
        if version > driver.get_schema()[0]:
            driver.upgrade(version)
    assert driver.get_schema() == ((19, 0), V4_1)

    # Misaligned I/O version histories
    driver = DummyMuxDriver("""
        dummy:2:5:1:1:0
        dummy:1:4:1:1:0
        dummy:2:3:1:1:0
    """)
    assert driver.get_schemas() == {
        (0, 0): V1_1, (1, 0): V2_0, (2, 0): V2_0, (3, 0): V2_0,
        (4, 0): V3_0, (5, 0): V3_0, (6, 0): V3_0, (7, 0): V3_0,
        (8, 0): V3_0
    }
    for version in driver.get_schemas():
        if version > driver.get_schema()[0]:
            driver.upgrade(version)
    assert driver.get_schema() == ((8, 0), V3_0)

    # Disconnected I/O version histories
    driver = DummyMuxDriver("""
        dummy:1:2:1:1:0
        dummy:4:5:1:1:0
    """)
    assert driver.get_schemas() == {
        (0, 0): V1_1, (1, 0): V2_0, (2, 0): V2_0
    }
    for version in driver.get_schemas():
        if version > driver.get_schema()[0]:
            driver.upgrade(version)
    assert driver.get_schema() == ((2, 0), V2_0)

    # Multiple minor versions
    driver = DummyMuxDriver("""
        dummy:1:5:1:1:0:0:1
        dummy:1:5:1:1:0:0:2
    """)
    assert driver.get_schemas() == {
        (0, 0): V1_1, (1, 0): V1_1, (1, 1): V1_1,
        (2, 0): V2_0, (3, 0): V2_0, (3, 1): V2_0,
        (4, 0): V3_0, (5, 0): V3_0, (5, 1): V3_0,
        (6, 0): V4_0, (7, 0): V4_0, (7, 1): V4_0,
        (8, 0): V4_1, (8, 1): V4_1,
    }
    for version in driver.get_schemas():
        if version > driver.get_schema()[0]:
            driver.upgrade(version)
    assert driver.get_schema() == ((8, 1), V4_1)

    # Multiple minor versions
    driver = DummyMuxDriver("""
        dummy:1:5:1:1:0:0:2
        dummy:1:5:1:1:0:0:1
    """)
    assert driver.get_schemas() == {
        (0, 0): V1_1, (0, 1): V1_1,
        (1, 0): V1_1, (2, 0): V2_0, (2, 1): V2_0,
        (3, 0): V2_0, (4, 0): V3_0, (4, 1): V3_0,
        (5, 0): V3_0, (6, 0): V4_0, (6, 1): V4_0,
        (7, 0): V4_0, (8, 0): V4_1, (8, 1): V4_1,
    }
    for version in driver.get_schemas():
        if version > driver.get_schema()[0]:
            driver.upgrade(version)
    assert driver.get_schema() == ((8, 1), V4_1)
