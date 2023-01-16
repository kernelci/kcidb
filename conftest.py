"""Kernel CI reporting - shared test fixtures"""

import os
import atexit
import tempfile
import pytest
import kcidb.db


def mux_specs(specs):
    """
    For a given list of database spec strings generate a copy with extra spec
    strings of mux databases combining them. All databases are used in each
    mux spec string. Each database appears first once to act as the RW
    database.

    Args:
        specs:  An iterable containing spec string to add mux spec strings to.

    Returns:
        The list of spec strings with mux spec strings added.
    """
    specs = list(specs)
    return specs + [
        "mux:" + kcidb.db.misc.format_spec_list(
            [rw_spec] +
            [spec for spec in specs if spec is not rw_spec]
        )
        for rw_spec in specs
    ]


# Clean (uninitialized) sqlite database file
CLEAN_SQLITE_FILE = tempfile.mkstemp(suffix=".sqlite3")[1]
atexit.register(os.unlink, CLEAN_SQLITE_FILE)

# Empty (no-data) sqlite database file
EMPTY_SQLITE_FILE = tempfile.mkstemp(suffix=".sqlite3")[1]
atexit.register(os.unlink, EMPTY_SQLITE_FILE)
EMPTY_SQLITE_DATABASE = kcidb.db.Client("sqlite:" + EMPTY_SQLITE_FILE)
EMPTY_SQLITE_DATABASE.init()
del EMPTY_SQLITE_DATABASE

# Specs of "clean" (uninitialized) databases
CLEAN_DATABASE_SPECS = mux_specs(
    list(
        kcidb.db.misc.parse_spec_list(
            os.environ.get("KCIDB_CLEAN_TEST_DATABASES", "")
        )
    ) + ["sqlite:" + CLEAN_SQLITE_FILE]
)

# Specs of "empty" (no-data) databases
EMPTY_DATABASE_SPECS = mux_specs(
    list(
        kcidb.db.misc.parse_spec_list(
            os.environ.get("KCIDB_EMPTY_TEST_DATABASES", "")
        )
    ) + ["sqlite:" + EMPTY_SQLITE_FILE]
)

# "Clean" (uninitialized) databases
CLEAN_DATABASES = [kcidb.db.Client(spec) for spec in CLEAN_DATABASE_SPECS]

# "Empty" (no-data) databases
EMPTY_DATABASES = [kcidb.db.Client(spec) for spec in EMPTY_DATABASE_SPECS]


@pytest.fixture
def clean_database(_clean_database):
    """Reusable clean (uninitialized) database"""
    yield _clean_database
    if _clean_database.is_initialized():
        _clean_database.cleanup()


@pytest.fixture
def empty_database(_empty_database):
    """Reusable empty (no-data) database"""
    yield _empty_database
    _empty_database.empty()


def pytest_generate_tests(metafunc):
    """Process a collected pytest test function"""
    if "empty_database" in metafunc.fixturenames:
        metafunc.parametrize("_empty_database", EMPTY_DATABASES,
                             scope="session")
    if "clean_database" in metafunc.fixturenames:
        metafunc.parametrize("_clean_database", CLEAN_DATABASES,
                             scope="session")
