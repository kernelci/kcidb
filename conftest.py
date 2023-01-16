"""Kernel CI reporting - shared test fixtures"""

import os
import atexit
import tempfile
import pytest
import kcidb.db


# Specs of "clean" (uninitialized) databases
CLEAN_DATABASE_SPECS = list(kcidb.db.misc.parse_spec_list(
    os.environ.get("KCIDB_CLEAN_TEST_DATABASES", "")
))

if not CLEAN_DATABASE_SPECS:
    # Clean (uninitialized) sqlite database file
    CLEAN_SQLITE_FILE = tempfile.mkstemp(suffix=".sqlite3")[1]
    atexit.register(os.unlink, CLEAN_SQLITE_FILE)
    CLEAN_DATABASE_SPECS = ["sqlite:" + CLEAN_SQLITE_FILE]

# Specs of "empty" (no-data) databases
EMPTY_DATABASE_SPECS = list(kcidb.db.misc.parse_spec_list(
    os.environ.get("KCIDB_EMPTY_TEST_DATABASES", "")
))

if not EMPTY_DATABASE_SPECS:
    # Empty (no-data) sqlite database file
    EMPTY_SQLITE_FILE = tempfile.mkstemp(suffix=".sqlite3")[1]
    atexit.register(os.unlink, EMPTY_SQLITE_FILE)
    EMPTY_DATABASE_SPECS = ["sqlite:" + EMPTY_SQLITE_FILE]
    EMPTY_SQLITE_DATABASE = kcidb.db.Client(EMPTY_DATABASE_SPECS[0])
    EMPTY_SQLITE_DATABASE.init()
    del EMPTY_SQLITE_DATABASE

# "Clean" (uninitialized) databases indexed by their specifications
CLEAN_DATABASES = {
    spec: kcidb.db.Client(spec)
    for spec in CLEAN_DATABASE_SPECS
}

# "Empty" (no-data) databases indexed by their specifications
EMPTY_DATABASES = {
    spec: kcidb.db.Client(spec)
    for spec in EMPTY_DATABASE_SPECS
}


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
        metafunc.parametrize("_empty_database",
                             EMPTY_DATABASES.values(),
                             ids=EMPTY_DATABASES.keys(),
                             scope="session")
    if "clean_database" in metafunc.fixturenames:
        metafunc.parametrize("_clean_database",
                             CLEAN_DATABASES.values(),
                             ids=CLEAN_DATABASES.keys(),
                             scope="session")
