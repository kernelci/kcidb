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


# Only available when KCIDB_DEPLOYMENT is set to "This deployment is empty"
@pytest.fixture(
    params=["empty_deployment"]
    if os.environ.get("KCIDB_DEPLOYMENT", "") == "This deployment is empty"
    else [],
    scope="session"
)
def empty_deployment():
    """Reusable empty (no-data) deployment"""
    yield None
    project = os.environ["GCP_PROJECT"]
    # Empty the load queue subscription
    topic = os.environ["KCIDB_LOAD_QUEUE_TOPIC"]
    subscription = os.environ["KCIDB_LOAD_QUEUE_SUBSCRIPTION"]
    for _ in kcidb.mq.IOSubscriber(project, topic, subscription). \
            pull_iter(timeout=30):
        pass
    # Empty the database
    kcidb.db.Client(os.environ["KCIDB_DATABASE"]).empty()
    # Wipe the spool
    kcidb.monitor.spool.Client(
        os.environ["KCIDB_SPOOL_COLLECTION_PATH"]
    ).wipe()
    # Empty the mock SMTP queue subscription
    topic = os.environ.get("KCIDB_SMTP_TOPIC")
    subscription = os.environ.get("KCIDB_SMTP_SUBSCRIPTION")
    if topic and subscription:
        for _ in kcidb.mq.EmailSubscriber(project, topic, subscription). \
                pull_iter(timeout=30):
            pass
    # Remove contents from the cache bucket
    bucket_name = os.environ.get("KCIDB_CACHE_BUCKET_NAME")
    client = kcidb.cache.Client(bucket_name, 0)
    client.empty()


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
