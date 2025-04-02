"""kcdib.db module tests"""

import re
import textwrap
import time
import datetime
import json
from copy import deepcopy
from itertools import permutations
import pytest
import dateutil.parser
import kcidb
from kcidb.unittest import assert_executes

# It's OK, pylint: disable=too-many-lines


def test_schemas_main():
    """Check kcidb-db-schemas works"""
    argv = ["kcidb.db.schemas_main", "-d", "sqlite::memory:"]
    assert_executes("", *argv,
                    stdout_re=r"4\.0: 4\.0\n"
                              r"4\.1: 4\.2\n"
                              r"4\.2: 4\.3\n"
                              r"4\.3: 4\.4\n"
                              r"4\.4: 4\.5\n"
                              r"5\.0: 5\.0\n"
                              r"5\.1: 5\.1\n"
                              r"5\.2: 5\.2\n"
                              r"5\.3: 5\.3\n")


def test_reset(clean_database):
    """Check Client.reset() works"""
    assert not clean_database.is_initialized()
    client = kcidb.db.Client(clean_database.database)
    assert not client.is_initialized()
    client.init()
    assert client.is_initialized()
    assert not clean_database.is_initialized()
    clean_database.reset()
    assert clean_database.is_initialized()


def test_init_main():
    """Check kcidb-db-init works"""
    argv = ["kcidb.db.init_main", "-d", "bigquery:project.dataset"]
    driver_source = textwrap.dedent("""
        from unittest.mock import patch, Mock
        client = Mock()
        client.init = Mock()
        client.is_initialized = Mock(return_value=False)
        with patch("kcidb.db.Client", return_value=client) as \
                Client:
            status = function()
        Client.assert_called_once_with("bigquery:project.dataset")
        client.init.assert_called_once()
        return status
    """)
    assert_executes("", *argv, driver_source=driver_source)


def test_cleanup_main():
    """Check kcidb-db-cleanup works"""
    argv = ["kcidb.db.cleanup_main", "-d", "bigquery:project.dataset"]
    driver_source = textwrap.dedent("""
        from unittest.mock import patch, Mock
        client = Mock()
        client.cleanup = Mock()
        client.is_initialized = Mock(return_value=True)
        with patch("kcidb.db.Client", return_value=client) as \
                Client:
            status = function()
        Client.assert_called_once_with("bigquery:project.dataset")
        client.cleanup.assert_called_once()
        return status
    """)
    assert_executes("", *argv, driver_source=driver_source)


def test_empty_main():
    """Check kcidb-db-empty works"""
    argv = ["kcidb.db.empty_main", "-d", "bigquery:project.dataset"]
    driver_source = textwrap.dedent("""
        from unittest.mock import patch, Mock
        client = Mock()
        client.empty = Mock()
        client.is_initialized = Mock(return_value=True)
        with patch("kcidb.db.Client", return_value=client) as \
                Client:
            status = function()
        Client.assert_called_once_with("bigquery:project.dataset")
        client.empty.assert_called_once()
        return status
    """)
    assert_executes("", *argv, driver_source=driver_source)


def test_dump_main():
    """Check kcidb-db-dump works"""
    empty = kcidb.io.SCHEMA.new()
    argv = ["kcidb.db.dump_main", "-d", "bigquery:project.dataset",
            "--indent=0"]

    driver_source = textwrap.dedent(f"""
        from unittest.mock import patch, Mock
        client = Mock()
        client.dump_iter = Mock(return_value=iter(({repr(empty)},)))
        with patch("kcidb.db.Client", return_value=client) as \
                Client:
            status = function()
        Client.assert_called_once_with("bigquery:project.dataset")
        client.dump_iter.assert_called_once()
        return status
    """)
    assert_executes("", *argv, driver_source=driver_source,
                    stdout_re=re.escape(json.dumps(empty) + "\n"))

    driver_source = textwrap.dedent(f"""
        from unittest.mock import patch, Mock
        client = Mock()
        client.dump_iter = Mock(return_value=iter({repr((empty, empty))}))
        with patch("kcidb.db.Client", return_value=client) as \
                Client:
            status = function()
        Client.assert_called_once_with("bigquery:project.dataset")
        client.dump_iter.assert_called_once()
        return status
    """)
    assert_executes("", *argv, driver_source=driver_source,
                    stdout_re=re.escape(json.dumps(empty) + "\n" +
                                        json.dumps(empty) + "\n"))


def test_query_main():
    """Check kcidb-db-query works"""
    driver_source = textwrap.dedent("""
        from unittest.mock import patch
        with patch("kcidb.db.Client"):
            return function()
    """)
    argv = ["kcidb.db.query_main", "-d", "bigquery:project.dataset"]
    assert_executes("", *argv, driver_source=driver_source)

    argv = [
        "kcidb.db.query_main", "-d", "bigquery:project.dataset",
        "-c", "test:checkout:1", "-b", "test:build:1",
        "-t", "test:test:1",
        "--parents", "--children", "--objects-per-report", "10",
        "--indent=0",
        "-i", "test:issue:1", "--iv", "10",
        "-n", "test:incident:1",
    ]
    empty = kcidb.io.SCHEMA.new()
    driver_source = textwrap.dedent(f"""
        from unittest.mock import patch, Mock
        client = Mock()
        client.query_iter = Mock(return_value=iter((
            {repr(empty)}, {repr(empty)},
        )))
        with patch("kcidb.db.Client", return_value=client) as Client:
            status = function()
        Client.assert_called_once_with("bigquery:project.dataset")
        client.query_iter.assert_called_once_with(
            ids=dict(checkouts=["test:checkout:1"],
                     builds=["test:build:1"],
                     tests=["test:test:1"],
                     issues=[("test:issue:1", 10)],
                     incidents=["test:incident:1"]),
            parents=True,
            children=True,
            objects_per_report=10,
            with_metadata=False
        )
        return status
    """)
    assert_executes(
        json.dumps(empty), *argv,
        driver_source=driver_source,
        stdout_re=re.escape(
            json.dumps(empty) + "\n" +
            json.dumps(empty) + "\n"
        )
    )


def test_load_main():
    """Check kcidb-db-load works"""
    driver_source = textwrap.dedent("""
        from unittest.mock import patch, Mock
        from kcidb.io import SCHEMA
        client = Mock()
        client.get_schema = Mock(return_value=((1, 0), SCHEMA))
        with patch("kcidb.db.Client", return_value=client):
            return function()
    """)
    argv = ["kcidb.db.load_main", "-d", "bigquery:project.dataset"]

    assert_executes("", *argv, driver_source=driver_source)
    assert_executes('{', *argv, driver_source=driver_source,
                    status=1, stderr_re=".*JSONParseError.*")
    assert_executes('{}', *argv, driver_source=driver_source,
                    status=1, stderr_re=".*ValidationError.*")

    empty = kcidb.io.SCHEMA.new()

    driver_source = textwrap.dedent(f"""
        from unittest.mock import patch, Mock
        from kcidb.io import SCHEMA
        client = Mock()
        client.get_schema = Mock(return_value=((1, 0), SCHEMA))
        client.load = Mock()
        with patch("kcidb.db.Client", return_value=client) as Client:
            status = function()
        Client.assert_called_once_with("bigquery:project.dataset")
        client.load.assert_called_once_with({repr(empty)},
                                            with_metadata=False,
                                            copy=False)
        return status
    """)
    assert_executes(json.dumps(empty), *argv,
                    driver_source=driver_source)

    driver_source = textwrap.dedent(f"""
        from unittest.mock import patch, Mock, call
        from kcidb.io import SCHEMA
        client = Mock()
        client.get_schema = Mock(return_value=((1, 0), SCHEMA))
        client.load = Mock()
        with patch("kcidb.db.Client", return_value=client) as Client:
            status = function()
        Client.assert_called_once_with("bigquery:project.dataset")
        assert client.load.call_count == 2
        client.load.assert_has_calls([
            call({repr(empty)}, with_metadata=False, copy=False),
            call({repr(empty)}, with_metadata=False, copy=False)
        ])
        return status
    """)
    assert_executes(json.dumps(empty) + json.dumps(empty), *argv,
                    driver_source=driver_source)


# I/O data containing all possible fields
COMPREHENSIVE_IO_DATA = {
    **kcidb.io.SCHEMA.new(),
    "checkouts": [
        dict(
            id="origin:1",
            origin="origin",
            tree_name="mainline",
            git_repository_url="https://git.kernel.org/pub/scm/"
                               "linux/kernel/git/torvalds/linux.git",
            git_commit_hash="ef2b753c38bc9c0d1eea84e29a6bb6f9e776d0e3",
            git_commit_tags=["v5.8"],
            git_commit_message="Linux 5.8-rc7",
            git_commit_name="v5.8-rc7-98-g7dc6fd0f3b84",
            git_repository_branch="master",
            git_repository_branch_tip=False,
            patchset_files=[
                dict(name="0001.patch",
                     url="https://example.com/0001.patch"),
                dict(name="0002.patch",
                     url="https://example.com/0002.patch"),
            ],
            patchset_hash="903638c087335b10293663c682b9aa00"
                          "76f9f7be478a8e7828bc22e12d301b42",
            message_id="5f9fccd2.1c69fb81.1b2f.6d9c@mx.google.com",
            comment="v5.8",
            start_time="2020-08-14T23:08:06.967000+00:00",
            log_url="https://example.com/checkout.log",
            log_excerpt="error: patch failed: "
                        "arch/arm64/boot/dts/qcom/sc7180.dtsi:510\n"
                        "error: arch/arm64/boot/dts/qcom/sc7180.dtsi:"
                        " patch does not apply\n",
            valid=True,
            misc=dict(
                foo="bar",
                baz=42
            ),
        ),
    ],
    "builds": [
        dict(
            checkout_id="origin:1",
            id="origin:1",
            origin="origin",
            comment="x86_64",
            start_time="2020-08-14T23:08:06.967000+00:00",
            duration=600.0,
            architecture="x86_64",
            command="make",
            compiler="gcc",
            input_files=[
                dict(name="foo.bar",
                     url="https://example.com/foo.bar"),
                dict(name="bar.foo",
                     url="https://example.com/bar.foo"),
            ],
            output_files=[
                dict(name="foo.bar",
                     url="https://example.com/foo.bar"),
                dict(name="bar.foo",
                     url="https://example.com/bar.foo"),
            ],
            config_name="fedora",
            config_url="https://example.com/fedora.config",
            log_url="https://example.com/build.log",
            log_excerpt="error: invalid input constraint 'rZ'\n",
            status="PASS",
            misc=dict(
                foo="bar",
                baz=42
            ),
        ),
    ],
    "tests": [
        dict(
            build_id="origin:1",
            id="origin:1",
            origin="origin",
            environment=dict(
                comment="Lenovo x230",
                misc=dict(
                    foo="bar",
                    baz=42
                ),
                compatible=[
                    "ti,omap3-beagleboard",
                    "ti,omap3450",
                    "ti,omap3"
                ],
            ),
            path="ltp",
            comment="ltp on Lenovo x230",
            log_url="https://example.com/test.log",
            log_excerpt="kernel BUG at net/core/dev.c:2648!\n",
            status="FAIL",
            number=dict(
                value=1.6e-7,
                unit="s",
                prefix="metric",
            ),
            start_time="2020-08-14T23:08:07.967000+00:00",
            duration=600.0,
            output_files=[
                dict(name="foo.bar",
                     url="https://example.com/foo.bar"),
                dict(name="bar.foo",
                     url="https://example.com/bar.foo"),
            ],
            misc=dict(
                foo="bar",
                baz=42
            ),
        ),
    ],
    "issues": [
        dict(
            id="origin:1",
            version=1,
            origin="origin",
            report_url="https://bugzilla/1298712",
            report_subject="Printing doesn't work",
            culprit=dict(
                code=False,
                tool=True,
                harness=False,
            ),
            comment="Match printing failures",
            misc=dict(
                foo="bar",
                baz=42
            ),
        )
    ],
    "incidents": [
        dict(
            id="origin:1",
            origin="origin",
            issue_id="origin:1",
            issue_version=1,
            build_id="origin:1",
            test_id="origin:1",
            present=True,
            comment="It crashed",
            misc=dict(
                foo="bar",
                baz=42
            ),
        )
    ]
}


def test_get_current_time(clean_database):
    """
    Check get_current_time() works correctly
    """
    client = clean_database
    timestamp = client.get_current_time()
    assert timestamp is not None
    assert isinstance(timestamp, datetime.datetime)
    assert timestamp.tzinfo is not None
    time.sleep(1)
    assert client.get_current_time() > timestamp


def test_get_modified(clean_database):
    """
    Check get_first_modified() and get_last_modified() work correctly
    """
    client = clean_database
    # Check a pre-timestamp schema version
    client.init(kcidb.io.schema.V4_2)
    with pytest.raises(kcidb.db.misc.NoTimestamps):
        client.get_first_modified()
    with pytest.raises(kcidb.db.misc.NoTimestamps):
        client.get_last_modified()
    client.load({
        **kcidb.io.schema.V4_2.new(),
        "checkouts": [
            dict(id="origin:1", origin="origin",),
        ],
        "builds": [
            dict(checkout_id="origin:1", id="origin:1", origin="origin",),
        ],
        "tests": [
            dict(build_id="origin:1", id="origin:1", origin="origin",),
        ],
        "issues": [
            dict(id="origin:1", version=1, origin="origin",),
        ],
        "incidents": [
            dict(
                id="origin:1",
                origin="origin",
                issue_id="origin:1",
                issue_version=1,
            )
        ]
    })
    with pytest.raises(kcidb.db.misc.NoTimestamps):
        client.get_first_modified()
    with pytest.raises(kcidb.db.misc.NoTimestamps):
        client.get_last_modified()
    client.cleanup()

    # Check a post-timestamp schema version
    time.sleep(1)
    client.init()
    io_schema = client.get_schema()[1]
    timestamps = client.get_first_modified()
    assert timestamps == {}
    timestamps = client.get_last_modified()
    assert timestamps == {}
    before_load = client.get_current_time()
    client.load(COMPREHENSIVE_IO_DATA)

    first_modified = client.get_first_modified()
    last_modified = client.get_last_modified()

    assert isinstance(first_modified, dict)
    assert set(io_schema.id_fields) == set(first_modified)
    assert all(
        isinstance(t, datetime.datetime) and
        t.tzinfo is not None and
        t >= before_load
        for t in first_modified.values()
    )

    assert isinstance(last_modified, dict)
    assert set(io_schema.id_fields) == set(last_modified)
    assert all(
        isinstance(t, datetime.datetime) and
        t.tzinfo is not None and
        t >= before_load
        for t in first_modified.values()
    )

    assert all(t >= first_modified[n] for n, t in last_modified.items())

    client.cleanup()


def test_all_fields(empty_database):
    """
    Check all possible I/O fields can be loaded into and dumped from
    a database.
    """
    io_data = COMPREHENSIVE_IO_DATA
    client = empty_database
    client.load(io_data)
    assert io_data == client.dump(with_metadata=False)


def test_metadata_introduction(clean_database):
    """
    Check metadata generation works right on database upgrade.
    """
    # It's OK, pylint: disable=too-many-branches
    client = clean_database

    # Find DB/IO schemas on both sides of the metadata introduction
    pre_metadata_schema = None
    post_metadata_schema = None
    for schema in client.get_schemas().items():
        print("schema:", schema)
        if schema[1] >= kcidb.io.schema.V4_3:
            post_metadata_schema = schema
            break
        # Find oldest DB schema version with previous I/O schema version
        # (the oldest is needed for handling partial mux upgrades)
        # Oh, but it is, pylint: disable=unsubscriptable-object
        if not pre_metadata_schema or schema[1] != pre_metadata_schema[1]:
            pre_metadata_schema = schema
            print("pre_metadata_schema:", pre_metadata_schema)
    assert pre_metadata_schema
    assert pre_metadata_schema[1] >= kcidb.io.schema.V4_1
    assert post_metadata_schema
    print("post_metadata_schema:", post_metadata_schema)

    # Initialize the database with pre-metadata schema
    client.init(pre_metadata_schema[0])
    # Load data both with and without start_time fields
    pre_metadata_io = dict(
        version=dict(major=4, minor=1),
        checkouts=[
            dict(
                id="origin:1",
                origin="origin",
                start_time="2020-08-14T23:08:06.967000+00:00",
            ),
            dict(
                id="origin:2",
                origin="origin",
            ),
        ],
        builds=[
            dict(
                checkout_id="origin:1",
                id="origin:1",
                origin="origin",
                start_time="2020-08-14T23:08:06.967000+00:00",
            ),
            dict(
                checkout_id="origin:2",
                id="origin:2",
                origin="origin",
            ),
        ],
        tests=[
            dict(
                build_id="origin:1",
                id="origin:1",
                origin="origin",
                start_time="2020-08-14T23:08:07.967000+00:00",
            ),
            dict(
                build_id="origin:2",
                id="origin:2",
                origin="origin",
            ),
        ],
        issues=[
            dict(
                id="origin:1",
                version=1,
                origin="origin",
            ),
        ],
        incidents=[
            dict(
                id="origin:1",
                origin="origin",
                issue_id="origin:1",
                issue_version=1,
            ),
        ]
    )
    client.load(pre_metadata_schema[1].upgrade(pre_metadata_io))
    after_load = client.get_current_time()
    # Upgrade to post-metadata schema
    client.upgrade(post_metadata_schema[0])
    after_upgrade = client.get_current_time()
    # Get the upgraded data with metadata
    post_metadata_io = client.dump(with_metadata=True)

    # Sort I/O objects by IDs, so we could compare them
    for obj_list_name in pre_metadata_schema[1].graph:
        if obj_list_name:
            pre_metadata_io[obj_list_name].sort(key=lambda x: x["id"])
            post_metadata_io[obj_list_name].sort(key=lambda x: x["id"])

    # Check _timestamp is properly inherited/generated
    for obj_list_name in pre_metadata_schema[1].graph:
        if obj_list_name:
            for pre_obj, post_obj in zip(pre_metadata_io[obj_list_name],
                                         post_metadata_io[obj_list_name]):
                assert pre_obj["id"] == post_obj["id"]
                assert "_timestamp" in post_obj, \
                    f"_timestamp is missing in " \
                    f"{obj_list_name[:-1]} ID {post_obj['id']}"
                if "start_time" in pre_obj:
                    assert post_obj["start_time"] == pre_obj["start_time"], \
                        f"start_time unexpectedly modified in " \
                        f"{obj_list_name[:-1]} ID {post_obj['id']}"
                    assert post_obj["_timestamp"] == pre_obj["start_time"], \
                        f"_timestamp doesn't match start_time in " \
                        f"{obj_list_name[:-1]} ID {post_obj['id']}"
                else:
                    timestamp = dateutil.parser.isoparse(
                        post_obj["_timestamp"]
                    )
                    assert after_load <= timestamp <= after_upgrade, \
                        f"Generated _timestamp out range in " \
                        f"{obj_list_name[:-1]} ID {post_obj['id']}"

    # Check the upgraded schema can generate _timestamp correctly
    client.empty()
    before_load = client.get_current_time()
    client.load(post_metadata_schema[1].upgrade(pre_metadata_io))
    after_load = client.get_current_time()
    post_metadata_io = client.dump(with_metadata=True)
    for obj_list_name in pre_metadata_schema[1].graph:
        if obj_list_name:
            for post_obj in post_metadata_io[obj_list_name]:
                assert "_timestamp" in post_obj, \
                    f"_timestamp is missing in " \
                    f"{obj_list_name[:-1]} ID {post_obj['id']}"
                timestamp = dateutil.parser.isoparse(
                    post_obj["_timestamp"]
                )
                assert before_load <= timestamp <= after_load, \
                    f"Generated _timestamp out range in " \
                    f"{obj_list_name[:-1]} ID {post_obj['id']}"


def test_metadata_generation_and_fetching(empty_database):
    """
    Check metadata generation and fetching works right.
    """
    client = empty_database
    io_data = COMPREHENSIVE_IO_DATA
    ids = kcidb.io.SCHEMA.get_ids(io_data)

    # Check metadata is generated
    before_load = client.get_current_time()
    client.load(io_data)
    after_load = client.get_current_time()
    dump_with_metadata = client.dump()
    dump_without_metadata = kcidb.io.SCHEMA.new()
    for obj_list_name in kcidb.io.SCHEMA.graph:
        if obj_list_name:
            dump_without_metadata[obj_list_name] = []
            for obj in dump_with_metadata[obj_list_name]:
                assert "_timestamp" in obj
                obj = obj.copy()
                timestamp = dateutil.parser.isoparse(obj.pop("_timestamp"))
                assert timestamp >= before_load
                assert timestamp <= after_load
                dump_without_metadata[obj_list_name].append(obj)
    assert dump_without_metadata == io_data
    assert client.dump(with_metadata=False) == dump_without_metadata

    # Check queries can be done both with and without metadata
    query_with_metadata = client.query(ids=ids, with_metadata=True)
    query_without_metadata = kcidb.io.SCHEMA.new()
    for obj_list_name in kcidb.io.SCHEMA.graph:
        if obj_list_name:
            query_without_metadata[obj_list_name] = []
            for obj in query_with_metadata[obj_list_name]:
                assert "_timestamp" in obj
                obj = obj.copy()
                timestamp = dateutil.parser.isoparse(obj.pop("_timestamp"))
                assert timestamp >= before_load
                assert timestamp <= after_load
                query_without_metadata[obj_list_name].append(obj)
    assert query_without_metadata == io_data
    assert client.query(ids=ids, with_metadata=False) == \
        query_without_metadata


def test_metadata_ignoring_and_updating(empty_database):
    """
    Check metadata ignoring and updating works right.
    """
    client = empty_database

    # Generate a dump with metadata
    client.load(COMPREHENSIVE_IO_DATA)
    dump_with_metadata = client.dump()
    dump_without_metadata = client.dump(with_metadata=False)

    # Check loaded metadata is ignored by default and new one is generated
    before_later_load = client.get_current_time()
    client.load(dump_with_metadata)
    after_later_load = client.get_current_time()
    later_dump_with_metadata = client.dump()
    assert later_dump_with_metadata != dump_with_metadata
    for obj_list_name in kcidb.io.SCHEMA.graph:
        if obj_list_name:
            for obj in later_dump_with_metadata[obj_list_name]:
                assert "_timestamp" in obj
                timestamp = dateutil.parser.isoparse(obj["_timestamp"])
                assert timestamp >= before_later_load
                assert timestamp <= after_later_load

    # Empty the database
    empty_database.empty()

    # Check metadata can be loaded when requested
    client.load(dump_with_metadata, with_metadata=True)
    assert client.dump() == dump_with_metadata

    # Check metadata can be overwritten
    client.load(later_dump_with_metadata, with_metadata=True)
    assert client.dump() == later_dump_with_metadata

    # Check loading missing metadata has no effect
    client.load(dump_without_metadata, with_metadata=True)
    assert client.dump() == later_dump_with_metadata


def test_upgrade(clean_database):
    """
    Test database schema upgrade affects accepted I/O schema, and doesn't
    affect ORM results.
    """
    io_version_params = {
        kcidb.io.schema.V3_0: dict(
            io={
                "version": {
                    "major": 3,
                    "minor": 0
                },
                "revisions": [{
                    "id": "f00af9d68ed146b47fdbfe91134fcf04c36e6d78",
                    "origin": "tuxsuite",
                    "git_repository_url":
                        "https://android.googlesource.com/kernel/common.git",
                    "git_commit_hash":
                        "f00af9d68ed146b47fdbfe91134fcf04c36e6d78",
                    "git_commit_name":
                        "ASB-2023-01-05_mainline-9680-gf00af9d68ed1",
                    "discovery_time": "2023-01-27T08:27:50.000000+00:00",
                    "valid": True,
                    "git_repository_branch": "android-mainline"
                }],
                "builds": [{
                    "id": "tuxsuite:2KtyFbORDouvFKy49kQtfgCmcac",
                    "revision_id": "f00af9d68ed146b47fdbfe91134fcf04c36e6d78",
                    "origin": "tuxsuite",
                    "architecture": "x86_64",
                    "compiler": "Debian clang version 15.0.7",
                    "config_name": "gki_defconfig",
                    "config_url":
                        "https://storage.tuxsuite.com/public/"
                        "clangbuiltlinux/continuous-integration2/builds/"
                        "2KtyFbORDouvFKy49kQtfgCmcac/config",
                    "log_url":
                        "https://storage.tuxsuite.com/public/"
                        "clangbuiltlinux/continuous-integration2/builds/"
                        "2KtyFbORDouvFKy49kQtfgCmcac/build.log",
                    "start_time": "2023-01-27T08:27:50.000000+00:00",
                    "valid": True
                }],
                "tests": [{
                    "build_id":
                        "syzbot:e716fd2a536671b69625b2536ebe9ede623b93b4",
                    "description": "INFO: task hung in ipv6_route_ioctl (2)",
                    "id": "syzbot:bf7c6406637722a401e0",
                    "misc": {
                        "origin_url":
                            "https://syzkaller.appspot.com/bug?"
                            "extid=bf7c6406637722a401e0",
                        "reported_by":
                            "syzbot+bf7c6406637722a401e0@"
                            "syzkaller.appspotmail.com"
                    },
                    "origin": "syzbot",
                    "output_files": [
                        {
                            "name": "report.txt",
                            "url":
                                "https://syzkaller.appspot.com/x/report.txt?"
                                "x=1262b549480000"
                        },
                        {
                            "name": "log.txt",
                            "url":
                                "https://syzkaller.appspot.com/x/log.txt?"
                                "x=10ab93cd480000"
                        },
                        {
                            "name": "machine_info.txt",
                            "url": "https://syzkaller.appspot.com/x/"
                                "minfo.txt?x=75affc83eb386f34"
                        }
                    ],
                    "path": "syzkaller",
                    "start_time": "2023-01-28T02:21:00.000000+00:00",
                    "status": "FAIL",
                }],
            },
            oo={
                'revision': [{
                    'git_commit_hash':
                        'f00af9d68ed146b47fdbfe91134fcf04c36e6d78',
                    'git_commit_name':
                        'ASB-2023-01-05_mainline-9680-gf00af9d68ed1',
                    'patchset_files': None,
                    'patchset_hash': ''
                }],
                'checkout': [{
                    'comment': None,
                    'git_commit_hash':
                        'f00af9d68ed146b47fdbfe91134fcf04c36e6d78',
                    'git_commit_tags': None,
                    'git_commit_message': None,
                    'git_repository_branch': 'android-mainline',
                    'git_repository_branch_tip': None,
                    'git_repository_url':
                        'https://android.googlesource.com/kernel/common.git',
                    'id':
                        '_:tuxsuite:f00af9d68ed146b47fdbfe91134fcf04c36e6d78',
                    'log_url': None,
                    'message_id': None,
                    'misc': None,
                    'origin': 'tuxsuite',
                    'patchset_hash': '',
                    'start_time': '2023-01-27T08:27:50.000000+00:00',
                    'tree_name': None,
                    'valid': True
                }],
                'build': [{
                    'architecture': 'x86_64',
                    'checkout_id':
                        '_:tuxsuite:f00af9d68ed146b47fdbfe91134fcf04c36e6d78',
                    'command': None,
                    'comment': None,
                    'compiler': 'Debian clang version 15.0.7',
                    'config_name': 'gki_defconfig',
                    'config_url': 'https://storage.tuxsuite.com/public/'
                        'clangbuiltlinux/continuous-integration2/builds/'
                        '2KtyFbORDouvFKy49kQtfgCmcac/config',
                    'duration': None,
                    'id': 'tuxsuite:2KtyFbORDouvFKy49kQtfgCmcac',
                    'input_files': None,
                    'log_url': 'https://storage.tuxsuite.com/public/'
                        'clangbuiltlinux/continuous-integration2/builds/'
                        '2KtyFbORDouvFKy49kQtfgCmcac/build.log',
                    'misc': None,
                    'origin': 'tuxsuite',
                    'output_files': None,
                    'start_time': '2023-01-27T08:27:50.000000+00:00',
                    'status': 'PASS',
                }],
                'test': [{
                    'build_id':
                        'syzbot:e716fd2a536671b69625b2536ebe9ede623b93b4',
                    'comment': 'INFO: task hung in ipv6_route_ioctl (2)',
                    'duration': None,
                    'environment_comment': None,
                    'environment_compatible': None,
                    'environment_misc': None,
                    'id': 'syzbot:bf7c6406637722a401e0',
                    'log_url': None,
                    'misc': {
                        'origin_url':
                            'https://syzkaller.appspot.com/bug?'
                            'extid=bf7c6406637722a401e0',
                        'reported_by':
                            'syzbot+bf7c6406637722a401e0@'
                            'syzkaller.appspotmail.com'
                    },
                    'origin': 'syzbot',
                    'output_files': [
                        {
                            'name': 'report.txt',
                            'url':
                                'https://syzkaller.appspot.com/x/'
                                'report.txt?x=1262b549480000'
                        },
                        {
                            'name': 'log.txt',
                            'url':
                                'https://syzkaller.appspot.com/x/'
                                'log.txt?x=10ab93cd480000'
                        },
                        {
                            'name': 'machine_info.txt',
                            'url':
                                'https://syzkaller.appspot.com/x/'
                                'minfo.txt?x=75affc83eb386f34'
                        }
                    ],
                    'path': 'syzkaller',
                    'start_time': '2023-01-28T02:21:00.000000+00:00',
                    'status': 'FAIL',
                    'number_value': None,
                    'number_unit': None,
                    'number_prefix': None,
                }],
                'issue': [],
                'issue_version': [],
                'incident': [],
            }
        ),
        kcidb.io.schema.V4_0: dict(
            io={
                "version": {"major": 4, "minor": 0},
                "checkouts": [{
                    "id": "_:kernelci:5acb9c2a7bc836e"
                          "9e5172bbcd2311499c5b4e5f1",
                    "origin": "kernelci",
                    "git_commit_hash": "5acb9c2a7bc836e9e5172bb"
                                       "cd2311499c5b4e5f1",
                    "git_commit_name": "v5.15-4077-g5acb9c2a7bc8",
                    "patchset_hash": ""
                }],
                "builds": [{
                    "id": "google:google.org:a1d993c3n4c448b2j0l1hbf1",
                    "origin": "google",
                    "checkout_id": "_:google:bd355732283c23a365f7c"
                                   "55206c0385100d1c389"
                }],
                "tests": [{
                    "id": "google:google.org:a19di3j5h67f8d9475f26v11",
                    "build_id": "google:google.org:a1d993c3n4c448b2"
                                "j0l1hbf1",
                    "origin": "google",
                }],
            },
            oo={
                "revision": [{
                    "git_commit_hash":
                        "5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
                    "git_commit_name":
                        "v5.15-4077-g5acb9c2a7bc8",
                    "patchset_files": None,
                    "patchset_hash": "",
                }],
                "checkout": [{
                    "comment": None,
                    "git_commit_hash":
                        "5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
                    "git_commit_tags": None,
                    "git_commit_message": None,
                    "git_repository_branch": None,
                    "git_repository_branch_tip": None,
                    "git_repository_url": None,
                    "id":
                        "_:kernelci:"
                        "5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
                    "log_url": None,
                    "message_id": None,
                    "misc": None,
                    "origin": "kernelci",
                    "patchset_hash": "",
                    "start_time": None,
                    "tree_name": None,
                    "valid": None,
                }],
                "build": [{
                    "architecture": None,
                    "checkout_id":
                        "_:google:"
                        "bd355732283c23a365f7c55206c0385100d1c389",
                    "command": None,
                    "comment": None,
                    "compiler": None,
                    "config_name": None,
                    "config_url": None,
                    "duration": None,
                    "id": "google:google.org:a1d993c3n4c448b2j0l1hbf1",
                    "input_files": None,
                    "log_url": None,
                    "misc": None,
                    "origin": "google",
                    "output_files": None,
                    "start_time": None,
                    "status": None,
                }],
                "test": [{
                    "build_id":
                        "google:google.org:a1d993c3n4c448b2j0l1hbf1",
                    "comment": None,
                    "duration": None,
                    "environment_comment": None,
                    "environment_compatible": None,
                    "environment_misc": None,
                    "id":
                        "google:google.org:a19di3j5h67f8d9475f26v11",
                    "log_url": None,
                    "misc": None,
                    "origin": "google",
                    "output_files": None,
                    "path": None,
                    "start_time": None,
                    "status": None,
                    'number_value': None,
                    'number_unit': None,
                    'number_prefix': None,
                }],
                "issue": [],
                "issue_version": [],
                "incident": [],
            }
        ),
        kcidb.io.schema.V4_2: dict(
            io={
                "version": {"major": 4, "minor": 2},
                "checkouts": [{
                    "id": "_:kernelci:5acb9c2a7bc836e"
                          "9e5172bbcd2311499c5b4e5f1",
                    "origin": "kernelci",
                    "git_commit_hash": "5acb9c2a7bc836e9e5172bb"
                                       "cd2311499c5b4e5f1",
                    "git_commit_name": "v5.15-4077-g5acb9c2a7bc8",
                    "patchset_hash": ""
                }],
                "builds": [{
                    "id": "google:google.org:a1d993c3n4c448b2j0l1hbf1",
                    "origin": "google",
                    "checkout_id": "_:google:bd355732283c23a365f7c"
                                   "55206c0385100d1c389"
                }],
                "tests": [{
                    "id": "google:google.org:a19di3j5h67f8d9475f26v11",
                    "build_id": "google:google.org:a1d993c3n4c448b2"
                                "j0l1hbf1",
                    "origin": "google",
                    "status": "MISS",
                }],
                "issues": [{
                    "id": "redhat:878234322",
                    "version": 3,
                    "origin": "redhat",
                    "report_url":
                        "https://bugzilla.redhat.com/show_bug.cgi"
                        "?id=873123",
                    "report_subject":
                        "(cups-usb-quirks) - usb printer doesn't print "
                        "(usblp0: USB Bidirectional printer dev)",
                    "culprit": {
                        "code": True,
                        "tool": False,
                        "harness": False,
                    },
                    "comment": "Match USB Bidirectional printer dev",
                }],
                "incidents": [{
                    "id": "redhat:2340981234098123409382",
                    "issue_id": "redhat:878234322",
                    "issue_version": 3,
                    "origin": "redhat",
                    "test_id":
                        "google:google.org:a19di3j5h67f8d9475f26v11",
                    "present": True,
                }],
            },
            oo={
                "revision": [{
                    "git_commit_hash":
                        "5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
                    "git_commit_name":
                        "v5.15-4077-g5acb9c2a7bc8",
                    "patchset_files": None,
                    "patchset_hash": "",
                }],
                "checkout": [{
                    "comment": None,
                    "git_commit_hash":
                        "5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
                    "git_commit_tags": None,
                    "git_commit_message": None,
                    "git_repository_branch": None,
                    "git_repository_branch_tip": None,
                    "git_repository_url": None,
                    "id":
                        "_:kernelci:"
                        "5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
                    "log_url": None,
                    "message_id": None,
                    "misc": None,
                    "origin": "kernelci",
                    "patchset_hash": "",
                    "start_time": None,
                    "tree_name": None,
                    "valid": None,
                }],
                "build": [{
                    "architecture": None,
                    "checkout_id":
                        "_:google:"
                        "bd355732283c23a365f7c55206c0385100d1c389",
                    "command": None,
                    "comment": None,
                    "compiler": None,
                    "config_name": None,
                    "config_url": None,
                    "duration": None,
                    "id": "google:google.org:a1d993c3n4c448b2j0l1hbf1",
                    "input_files": None,
                    "log_url": None,
                    "misc": None,
                    "origin": "google",
                    "output_files": None,
                    "start_time": None,
                    "status": None,
                }],
                "test": [{
                    "build_id":
                        "google:google.org:a1d993c3n4c448b2j0l1hbf1",
                    "comment": None,
                    "duration": None,
                    "environment_comment": None,
                    "environment_compatible": None,
                    "environment_misc": None,
                    "id":
                        "google:google.org:a19di3j5h67f8d9475f26v11",
                    "log_url": None,
                    "misc": None,
                    "origin": "google",
                    "output_files": None,
                    "path": None,
                    "start_time": None,
                    "status": "MISS",
                    'number_value': None,
                    'number_unit': None,
                    'number_prefix': None,
                }],
                "issue": [{
                    "id": "redhat:878234322",
                    "origin": "redhat",
                }],
                "issue_version": [{
                    "comment": "Match USB Bidirectional printer dev",
                    "id": "redhat:878234322",
                    "misc": None,
                    "origin": "redhat",
                    "report_url":
                        "https://bugzilla.redhat.com/show_bug.cgi?"
                        "id=873123",
                    "report_subject":
                        "(cups-usb-quirks) - usb printer doesn't print "
                        "(usblp0: USB Bidirectional printer dev)",
                    "culprit_code": True,
                    "culprit_tool": False,
                    "culprit_harness": False,
                    "version_num": 3,
                }],
                "incident": [{
                    "build_id": None,
                    "comment": None,
                    "id": "redhat:2340981234098123409382",
                    "issue_id": "redhat:878234322",
                    "issue_version_num": 3,
                    "misc": None,
                    "origin": "redhat",
                    "present": True,
                    "test_id":
                        "google:google.org:a19di3j5h67f8d9475f26v11",
                }],
            }
        ),
        kcidb.io.schema.V4_3: dict(
            io={
                "version": {"major": 4, "minor": 3},
                "checkouts": [{
                    "id": "_:kernelci:5acb9c2a7bc836e"
                          "9e5172bbcd2311499c5b4e5f1",
                    "origin": "kernelci",
                    "git_commit_hash": "5acb9c2a7bc836e9e5172bb"
                                       "cd2311499c5b4e5f1",
                    "git_commit_name": "v5.15-4077-g5acb9c2a7bc8",
                    "patchset_hash": ""
                }],
                "builds": [{
                    "id": "google:google.org:a1d993c3n4c448b2j0l1hbf1",
                    "origin": "google",
                    "checkout_id": "_:google:bd355732283c23a365f7c"
                                   "55206c0385100d1c389"
                }],
                "tests": [{
                    "id": "google:google.org:a19di3j5h67f8d9475f26v11",
                    "build_id": "google:google.org:a1d993c3n4c448b2"
                                "j0l1hbf1",
                    "origin": "google",
                    "status": "MISS",
                }],
                "issues": [{
                    "id": "redhat:878234322",
                    "version": 3,
                    "origin": "redhat",
                    "report_url":
                        "https://bugzilla.redhat.com/show_bug.cgi"
                        "?id=873123",
                    "report_subject":
                        "(cups-usb-quirks) - usb printer doesn't print "
                        "(usblp0: USB Bidirectional printer dev)",
                    "culprit": {
                        "code": True,
                        "tool": False,
                        "harness": False,
                    },
                    "comment": "Match USB Bidirectional printer dev",
                }],
                "incidents": [{
                    "id": "redhat:2340981234098123409382",
                    "issue_id": "redhat:878234322",
                    "issue_version": 3,
                    "origin": "redhat",
                    "test_id":
                        "google:google.org:a19di3j5h67f8d9475f26v11",
                    "present": True,
                }],
            },
            oo={
                "revision": [{
                    "git_commit_hash":
                        "5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
                    "git_commit_name":
                        "v5.15-4077-g5acb9c2a7bc8",
                    "patchset_files": None,
                    "patchset_hash": "",
                }],
                "checkout": [{
                    "comment": None,
                    "git_commit_hash":
                        "5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
                    "git_commit_tags": None,
                    "git_commit_message": None,
                    "git_repository_branch": None,
                    "git_repository_branch_tip": None,
                    "git_repository_url": None,
                    "id":
                        "_:kernelci:"
                        "5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
                    "log_url": None,
                    "message_id": None,
                    "misc": None,
                    "origin": "kernelci",
                    "patchset_hash": "",
                    "start_time": None,
                    "tree_name": None,
                    "valid": None,
                }],
                "build": [{
                    "architecture": None,
                    "checkout_id":
                        "_:google:"
                        "bd355732283c23a365f7c55206c0385100d1c389",
                    "command": None,
                    "comment": None,
                    "compiler": None,
                    "config_name": None,
                    "config_url": None,
                    "duration": None,
                    "id": "google:google.org:a1d993c3n4c448b2j0l1hbf1",
                    "input_files": None,
                    "log_url": None,
                    "misc": None,
                    "origin": "google",
                    "output_files": None,
                    "start_time": None,
                    "status": None,
                }],
                "test": [{
                    "build_id":
                        "google:google.org:a1d993c3n4c448b2j0l1hbf1",
                    "comment": None,
                    "duration": None,
                    "environment_comment": None,
                    "environment_compatible": None,
                    "environment_misc": None,
                    "id":
                        "google:google.org:a19di3j5h67f8d9475f26v11",
                    "log_url": None,
                    "misc": None,
                    "origin": "google",
                    "output_files": None,
                    "path": None,
                    "start_time": None,
                    "status": "MISS",
                    'number_value': None,
                    'number_unit': None,
                    'number_prefix': None,
                }],
                "issue": [{
                    "id": "redhat:878234322",
                    "origin": "redhat",
                }],
                "issue_version": [{
                    "comment": "Match USB Bidirectional printer dev",
                    "id": "redhat:878234322",
                    "misc": None,
                    "origin": "redhat",
                    "report_url":
                        "https://bugzilla.redhat.com/show_bug.cgi?"
                        "id=873123",
                    "report_subject":
                        "(cups-usb-quirks) - usb printer doesn't print "
                        "(usblp0: USB Bidirectional printer dev)",
                    "culprit_code": True,
                    "culprit_tool": False,
                    "culprit_harness": False,
                    "version_num": 3,
                }],
                "incident": [{
                    "build_id": None,
                    "comment": None,
                    "id": "redhat:2340981234098123409382",
                    "issue_id": "redhat:878234322",
                    "issue_version_num": 3,
                    "misc": None,
                    "origin": "redhat",
                    "present": True,
                    "test_id":
                        "google:google.org:a19di3j5h67f8d9475f26v11",
                }],
            }
        ),
        kcidb.io.schema.V4_4: dict(
            io={
                "version": {"major": 4, "minor": 4},
                "checkouts": [{
                    "id": "_:kernelci:5acb9c2a7bc836e"
                          "9e5172bbcd2311499c5b4e5f1",
                    "origin": "kernelci",
                    "git_commit_hash": "5acb9c2a7bc836e9e5172bb"
                                       "cd2311499c5b4e5f1",
                    "git_commit_name": "v5.15-4077-g5acb9c2a7bc8",
                    "patchset_hash": ""
                }],
                "builds": [{
                    "id": "google:google.org:a1d993c3n4c448b2j0l1hbf1",
                    "origin": "google",
                    "checkout_id": "_:google:bd355732283c23a365f7c"
                                   "55206c0385100d1c389"
                }],
                "tests": [{
                    "id": "google:google.org:a19di3j5h67f8d9475f26v11",
                    "build_id": "google:google.org:a1d993c3n4c448b2"
                                "j0l1hbf1",
                    "origin": "google",
                    "status": "MISS",
                    "number": {
                        "value": 1.6e-7,
                        "unit": "s",
                        "prefix": "metric",
                    },
                    "environment": {
                        "comment": "Tidy",
                        "misc": {"foo": "bar"},
                        "compatible": [
                            "ti,omap3-beagleboard",
                            "ti,omap3450",
                            "ti,omap3"
                        ],
                    },
                }],
                "issues": [{
                    "id": "redhat:878234322",
                    "version": 3,
                    "origin": "redhat",
                    "report_url":
                        "https://bugzilla.redhat.com/show_bug.cgi"
                        "?id=873123",
                    "report_subject":
                        "(cups-usb-quirks) - usb printer doesn't print "
                        "(usblp0: USB Bidirectional printer dev)",
                    "culprit": {
                        "code": True,
                        "tool": False,
                        "harness": False,
                    },
                    "comment": "Match USB Bidirectional printer dev",
                }],
                "incidents": [{
                    "id": "redhat:2340981234098123409382",
                    "issue_id": "redhat:878234322",
                    "issue_version": 3,
                    "origin": "redhat",
                    "test_id":
                        "google:google.org:a19di3j5h67f8d9475f26v11",
                    "present": True,
                }],
            },
            oo={
                "revision": [{
                    "git_commit_hash":
                        "5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
                    "git_commit_name":
                        "v5.15-4077-g5acb9c2a7bc8",
                    "patchset_files": None,
                    "patchset_hash": "",
                }],
                "checkout": [{
                    "comment": None,
                    "git_commit_hash":
                        "5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
                    "git_commit_tags": None,
                    "git_commit_message": None,
                    "git_repository_branch": None,
                    "git_repository_branch_tip": None,
                    "git_repository_url": None,
                    "id":
                        "_:kernelci:"
                        "5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
                    "log_url": None,
                    "message_id": None,
                    "misc": None,
                    "origin": "kernelci",
                    "patchset_hash": "",
                    "start_time": None,
                    "tree_name": None,
                    "valid": None,
                }],
                "build": [{
                    "architecture": None,
                    "checkout_id":
                        "_:google:"
                        "bd355732283c23a365f7c55206c0385100d1c389",
                    "command": None,
                    "comment": None,
                    "compiler": None,
                    "config_name": None,
                    "config_url": None,
                    "duration": None,
                    "id": "google:google.org:a1d993c3n4c448b2j0l1hbf1",
                    "input_files": None,
                    "log_url": None,
                    "misc": None,
                    "origin": "google",
                    "output_files": None,
                    "start_time": None,
                    "status": None,
                }],
                "test": [{
                    "build_id":
                        "google:google.org:a1d993c3n4c448b2j0l1hbf1",
                    "comment": None,
                    "duration": None,
                    "environment_comment": "Tidy",
                    "environment_compatible": [
                        "ti,omap3-beagleboard",
                        "ti,omap3450",
                        "ti,omap3"
                    ],
                    "environment_misc": {"foo": "bar"},
                    "id":
                        "google:google.org:a19di3j5h67f8d9475f26v11",
                    "log_url": None,
                    "misc": None,
                    "origin": "google",
                    "output_files": None,
                    "path": None,
                    "start_time": None,
                    "status": "MISS",
                    "number_value": 1.6e-7,
                    "number_unit": "s",
                    "number_prefix": "metric",
                }],
                "issue": [{
                    "id": "redhat:878234322",
                    "origin": "redhat",
                }],
                "issue_version": [{
                    "comment": "Match USB Bidirectional printer dev",
                    "id": "redhat:878234322",
                    "misc": None,
                    "origin": "redhat",
                    "report_url":
                        "https://bugzilla.redhat.com/show_bug.cgi?"
                        "id=873123",
                    "report_subject":
                        "(cups-usb-quirks) - usb printer doesn't print "
                        "(usblp0: USB Bidirectional printer dev)",
                    "culprit_code": True,
                    "culprit_tool": False,
                    "culprit_harness": False,
                    "version_num": 3,
                }],
                "incident": [{
                    "build_id": None,
                    "comment": None,
                    "id": "redhat:2340981234098123409382",
                    "issue_id": "redhat:878234322",
                    "issue_version_num": 3,
                    "misc": None,
                    "origin": "redhat",
                    "present": True,
                    "test_id":
                        "google:google.org:a19di3j5h67f8d9475f26v11",
                }],
            }
        ),
        kcidb.io.schema.V4_5: dict(
            io={
                "version": {"major": 4, "minor": 5},
                "checkouts": [{
                    "id": "_:kernelci:5acb9c2a7bc836e"
                          "9e5172bbcd2311499c5b4e5f1",
                    "origin": "kernelci",
                    "git_commit_hash": "5acb9c2a7bc836e9e5172bb"
                                       "cd2311499c5b4e5f1",
                    "git_commit_name": "v5.15-4077-g5acb9c2a7bc8",
                    "patchset_hash": "",
                    "git_commit_tags": ["v5.15"],
                    "git_commit_message": "Linux 5.15",
                    "git_repository_branch_tip": False,
                }],
                "builds": [{
                    "id": "google:google.org:a1d993c3n4c448b2j0l1hbf1",
                    "origin": "google",
                    "checkout_id": "_:google:bd355732283c23a365f7c"
                                   "55206c0385100d1c389"
                }],
                "tests": [{
                    "id": "google:google.org:a19di3j5h67f8d9475f26v11",
                    "build_id": "google:google.org:a1d993c3n4c448b2"
                                "j0l1hbf1",
                    "origin": "google",
                    "waived": True,
                    "status": "MISS",
                    "number": {
                        "value": 1.6e-7,
                        "unit": "s",
                        "prefix": "metric",
                    },
                    "environment": {
                        "comment": "Tidy",
                        "misc": {"foo": "bar"},
                        "compatible": [
                            "ti,omap3-beagleboard",
                            "ti,omap3450",
                            "ti,omap3"
                        ],
                    },
                }],
                "issues": [{
                    "id": "redhat:878234322",
                    "version": 3,
                    "origin": "redhat",
                    "report_url":
                        "https://bugzilla.redhat.com/show_bug.cgi"
                        "?id=873123",
                    "report_subject":
                        "(cups-usb-quirks) - usb printer doesn't print "
                        "(usblp0: USB Bidirectional printer dev)",
                    "culprit": {
                        "code": True,
                        "tool": False,
                        "harness": False,
                    },
                    "comment": "Match USB Bidirectional printer dev",
                }],
                "incidents": [{
                    "id": "redhat:2340981234098123409382",
                    "issue_id": "redhat:878234322",
                    "issue_version": 3,
                    "origin": "redhat",
                    "test_id":
                        "google:google.org:a19di3j5h67f8d9475f26v11",
                    "present": True,
                }],
            },
            oo={
                "revision": [{
                    "git_commit_hash":
                        "5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
                    "git_commit_name":
                        "v5.15-4077-g5acb9c2a7bc8",
                    "patchset_files": None,
                    "patchset_hash": "",
                }],
                "checkout": [{
                    "comment": None,
                    "git_commit_hash":
                        "5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
                    "git_commit_tags": ["v5.15"],
                    "git_commit_message": "Linux 5.15",
                    "git_repository_branch": None,
                    "git_repository_branch_tip": False,
                    "git_repository_url": None,
                    "id":
                        "_:kernelci:"
                        "5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
                    "log_url": None,
                    "message_id": None,
                    "misc": None,
                    "origin": "kernelci",
                    "patchset_hash": "",
                    "start_time": None,
                    "tree_name": None,
                    "valid": None,
                }],
                "build": [{
                    "architecture": None,
                    "checkout_id":
                        "_:google:"
                        "bd355732283c23a365f7c55206c0385100d1c389",
                    "command": None,
                    "comment": None,
                    "compiler": None,
                    "config_name": None,
                    "config_url": None,
                    "duration": None,
                    "id": "google:google.org:a1d993c3n4c448b2j0l1hbf1",
                    "input_files": None,
                    "log_url": None,
                    "misc": None,
                    "origin": "google",
                    "output_files": None,
                    "start_time": None,
                    "status": None,
                }],
                "test": [{
                    "build_id":
                        "google:google.org:a1d993c3n4c448b2j0l1hbf1",
                    "comment": None,
                    "duration": None,
                    "environment_comment": "Tidy",
                    "environment_compatible": [
                        "ti,omap3-beagleboard",
                        "ti,omap3450",
                        "ti,omap3"
                    ],
                    "environment_misc": {"foo": "bar"},
                    "id":
                        "google:google.org:a19di3j5h67f8d9475f26v11",
                    "log_url": None,
                    "misc": None,
                    "origin": "google",
                    "output_files": None,
                    "path": None,
                    "start_time": None,
                    "status": "MISS",
                    "number_value": 1.6e-7,
                    "number_unit": "s",
                    "number_prefix": "metric",
                }],
                "issue": [{
                    "id": "redhat:878234322",
                    "origin": "redhat",
                }],
                "issue_version": [{
                    "comment": "Match USB Bidirectional printer dev",
                    "id": "redhat:878234322",
                    "misc": None,
                    "origin": "redhat",
                    "report_url":
                        "https://bugzilla.redhat.com/show_bug.cgi?"
                        "id=873123",
                    "report_subject":
                        "(cups-usb-quirks) - usb printer doesn't print "
                        "(usblp0: USB Bidirectional printer dev)",
                    "culprit_code": True,
                    "culprit_tool": False,
                    "culprit_harness": False,
                    "version_num": 3,
                }],
                "incident": [{
                    "build_id": None,
                    "comment": None,
                    "id": "redhat:2340981234098123409382",
                    "issue_id": "redhat:878234322",
                    "issue_version_num": 3,
                    "misc": None,
                    "origin": "redhat",
                    "present": True,
                    "test_id":
                        "google:google.org:a19di3j5h67f8d9475f26v11",
                }],
            }
        ),
        kcidb.io.schema.V5_0: dict(
            io={
                "version": {"major": 5, "minor": 0},
                "checkouts": [{
                    "id": "_:kernelci:5acb9c2a7bc836e"
                          "9e5172bbcd2311499c5b4e5f1",
                    "origin": "kernelci",
                    "git_commit_hash": "5acb9c2a7bc836e9e5172bb"
                                       "cd2311499c5b4e5f1",
                    "git_commit_name": "v5.15-4077-g5acb9c2a7bc8",
                    "patchset_hash": "",
                    "git_commit_tags": ["v5.15"],
                    "git_commit_message": "Linux 5.15",
                    "git_repository_branch_tip": False,
                }],
                "builds": [{
                    "id": "google:google.org:a1d993c3n4c448b2j0l1hbf1",
                    "origin": "google",
                    "checkout_id": "_:google:bd355732283c23a365f7c"
                                   "55206c0385100d1c389",
                    "status": "PASS",
                }],
                "tests": [{
                    "id": "google:google.org:a19di3j5h67f8d9475f26v11",
                    "build_id": "google:google.org:a1d993c3n4c448b2"
                                "j0l1hbf1",
                    "origin": "google",
                    "status": "MISS",
                    "number": {
                        "value": 1.6e-7,
                        "unit": "s",
                        "prefix": "metric",
                    },
                    "environment": {
                        "comment": "Tidy",
                        "misc": {"foo": "bar"},
                        "compatible": [
                            "ti,omap3-beagleboard",
                            "ti,omap3450",
                            "ti,omap3"
                        ],
                    },
                }],
                "issues": [{
                    "id": "redhat:878234322",
                    "version": 3,
                    "origin": "redhat",
                    "report_url":
                        "https://bugzilla.redhat.com/show_bug.cgi"
                        "?id=873123",
                    "report_subject":
                        "(cups-usb-quirks) - usb printer doesn't print "
                        "(usblp0: USB Bidirectional printer dev)",
                    "culprit": {
                        "code": True,
                        "tool": False,
                        "harness": False,
                    },
                    "comment": "Match USB Bidirectional printer dev",
                }],
                "incidents": [{
                    "id": "redhat:2340981234098123409382",
                    "issue_id": "redhat:878234322",
                    "issue_version": 3,
                    "origin": "redhat",
                    "test_id":
                        "google:google.org:a19di3j5h67f8d9475f26v11",
                    "present": True,
                }],
            },
            oo={
                "revision": [{
                    "git_commit_hash":
                        "5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
                    "git_commit_name":
                        "v5.15-4077-g5acb9c2a7bc8",
                    "patchset_files": None,
                    "patchset_hash": "",
                }],
                "checkout": [{
                    "comment": None,
                    "git_commit_hash":
                        "5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
                    "git_commit_tags": ["v5.15"],
                    "git_commit_message": "Linux 5.15",
                    "git_repository_branch": None,
                    "git_repository_branch_tip": False,
                    "git_repository_url": None,
                    "id":
                        "_:kernelci:"
                        "5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
                    "log_url": None,
                    "message_id": None,
                    "misc": None,
                    "origin": "kernelci",
                    "patchset_hash": "",
                    "start_time": None,
                    "tree_name": None,
                    "valid": None,
                }],
                "build": [{
                    "architecture": None,
                    "checkout_id":
                        "_:google:"
                        "bd355732283c23a365f7c55206c0385100d1c389",
                    "command": None,
                    "comment": None,
                    "compiler": None,
                    "config_name": None,
                    "config_url": None,
                    "duration": None,
                    "id": "google:google.org:a1d993c3n4c448b2j0l1hbf1",
                    "input_files": None,
                    "log_url": None,
                    "misc": None,
                    "origin": "google",
                    "output_files": None,
                    "start_time": None,
                    "status": "PASS",
                }],
                "test": [{
                    "build_id":
                        "google:google.org:a1d993c3n4c448b2j0l1hbf1",
                    "comment": None,
                    "duration": None,
                    "environment_comment": "Tidy",
                    "environment_compatible": [
                        "ti,omap3-beagleboard",
                        "ti,omap3450",
                        "ti,omap3"
                    ],
                    "environment_misc": {"foo": "bar"},
                    "id":
                        "google:google.org:a19di3j5h67f8d9475f26v11",
                    "log_url": None,
                    "misc": None,
                    "origin": "google",
                    "output_files": None,
                    "path": None,
                    "start_time": None,
                    "status": "MISS",
                    "number_value": 1.6e-7,
                    "number_unit": "s",
                    "number_prefix": "metric",
                }],
                "issue": [{
                    "id": "redhat:878234322",
                    "origin": "redhat",
                }],
                "issue_version": [{
                    "comment": "Match USB Bidirectional printer dev",
                    "id": "redhat:878234322",
                    "misc": None,
                    "origin": "redhat",
                    "report_url":
                        "https://bugzilla.redhat.com/show_bug.cgi?"
                        "id=873123",
                    "report_subject":
                        "(cups-usb-quirks) - usb printer doesn't print "
                        "(usblp0: USB Bidirectional printer dev)",
                    "culprit_code": True,
                    "culprit_tool": False,
                    "culprit_harness": False,
                    "version_num": 3,
                }],
                "incident": [{
                    "build_id": None,
                    "comment": None,
                    "id": "redhat:2340981234098123409382",
                    "issue_id": "redhat:878234322",
                    "issue_version_num": 3,
                    "misc": None,
                    "origin": "redhat",
                    "present": True,
                    "test_id":
                        "google:google.org:a19di3j5h67f8d9475f26v11",
                }],
            }
        ),
        kcidb.io.schema.V5_1: dict(
            io={
                "version": {"major": 5, "minor": 1},
                "checkouts": [{
                    "id": "_:kernelci:5acb9c2a7bc836e"
                          "9e5172bbcd2311499c5b4e5f1",
                    "origin": "kernelci",
                    "git_commit_hash": "5acb9c2a7bc836e9e5172bb"
                                       "cd2311499c5b4e5f1",
                    "git_commit_name": "v5.15-4077-g5acb9c2a7bc8",
                    "patchset_hash": "",
                    "git_commit_tags": ["v5.15"],
                    "git_commit_message": "Linux 5.15",
                    "git_repository_branch_tip": False,
                    "origin_builds_finish_time":
                    "2025-08-14T23:08:06.967000+00:00",
                    "origin_tests_finish_time":
                    "2025-08-14T23:10:06.967000+00:00",
                }],
                "builds": [{
                    "id": "google:google.org:a1d993c3n4c448b2j0l1hbf1",
                    "origin": "google",
                    "checkout_id": "_:google:bd355732283c23a365f7c"
                                   "55206c0385100d1c389",
                    "status": "PASS",
                }],
                "tests": [{
                    "id": "google:google.org:a19di3j5h67f8d9475f26v11",
                    "build_id": "google:google.org:a1d993c3n4c448b2"
                                "j0l1hbf1",
                    "origin": "google",
                    "status": "MISS",
                    "number": {
                        "value": 1.6e-7,
                        "unit": "s",
                        "prefix": "metric",
                    },
                    "environment": {
                        "comment": "Tidy",
                        "misc": {"foo": "bar"},
                        "compatible": [
                            "ti,omap3-beagleboard",
                            "ti,omap3450",
                            "ti,omap3"
                        ],
                    },
                }],
                "issues": [{
                    "id": "redhat:878234322",
                    "version": 3,
                    "origin": "redhat",
                    "report_url":
                        "https://bugzilla.redhat.com/show_bug.cgi"
                        "?id=873123",
                    "report_subject":
                        "(cups-usb-quirks) - usb printer doesn't print "
                        "(usblp0: USB Bidirectional printer dev)",
                    "culprit": {
                        "code": True,
                        "tool": False,
                        "harness": False,
                    },
                    "comment": "Match USB Bidirectional printer dev",
                }],
                "incidents": [{
                    "id": "redhat:2340981234098123409382",
                    "issue_id": "redhat:878234322",
                    "issue_version": 3,
                    "origin": "redhat",
                    "test_id":
                        "google:google.org:a19di3j5h67f8d9475f26v11",
                    "present": True,
                }],
            },
            oo={
                "revision": [{
                    "git_commit_hash":
                        "5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
                    "git_commit_name":
                        "v5.15-4077-g5acb9c2a7bc8",
                    "patchset_files": None,
                    "patchset_hash": "",
                }],
                "checkout": [{
                    "comment": None,
                    "git_commit_hash":
                        "5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
                    "git_commit_tags": ["v5.15"],
                    "git_commit_message": "Linux 5.15",
                    "git_repository_branch": None,
                    "git_repository_branch_tip": False,
                    "git_repository_url": None,
                    "id":
                        "_:kernelci:"
                        "5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
                    "log_url": None,
                    "message_id": None,
                    "misc": None,
                    "origin": "kernelci",
                    "patchset_hash": "",
                    "start_time": None,
                    "tree_name": None,
                    "valid": None,
                }],
                "build": [{
                    "architecture": None,
                    "checkout_id":
                        "_:google:"
                        "bd355732283c23a365f7c55206c0385100d1c389",
                    "command": None,
                    "comment": None,
                    "compiler": None,
                    "config_name": None,
                    "config_url": None,
                    "duration": None,
                    "id": "google:google.org:a1d993c3n4c448b2j0l1hbf1",
                    "input_files": None,
                    "log_url": None,
                    "misc": None,
                    "origin": "google",
                    "output_files": None,
                    "start_time": None,
                    "status": "PASS",
                }],
                "test": [{
                    "build_id":
                        "google:google.org:a1d993c3n4c448b2j0l1hbf1",
                    "comment": None,
                    "duration": None,
                    "environment_comment": "Tidy",
                    "environment_compatible": [
                        "ti,omap3-beagleboard",
                        "ti,omap3450",
                        "ti,omap3"
                    ],
                    "environment_misc": {"foo": "bar"},
                    "id":
                        "google:google.org:a19di3j5h67f8d9475f26v11",
                    "log_url": None,
                    "misc": None,
                    "origin": "google",
                    "output_files": None,
                    "path": None,
                    "start_time": None,
                    "status": "MISS",
                    "number_value": 1.6e-7,
                    "number_unit": "s",
                    "number_prefix": "metric",
                }],
                "issue": [{
                    "id": "redhat:878234322",
                    "origin": "redhat",
                }],
                "issue_version": [{
                    "comment": "Match USB Bidirectional printer dev",
                    "id": "redhat:878234322",
                    "misc": None,
                    "origin": "redhat",
                    "report_url":
                        "https://bugzilla.redhat.com/show_bug.cgi?"
                        "id=873123",
                    "report_subject":
                        "(cups-usb-quirks) - usb printer doesn't print "
                        "(usblp0: USB Bidirectional printer dev)",
                    "culprit_code": True,
                    "culprit_tool": False,
                    "culprit_harness": False,
                    "version_num": 3,
                }],
                "incident": [{
                    "build_id": None,
                    "comment": None,
                    "id": "redhat:2340981234098123409382",
                    "issue_id": "redhat:878234322",
                    "issue_version_num": 3,
                    "misc": None,
                    "origin": "redhat",
                    "present": True,
                    "test_id":
                        "google:google.org:a19di3j5h67f8d9475f26v11",
                }],
            }
        ),
        kcidb.io.schema.V5_2: dict(
            io={
                "version": {"major": 5, "minor": 2},
                "checkouts": [{
                    "id": "_:kernelci:5acb9c2a7bc836e"
                          "9e5172bbcd2311499c5b4e5f1",
                    "origin": "kernelci",
                    "git_commit_hash": "5acb9c2a7bc836e9e5172bb"
                                       "cd2311499c5b4e5f1",
                    "git_commit_name": "v5.15-4077-g5acb9c2a7bc8",
                    "patchset_hash": "",
                    "git_commit_tags": ["v5.15"],
                    "git_commit_message": "Linux 5.15",
                    "git_repository_branch_tip": False,
                    "origin_builds_finish_time":
                    "2025-08-14T23:08:06.967000+00:00",
                    "origin_tests_finish_time":
                    "2025-08-14T23:10:06.967000+00:00",
                }],
                "builds": [{
                    "id": "google:google.org:a1d993c3n4c448b2j0l1hbf1",
                    "origin": "google",
                    "checkout_id": "_:google:bd355732283c23a365f7c"
                                   "55206c0385100d1c389",
                    "status": "PASS",
                }],
                "tests": [{
                    "id": "google:google.org:a19di3j5h67f8d9475f26v11",
                    "build_id": "google:google.org:a1d993c3n4c448b2"
                                "j0l1hbf1",
                    "origin": "google",
                    "status": "MISS",
                    "number": {
                        "value": 1.6e-7,
                        "unit": "s",
                        "prefix": "metric",
                    },
                    "environment": {
                        "comment": "Tidy",
                        "misc": {"foo": "bar"},
                        "compatible": [
                            "ti,omap3-beagleboard",
                            "ti,omap3450",
                            "ti,omap3"
                        ],
                    },
                }],
                "issues": [{
                    "id": "redhat:878234322",
                    "version": 3,
                    "origin": "redhat",
                    "report_url":
                        "https://bugzilla.redhat.com/show_bug.cgi"
                        "?id=873123",
                    "report_subject":
                        "(cups-usb-quirks) - usb printer doesn't print "
                        "(usblp0: USB Bidirectional printer dev)",
                    "culprit": {
                        "code": True,
                        "tool": False,
                        "harness": False,
                    },
                    "categories": ["subsystem.usb", "reported-by.user"],
                    "comment": "Match USB Bidirectional printer dev",
                }],
                "incidents": [{
                    "id": "redhat:2340981234098123409382",
                    "issue_id": "redhat:878234322",
                    "issue_version": 3,
                    "origin": "redhat",
                    "test_id":
                        "google:google.org:a19di3j5h67f8d9475f26v11",
                    "present": True,
                }],
            },
            oo={
                "revision": [{
                    "git_commit_hash":
                        "5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
                    "git_commit_name":
                        "v5.15-4077-g5acb9c2a7bc8",
                    "patchset_files": None,
                    "patchset_hash": "",
                }],
                "checkout": [{
                    "comment": None,
                    "git_commit_hash":
                        "5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
                    "git_commit_tags": ["v5.15"],
                    "git_commit_message": "Linux 5.15",
                    "git_repository_branch": None,
                    "git_repository_branch_tip": False,
                    "git_repository_url": None,
                    "id":
                        "_:kernelci:"
                        "5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
                    "log_url": None,
                    "message_id": None,
                    "misc": None,
                    "origin": "kernelci",
                    "patchset_hash": "",
                    "start_time": None,
                    "tree_name": None,
                    "valid": None,
                }],
                "build": [{
                    "architecture": None,
                    "checkout_id":
                        "_:google:"
                        "bd355732283c23a365f7c55206c0385100d1c389",
                    "command": None,
                    "comment": None,
                    "compiler": None,
                    "config_name": None,
                    "config_url": None,
                    "duration": None,
                    "id": "google:google.org:a1d993c3n4c448b2j0l1hbf1",
                    "input_files": None,
                    "log_url": None,
                    "misc": None,
                    "origin": "google",
                    "output_files": None,
                    "start_time": None,
                    "status": "PASS",
                }],
                "test": [{
                    "build_id":
                        "google:google.org:a1d993c3n4c448b2j0l1hbf1",
                    "comment": None,
                    "duration": None,
                    "environment_comment": "Tidy",
                    "environment_compatible": [
                        "ti,omap3-beagleboard",
                        "ti,omap3450",
                        "ti,omap3"
                    ],
                    "environment_misc": {"foo": "bar"},
                    "id":
                        "google:google.org:a19di3j5h67f8d9475f26v11",
                    "log_url": None,
                    "misc": None,
                    "origin": "google",
                    "output_files": None,
                    "path": None,
                    "start_time": None,
                    "status": "MISS",
                    "number_value": 1.6e-7,
                    "number_unit": "s",
                    "number_prefix": "metric",
                }],
                "issue": [{
                    "id": "redhat:878234322",
                    "origin": "redhat",
                }],
                "issue_version": [{
                    "comment": "Match USB Bidirectional printer dev",
                    "id": "redhat:878234322",
                    "misc": None,
                    "origin": "redhat",
                    "report_url":
                        "https://bugzilla.redhat.com/show_bug.cgi?"
                        "id=873123",
                    "report_subject":
                        "(cups-usb-quirks) - usb printer doesn't print "
                        "(usblp0: USB Bidirectional printer dev)",
                    "culprit_code": True,
                    "culprit_tool": False,
                    "culprit_harness": False,
                    "version_num": 3,
                }],
                "incident": [{
                    "build_id": None,
                    "comment": None,
                    "id": "redhat:2340981234098123409382",
                    "issue_id": "redhat:878234322",
                    "issue_version_num": 3,
                    "misc": None,
                    "origin": "redhat",
                    "present": True,
                    "test_id":
                        "google:google.org:a19di3j5h67f8d9475f26v11",
                }],
            }
        ),
        kcidb.io.schema.V5_3: dict(
            io={
                "version": {"major": 5, "minor": 3},
                "checkouts": [{
                    "id": "_:kernelci:5acb9c2a7bc836e"
                          "9e5172bbcd2311499c5b4e5f1",
                    "origin": "kernelci",
                    "git_commit_hash": "5acb9c2a7bc836e9e5172bb"
                                       "cd2311499c5b4e5f1",
                    "git_commit_name": "v5.15-4077-g5acb9c2a7bc8",
                    "patchset_hash": "",
                    "git_commit_tags": ["v5.15"],
                    "git_commit_message": "Linux 5.15",
                    "git_repository_branch_tip": False,
                    "origin_builds_finish_time":
                    "2025-08-14T23:08:06.967000+00:00",
                    "origin_tests_finish_time":
                    "2025-08-14T23:10:06.967000+00:00",
                }],
                "builds": [{
                    "id": "google:google.org:a1d993c3n4c448b2j0l1hbf1",
                    "origin": "google",
                    "checkout_id": "_:google:bd355732283c23a365f7c"
                                   "55206c0385100d1c389",
                    "status": "PASS",
                }],
                "tests": [{
                    "id": "google:google.org:a19di3j5h67f8d9475f26v11",
                    "build_id": "google:google.org:a1d993c3n4c448b2"
                                "j0l1hbf1",
                    "origin": "google",
                    "status": "MISS",
                    "number": {
                        "value": 1.6e-7,
                        "unit": "s",
                        "prefix": "metric",
                    },
                    "environment": {
                        "comment": "Tidy",
                        "misc": {"foo": "bar"},
                        "compatible": [
                            "ti,omap3-beagleboard",
                            "ti,omap3450",
                            "ti,omap3"
                        ],
                    },
                    "input_files": [
                        {
                            "name": "rootfs.img",
                            "url": "https://github.com/riscv/kernel/commit/"
                                   "f89927a3f84da7898ec9f4f1be8e4e0433db7879/"
                                   "checks/39474543369/rootfs.img"
                        },
                        {
                            "name": "samples.csv",
                            "url": "https://github.com/riscv/kernel/commit/"
                                   "f89927a3f84da7898ec9f4f1be8e4e0433db7879/"
                                   "checks/39474543369/samples.csv"
                        },
                    ],
                }],
                "issues": [{
                    "id": "redhat:878234322",
                    "version": 3,
                    "origin": "redhat",
                    "report_url":
                        "https://bugzilla.redhat.com/show_bug.cgi"
                        "?id=873123",
                    "report_subject":
                        "(cups-usb-quirks) - usb printer doesn't print "
                        "(usblp0: USB Bidirectional printer dev)",
                    "culprit": {
                        "code": True,
                        "tool": False,
                        "harness": False,
                    },
                    "categories": ["subsystem.usb", "reported-by.user"],
                    "comment": "Match USB Bidirectional printer dev",
                }],
                "incidents": [{
                    "id": "redhat:2340981234098123409382",
                    "issue_id": "redhat:878234322",
                    "issue_version": 3,
                    "origin": "redhat",
                    "test_id":
                        "google:google.org:a19di3j5h67f8d9475f26v11",
                    "present": True,
                }],
            },
            oo={
                "revision": [{
                    "git_commit_hash":
                        "5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
                    "git_commit_name":
                        "v5.15-4077-g5acb9c2a7bc8",
                    "patchset_files": None,
                    "patchset_hash": "",
                }],
                "checkout": [{
                    "comment": None,
                    "git_commit_hash":
                        "5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
                    "git_commit_tags": ["v5.15"],
                    "git_commit_message": "Linux 5.15",
                    "git_repository_branch": None,
                    "git_repository_branch_tip": False,
                    "git_repository_url": None,
                    "id":
                        "_:kernelci:"
                        "5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
                    "log_url": None,
                    "message_id": None,
                    "misc": None,
                    "origin": "kernelci",
                    "patchset_hash": "",
                    "start_time": None,
                    "tree_name": None,
                    "valid": None,
                }],
                "build": [{
                    "architecture": None,
                    "checkout_id":
                        "_:google:"
                        "bd355732283c23a365f7c55206c0385100d1c389",
                    "command": None,
                    "comment": None,
                    "compiler": None,
                    "config_name": None,
                    "config_url": None,
                    "duration": None,
                    "id": "google:google.org:a1d993c3n4c448b2j0l1hbf1",
                    "input_files": None,
                    "log_url": None,
                    "misc": None,
                    "origin": "google",
                    "output_files": None,
                    "start_time": None,
                    "status": "PASS",
                }],
                "test": [{
                    "build_id":
                        "google:google.org:a1d993c3n4c448b2j0l1hbf1",
                    "comment": None,
                    "duration": None,
                    "environment_comment": "Tidy",
                    "environment_compatible": [
                        "ti,omap3-beagleboard",
                        "ti,omap3450",
                        "ti,omap3"
                    ],
                    "environment_misc": {"foo": "bar"},
                    "id":
                        "google:google.org:a19di3j5h67f8d9475f26v11",
                    "log_url": None,
                    "misc": None,
                    "origin": "google",
                    "output_files": None,
                    "path": None,
                    "start_time": None,
                    "status": "MISS",
                    "number_value": 1.6e-7,
                    "number_unit": "s",
                    "number_prefix": "metric",
                }],
                "issue": [{
                    "id": "redhat:878234322",
                    "origin": "redhat",
                }],
                "issue_version": [{
                    "comment": "Match USB Bidirectional printer dev",
                    "id": "redhat:878234322",
                    "misc": None,
                    "origin": "redhat",
                    "report_url":
                        "https://bugzilla.redhat.com/show_bug.cgi?"
                        "id=873123",
                    "report_subject":
                        "(cups-usb-quirks) - usb printer doesn't print "
                        "(usblp0: USB Bidirectional printer dev)",
                    "culprit_code": True,
                    "culprit_tool": False,
                    "culprit_harness": False,
                    "version_num": 3,
                }],
                "incident": [{
                    "build_id": None,
                    "comment": None,
                    "id": "redhat:2340981234098123409382",
                    "issue_id": "redhat:878234322",
                    "issue_version_num": 3,
                    "misc": None,
                    "origin": "redhat",
                    "present": True,
                    "test_id":
                        "google:google.org:a19di3j5h67f8d9475f26v11",
                }],
            }
        ),
    }

    database = clean_database
    database_io_versions = set(database.get_schemas().values())
    last_io_version = None
    last_params = None

    # For each I/O version supported by the database, and corresponding params
    for io_version, params in io_version_params.items():
        if io_version not in database_io_versions:
            continue

        # If it's not the database's first I/O version
        if last_io_version and last_params:
            print(f"Upgrading the database from I/O {last_io_version} to "
                  f"{io_version}")
            # Upgrade the database to this I/O version
            database.upgrade(io_version)
            # Check upgrade went well
            # You're wrong, pylint: disable=unsubscriptable-object
            assert io_version.major > last_io_version.major or \
                database.oo_query(
                    kcidb.orm.query.Pattern.parse(">*#")
                ) == last_params["oo"], \
                f"OO data mismatch after minor version upgrade " \
                f"from {last_io_version} to {io_version}"
            upgraded_io = io_version.upgrade(last_params["io"])
            assert io_version.cmp_directly_compatible(
                    database.dump(with_metadata=False),
                    upgraded_io
                ) == 0, \
                f"Dump doesn't match upgraded I/O data after upgrade " \
                f"from {last_io_version} to {io_version}"
            assert io_version.cmp_directly_compatible(
                    database.query(io_version.get_ids(upgraded_io)),
                    upgraded_io
                ) == 0, \
                f"Query result doesn't match upgraded I/O data after " \
                f"upgrade from {last_io_version} to {io_version}"
            # Check all rows have _timestamp assigned, when they should
            assert io_version < kcidb.io.schema.V4_3 or \
                all(
                    all('_timestamp' in obj for obj in v)
                    for v in database.dump(with_metadata=True).values()
                    if isinstance(v, list)
                ), \
                f"Some objects are missing _timestamp metadata after " \
                f"upgrade from {last_io_version} to {io_version}"
        # Else, this is the database's first I/O version
        else:
            print(f"Initializing the database to I/O {io_version}")
            # Initialize the database to the first version
            database.init(io_version)

        # Check that directly-compatible data versions can be loaded correctly
        # into the database, and others cannot

        # For each data's I/O version and parameters
        for load_io_version, load_params in io_version_params.items():
            # If version of I/O data to load is not directly-compatible with
            # the database's version
            if load_io_version > io_version or \
               load_io_version.major < io_version.major:
                print(f"Checking {load_io_version} data fails to load into "
                      f"{io_version} I/O database")
                # Make sure load fails
                with pytest.raises(AssertionError):
                    database.load(load_params["io"])
                continue

            # Make sure the database is empty
            print(f"Emptying the {io_version} I/O database")
            database.empty()

            # Load the data
            print(f"Loading {load_io_version} data into "
                  f"{io_version} I/O database")
            database.load(load_params["io"])

            # Check we can query it in various ways
            upgraded_io = io_version.upgrade(load_params["io"])
            assert io_version.cmp_directly_compatible(
                    database.dump(with_metadata=False),
                    upgraded_io
                ) == 0, \
                f"Data with schema {load_io_version} loaded into " \
                f"{io_version} DB and dumped doesn't match directly-upgraded"
            assert io_version.cmp_directly_compatible(
                    database.query(io_version.get_ids(upgraded_io)),
                    upgraded_io
                ) == 0, \
                f"Data with schema {load_io_version} loaded into " \
                f"{io_version} DB and queried doesn't match directly-upgraded"
            assert database.oo_query(kcidb.orm.query.Pattern.parse(">*#")) == \
                load_params["oo"], \
                f"Data with schema {load_io_version} loaded into " \
                f"{io_version} DB and ORM-queried doesn't match expectations"
            # Check all rows have _timestamp assigned, when they should
            assert io_version < kcidb.io.schema.V4_3 or \
                all(
                    all('_timestamp' in obj for obj in v)
                    for v in database.dump(with_metadata=True).values()
                    if isinstance(v, list)
                ), \
                f"Data with schema {load_io_version} loaded into " \
                f"{io_version} DB has objects without _timestamp"

        # Remember this I/O version and its parameters for the next round
        last_io_version = io_version
        last_params = params


def test_query(empty_database):
    """Test the query() method retrieves objects correctly"""
    client = empty_database
    client.load(dict(
        version=dict(major=5, minor=0),
        checkouts=[
            dict(id="_:1", origin="_"),
            dict(id="_:2", origin="_"),
        ],
        builds=[
            dict(id="_:1", origin="_", checkout_id="_:1"),
            dict(id="_:2", origin="_", checkout_id="_:2"),
        ],
        tests=[
            dict(id="_:1", origin="_", build_id="_:1"),
            dict(id="_:2", origin="_", build_id="_:2"),
        ],
        issues=[
            dict(id="_:1", origin="_", version=1),
            dict(id="_:2", origin="_", version=1),
            dict(id="_:3", origin="_", version=1),
            dict(id="_:4", origin="_", version=1),
        ],
        incidents=[
            dict(id="_:1", origin="_", issue_id="_:1", issue_version=1,
                 build_id="_:1"),
            dict(id="_:2", origin="_", issue_id="_:2", issue_version=1,
                 test_id="_:1"),
            dict(id="_:3", origin="_", issue_id="_:3", issue_version=1,
                 build_id="_:2", test_id="_:2"),
            dict(id="_:4", origin="_", issue_id="_:4", issue_version=1,
                 test_id="_:1"),
            dict(id="_:5", origin="_", issue_id="_:4", issue_version=1,
                 test_id="_:2"),
        ],
    ))
    assert client.query(ids=dict(checkouts=["_:1"]), children=True) in \
        [
            {
                "version": {"major": kcidb.io.SCHEMA.major,
                            "minor": kcidb.io.SCHEMA.minor},
                "checkouts": [
                    {"id": "_:1", "origin": "_"}
                ],
                "builds": [
                    {"checkout_id": "_:1", "id": "_:1", "origin": "_"}
                ],
                "tests": [
                    {"build_id": "_:1", "id": "_:1", "origin": "_"}
                ],
                "incidents": list(incidents),
            }
            for incidents in permutations([
                {
                    "build_id": "_:1",
                    "id": "_:1",
                    "issue_id": "_:1",
                    "issue_version": 1,
                    "origin": "_",
                },
                {
                    "id": "_:2",
                    "issue_id": "_:2",
                    "issue_version": 1,
                    "origin": "_",
                    "test_id": "_:1",
                },
                {
                    'id': '_:4',
                    'issue_id': '_:4',
                    'issue_version': 1,
                    'origin': '_',
                    'test_id': '_:1',
                },
            ])
        ]

    unambigious_result = {
        "version": {"major": kcidb.io.SCHEMA.major,
                    "minor": kcidb.io.SCHEMA.minor},
        "checkouts": [
            {"id": "_:2", "origin": "_"}
        ],
        "builds": [
            {"checkout_id": "_:2", "id": "_:2", "origin": "_"}
        ],
        "tests": [
            {"build_id": "_:2", "id": "_:2", "origin": "_"}
        ],
    }
    incident_x = {
        "build_id": "_:2",
        "id": "_:3",
        "issue_id": "_:3",
        "issue_version": 1,
        "origin": "_",
        "test_id": "_:2",
    }
    incident_y = {
        "id": "_:5",
        "issue_id": "_:4",
        "issue_version": 1,
        "origin": "_",
        "test_id": "_:2",
    }
    assert client.query(ids=dict(checkouts=["_:2"]), children=True) in [
        {
            **unambigious_result,
            "incidents": [incident_x, incident_y],
        },
        {
            **unambigious_result,
            "incidents": [incident_y, incident_x],
        },
    ]

    assert client.query(ids=dict(incidents=["_:3"]), parents=True) == \
        {
            "version": {"major": kcidb.io.SCHEMA.major,
                        "minor": kcidb.io.SCHEMA.minor},
            "checkouts": [
                {"id": "_:2", "origin": "_"}
            ],
            "builds": [
                {"checkout_id": "_:2", "id": "_:2", "origin": "_"}
            ],
            "tests": [
                {"build_id": "_:2", "id": "_:2", "origin": "_"}
            ],
            "issues": [
                {"id": "_:3", "origin": "_", "version": 1}
            ],
            "incidents": [
                {
                    "build_id": "_:2",
                    "id": "_:3",
                    "issue_id": "_:3",
                    "issue_version": 1,
                    "origin": "_",
                    "test_id": "_:2",
                }
            ],
        }

    unambigious_result = {
        "version": {"major": kcidb.io.SCHEMA.major,
                    "minor": kcidb.io.SCHEMA.minor},
        "checkouts": [
            {"id": "_:2", "origin": "_"}
        ],
        "builds": [
            {"checkout_id": "_:2", "id": "_:2", "origin": "_"}
        ],
        "tests": [
            {"build_id": "_:2", "id": "_:2", "origin": "_"}
        ],
        "issues": [
            {"id": "_:3", "origin": "_", "version": 1}
        ],
    }
    incident_x = {
        "build_id": "_:2",
        "id": "_:3",
        "issue_id": "_:3",
        "issue_version": 1,
        "origin": "_",
        "test_id": "_:2",
    }
    incident_y = {
        "id": "_:5",
        "issue_id": "_:4",
        "issue_version": 1,
        "origin": "_",
        "test_id": "_:2",
    }
    assert client.query(ids=dict(incidents=["_:3"]),
                        parents=True, children=True) in [
        {
            **unambigious_result,
            "incidents": [incident_x, incident_y],
        },
        {
            **unambigious_result,
            "incidents": [incident_y, incident_x],
        },
    ]


def test_test_status(empty_database):
    """Test all test status values are accepted and preserved"""
    status_set = set(kcidb.io.SCHEMA.json["$defs"]["status"]["enum"])
    client = empty_database
    client.load({
        **kcidb.io.SCHEMA.new(),
        "tests": [
            dict(build_id="_:1", origin="_", id="_:" + status, status=status)
            for status in status_set
        ]
    })
    dump = client.dump(with_metadata=False)
    assert status_set == set(test["id"][2:] for test in dump["tests"])
    assert status_set == set(test["status"] for test in dump["tests"])


def test_empty(empty_database):
    """Test the empty() method removes all data"""
    io_data = COMPREHENSIVE_IO_DATA
    client = empty_database
    client.load(io_data)
    assert io_data == client.dump(with_metadata=False)
    client.empty()
    assert kcidb.io.SCHEMA.new() == client.dump()


def test_cleanup(clean_database):
    """Test the clean() method removes all data"""
    client = clean_database
    # Initialize/clean every version twice, going up
    for version in client.get_schemas():
        client.init(version)
        client.cleanup()
        client.init(version)
        client.cleanup()
    # Initialize/clean every version going down
    for version in reversed(list(client.get_schemas())[:-1]):
        client.init(version)
        client.cleanup()


def test_purge(empty_database):
    """Test the purge() method behaves as documented"""
    client = empty_database

    drivers = [*client.driver.drivers] \
        if isinstance(client.driver, kcidb.db.mux.Driver) \
        else [client.driver]

    # If this is a database and schema which *should* support purging
    if all(
        isinstance(driver,
                   (kcidb.db.bigquery.Driver,
                    kcidb.db.postgresql.Driver,
                    kcidb.db.sqlite.Driver)) and
        driver.get_schema()[0] >= (4, 2)
        for driver in drivers
    ):
        io_data_1 = deepcopy(COMPREHENSIVE_IO_DATA)
        io_data_2 = deepcopy(COMPREHENSIVE_IO_DATA)
        for obj_list_name in kcidb.io.SCHEMA.graph:
            if obj_list_name:
                for obj in io_data_2[obj_list_name]:
                    obj["id"] = "origin:2"

        assert client.purge(None)
        client.load(io_data_1)
        assert client.dump(with_metadata=False) == io_data_1
        time.sleep(1)
        after_first_load = client.get_current_time()
        time.sleep(1)
        client.load(io_data_2)

        # Check both datasets are in the database
        # regardless of the object order
        dump = client.dump(with_metadata=False)
        for io_data in (io_data_1, io_data_2):
            for obj_list_name in kcidb.io.SCHEMA.graph:
                if obj_list_name:
                    assert obj_list_name in dump
                    for obj in io_data[obj_list_name]:
                        assert obj in dump[obj_list_name]

        assert client.purge(after_first_load)
        assert client.dump(with_metadata=False) == io_data_2
        time.sleep(1)
        assert client.purge(client.get_current_time())
        assert client.dump() == kcidb.io.SCHEMA.new()
    else:
        assert not client.purge(None)


def test_dump_limits(empty_database):
    """Test the dump() method observes time limits"""
    # It's OK, pylint: disable=too-many-locals
    client = empty_database

    # Check we can do a basic dump
    io_schema = client.get_schema()[1]
    assert client.dump(with_metadata=False) == io_schema.new()

    drivers = [*client.driver.drivers] \
        if isinstance(client.driver, kcidb.db.mux.Driver) \
        else [client.driver]

    # If this is a database and schema which *should* support purging
    if all(
        isinstance(driver,
                   (kcidb.db.bigquery.Driver,
                    kcidb.db.postgresql.Driver,
                    kcidb.db.sqlite.Driver)) and
        driver.get_schema()[0] >= (4, 2)
        for driver in drivers
    ):
        io_data_1 = deepcopy(COMPREHENSIVE_IO_DATA)
        io_data_2 = deepcopy(COMPREHENSIVE_IO_DATA)
        for obj_list_name in kcidb.io.SCHEMA.graph:
            if obj_list_name:
                for obj in io_data_2[obj_list_name]:
                    obj["id"] = "origin:2"

        client.load(io_data_1)
        assert client.dump(with_metadata=False) == io_data_1
        time.sleep(1)
        dump = client.dump(with_metadata=True)
        first_load_timestamps = [
            dateutil.parser.isoparse(obj["_timestamp"])
            for obj_list_name in io_schema.id_fields
            for obj in dump[obj_list_name]
        ]
        before_first_load = min(first_load_timestamps) - \
            datetime.timedelta(microseconds=1)
        latest_first_load = max(first_load_timestamps)
        time.sleep(1)
        client.load(io_data_2)
        dump = client.dump(with_metadata=True)
        second_load_timestamps = [
            dateutil.parser.isoparse(obj["_timestamp"])
            for obj_list_name in io_schema.id_fields
            for obj in dump[obj_list_name]
        ]
        latest_second_load = max(second_load_timestamps)

        # Check both datasets are in the database
        # regardless of the object order
        for dump in [
            client.dump(with_metadata=False),
            client.dump(with_metadata=False,
                        after=None,
                        until=None),
            client.dump(with_metadata=False,
                        after=before_first_load,
                        until=latest_second_load),
        ]:
            for io_data in (io_data_1, io_data_2):
                for obj_list_name in io_schema.id_fields:
                    assert obj_list_name in dump
                    for obj in io_data[obj_list_name]:
                        assert obj in dump[obj_list_name]

        assert client.dump(with_metadata=False,
                           until=latest_first_load) == io_data_1
        assert client.dump(with_metadata=False,
                           after=latest_first_load) == io_data_2
        assert client.dump(with_metadata=False,
                           until=before_first_load) == io_schema.new()
        assert client.dump(with_metadata=False,
                           after=latest_second_load) == io_schema.new()
        time.sleep(1)
        assert client.purge(client.get_current_time())
        assert client.dump() == io_schema.new()
    else:
        now = datetime.datetime.now(datetime.timezone.utc)
        with pytest.raises(kcidb.db.misc.NoTimestamps):
            client.dump(after=now)
        with pytest.raises(kcidb.db.misc.NoTimestamps):
            client.dump(until=now)
        with pytest.raises(kcidb.db.misc.NoTimestamps):
            client.dump(after=now, until=now)
