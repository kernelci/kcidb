"""kcdib.db module tests"""

import re
import textwrap
import datetime
import json
import pytest
import kcidb
from kcidb.unittest import local_only, assert_executes


@local_only
def test_schemas_main():
    """Check kcidb-db-schemas works"""
    argv = ["kcidb.db.schemas_main", "-d", "sqlite::memory:"]
    assert_executes("", *argv,
                    stdout_re=r"4\.0: 4\.0\n4\.1: 4\.1\n")


@local_only
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


@local_only
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


@local_only
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


@local_only
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


@local_only
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
                     tests=["test:test:1"]),
            parents=True,
            children=True,
            objects_per_report=10
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


@local_only
def test_load_main():
    """Check kcidb-db-load works"""
    driver_source = textwrap.dedent("""
        from unittest.mock import patch, Mock
        from kcidb_io.schema import V4_1
        client = Mock()
        client.get_schema = Mock(return_value=((1, 0), V4_1))
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
        from kcidb_io.schema import V4_1
        client = Mock()
        client.get_schema = Mock(return_value=((1, 0), V4_1))
        client.load = Mock()
        with patch("kcidb.db.Client", return_value=client) as Client:
            status = function()
        Client.assert_called_once_with("bigquery:project.dataset")
        client.load.assert_called_once_with({repr(empty)})
        return status
    """)
    assert_executes(json.dumps(empty), *argv,
                    driver_source=driver_source)

    driver_source = textwrap.dedent(f"""
        from unittest.mock import patch, Mock, call
        from kcidb_io.schema import V4_1
        client = Mock()
        client.get_schema = Mock(return_value=((1, 0), V4_1))
        client.load = Mock()
        with patch("kcidb.db.Client", return_value=client) as Client:
            status = function()
        Client.assert_called_once_with("bigquery:project.dataset")
        assert client.load.call_count == 2
        client.load.assert_has_calls([call({repr(empty)}),
                                      call({repr(empty)})])
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
            git_commit_name="v5.8-rc7-98-g7dc6fd0f3b84",
            git_repository_branch="master",
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
            contacts=[
                "clang-built-linux@googlegroups.com",
                "stable@vger.kernel.org"
            ],
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
            duration=600,
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
            valid=True,
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
            ),
            path="ltp",
            comment="ltp on Lenovo x230",
            log_url="https://example.com/test.log",
            log_excerpt="kernel BUG at net/core/dev.c:2648!\n",
            status="FAIL",
            waived=False,
            start_time="2020-08-14T23:08:07.967000+00:00",
            duration=600,
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
            build_valid=True,
            test_status="FAIL",
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


def test_get_last_modified(empty_database):
    """
    Check get_last_modified() works correctly
    """
    client = empty_database
    timestamp = client.get_last_modified()
    assert timestamp is not None
    assert isinstance(timestamp, datetime.datetime)
    assert timestamp.tzinfo is not None


def test_all_fields(empty_database):
    """
    Check all possible I/O fields can be loaded into and dumped from
    a database.
    """
    io_data = COMPREHENSIVE_IO_DATA
    client = empty_database
    client.load(io_data)
    assert io_data == client.dump()


def test_upgrade(clean_database):
    """
    Test database schema upgrade affects accepted I/O schema, and doesn't
    affect ORM results.
    """
    db_client = clean_database
    db_client.init(kcidb.io.schema.V4_0)
    assert db_client.get_schema()[1] == kcidb.io.schema.V4_0
    assert db_client.dump() == dict(version=dict(major=4, minor=0))

    # NOTE: Having only one element per list to ensure comparison
    v4_0_data = {
        "version": {"major": 4, "minor": 0},
        "checkouts": [
            {
                "id": "_:kernelci:5acb9c2a7bc836e"
                      "9e5172bbcd2311499c5b4e5f1",
                "origin": "kernelci",
                "git_commit_hash": "5acb9c2a7bc836e9e5172bb"
                                   "cd2311499c5b4e5f1",
                "git_commit_name": "v5.15-4077-g5acb9c2a7bc8",
                "patchset_hash": ""
            },
        ],
        "builds": [
            {
                "id": "google:google.org:a1d993c3n4c448b2j0l1hbf1",
                "origin": "google",
                "checkout_id": "_:google:bd355732283c23a365f7c"
                               "55206c0385100d1c389"
            },
        ],
        "tests": [
            {
                "id": "google:google.org:a19di3j5h67f8d9475f26v11",
                "build_id": "google:google.org:a1d993c3n4c448b2"
                            "j0l1hbf1",
                "origin": "google",
            },
        ]
    }
    v4_1_data = {
        "version": {"major": 4, "minor": 1},
        "issues": [
            {
                "id": "redhat:878234322",
                "version": 3,
                "origin": "redhat",
                "report_url":
                    "https://bugzilla.redhat.com/show_bug.cgi?id=873123",
                "report_subject":
                    "(cups-usb-quirks) - usb printer doesn't print "
                    "(usblp0: USB Bidirectional printer dev)",
                "culprit": {
                    "code": True,
                    "tool": False,
                    "harness": False,
                },
                "comment": "Match USB Bidirectional printer dev",
            },
        ],
        "incidents": [
            {
                "id": "redhat:2340981234098123409382",
                "issue_id": "redhat:878234322",
                "issue_version": 3,
                "origin": "redhat",
                "test_id": "google:google.org:a19di3j5h67f8d9475f26v11",
                "present": True,
            },
        ],
    }
    v4_0_oo_data = {
        "revision": [
            {
                "contacts": None,
                "git_commit_hash":
                    "5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
                "git_commit_name":
                    "v5.15-4077-g5acb9c2a7bc8",
                "patchset_files": None,
                "patchset_hash": "",
            }
        ],
        "checkout": [
            {
                "comment": None,
                "git_commit_hash":
                    "5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
                "git_repository_branch": None,
                "git_repository_url": None,
                "id":
                    "_:kernelci:5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
                "log_excerpt": None,
                "log_url": None,
                "message_id": None,
                "misc": None,
                "origin": "kernelci",
                "patchset_hash": "",
                "start_time": None,
                "tree_name": None,
                "valid": None,
            }
        ],
        "build": [
            {
                "architecture": None,
                "checkout_id":
                    "_:google:bd355732283c23a365f7c55206c0385100d1c389",
                "command": None,
                "comment": None,
                "compiler": None,
                "config_name": None,
                "config_url": None,
                "duration": None,
                "id": "google:google.org:a1d993c3n4c448b2j0l1hbf1",
                "input_files": None,
                "log_excerpt": None,
                "log_url": None,
                "misc": None,
                "origin": "google",
                "output_files": None,
                "start_time": None,
                "valid": None,
            }
        ],
        "test": [
            {
                "build_id":
                    "google:google.org:a1d993c3n4c448b2j0l1hbf1",
                "comment": None,
                "duration": None,
                "environment_comment": None,
                "environment_misc": None,
                "id":
                    "google:google.org:a19di3j5h67f8d9475f26v11",
                "log_excerpt": None,
                "log_url": None,
                "misc": None,
                "origin": "google",
                "output_files": None,
                "path": None,
                "start_time": None,
                "status": None,
                "waived": None,
            }
        ],
        "bug": [],
        "issue": [],
        "incident": [],
    }
    v4_1_oo_data = kcidb.misc.merge_dicts(
        v4_0_oo_data,
        bug=[
            {
                "culprit_code": True,
                "culprit_tool": False,
                "culprit_harness": False,
                "url":
                    "https://bugzilla.redhat.com/show_bug.cgi?id=873123",
                "subject":
                    "(cups-usb-quirks) - usb printer doesn't print "
                    "(usblp0: USB Bidirectional printer dev)",
            }
        ],
        issue=[
            {
                "comment": "Match USB Bidirectional printer dev",
                "id": "redhat:878234322",
                "misc": None,
                "origin": "redhat",
                "report_url":
                    "https://bugzilla.redhat.com/show_bug.cgi?id=873123",
                "report_subject":
                    "(cups-usb-quirks) - usb printer doesn't print "
                    "(usblp0: USB Bidirectional printer dev)",
                "culprit_code": True,
                "culprit_tool": False,
                "culprit_harness": False,
                "build_valid": None,
                "test_status": None,
                "version": 3,
            }
        ],
        incident=[
            {
                "build_id": None,
                "comment": None,
                "id": "redhat:2340981234098123409382",
                "issue_id": "redhat:878234322",
                "issue_version": 3,
                "misc": None,
                "origin": "redhat",
                "test_id": "google:google.org:a19di3j5h67f8d9475f26v11",
            }
        ],
    )

    db_client.load(v4_0_data)
    assert db_client.dump() == v4_0_data
    assert db_client.oo_query(kcidb.orm.Pattern.parse(">*#")) == v4_0_oo_data
    with pytest.raises(AssertionError):
        db_client.load(v4_1_data)
    db_client.upgrade(kcidb.io.schema.V4_1)
    assert db_client.get_schema()[1] == kcidb.io.schema.V4_1
    assert db_client.oo_query(kcidb.orm.Pattern.parse(">*#")) == v4_0_oo_data
    # Shouldn't raise an assertion, SHOULDN'T IT? <----
    db_client.load(v4_0_data)
    upgraded_v4_0_data = kcidb.io.schema.V4_1.upgrade(v4_0_data)
    assert db_client.dump() == upgraded_v4_0_data
    db_client.load(v4_1_data)
    merged_data = {**upgraded_v4_0_data, **v4_1_data}
    assert db_client.dump() == merged_data
    assert db_client.oo_query(kcidb.orm.Pattern.parse(">*#")) == v4_1_oo_data


def test_query(empty_database):
    """Test the query() method retrieves objects correctly"""
    client = empty_database
    client.load(dict(
        version=dict(major=4, minor=1),
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
    assert client.query(ids=dict(checkouts=["_:1"]), children=True) == \
        {
            "version": {"major": 4, "minor": 1},
            "checkouts": [
                {"id": "_:1", "origin": "_"}
            ],
            "builds": [
                {"checkout_id": "_:1", "id": "_:1", "origin": "_"}
            ],
            "tests": [
                {"build_id": "_:1", "id": "_:1", "origin": "_"}
            ],
            "incidents": [
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
            ],
        }

    unambigious_result = {
        "version": {"major": 4, "minor": 1},
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
            "version": {"major": 4, "minor": 1},
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
        "version": {"major": 4, "minor": 1},
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


def test_empty(empty_database):
    """Test the empty() method removes all data"""
    io_data = COMPREHENSIVE_IO_DATA
    client = empty_database
    client.load(io_data)
    assert io_data == client.dump()
    client.empty()
    assert kcidb.io.SCHEMA.new() == client.dump()
