"""kcdib.db module tests"""

import re
import textwrap
import datetime
import json
from itertools import permutations
import pytest
import kcidb
from kcidb.unittest import assert_executes

# It's OK, pylint: disable=too-many-lines


def test_schemas_main():
    """Check kcidb-db-schemas works"""
    argv = ["kcidb.db.schemas_main", "-d", "sqlite::memory:"]
    assert_executes("", *argv,
                    stdout_re=r"4\.0: 4\.0\n4\.1: 4\.1\n")


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
    # NOTE: Having only one element per data type/list to ensure comparison
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
                    "waived": False
                }],
            },
            oo={
                'revision': [{
                    'contacts': None,
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
                    'git_repository_branch': 'android-mainline',
                    'git_repository_url':
                        'https://android.googlesource.com/kernel/common.git',
                    'id':
                        '_:tuxsuite:f00af9d68ed146b47fdbfe91134fcf04c36e6d78',
                    'log_excerpt': None,
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
                    'log_excerpt': None,
                    'log_url': 'https://storage.tuxsuite.com/public/'
                        'clangbuiltlinux/continuous-integration2/builds/'
                        '2KtyFbORDouvFKy49kQtfgCmcac/build.log',
                    'misc': None,
                    'origin': 'tuxsuite',
                    'output_files': None,
                    'start_time': '2023-01-27T08:27:50.000000+00:00',
                    'valid': True
                }],
                'test': [{
                    'build_id':
                        'syzbot:e716fd2a536671b69625b2536ebe9ede623b93b4',
                    'comment': 'INFO: task hung in ipv6_route_ioctl (2)',
                    'duration': None,
                    'environment_comment': None,
                    'environment_misc': None,
                    'id': 'syzbot:bf7c6406637722a401e0',
                    'log_excerpt': None,
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
                    'waived': False
                }],
                'bug': [],
                'issue': [],
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
                    "contacts": None,
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
                    "git_repository_branch": None,
                    "git_repository_url": None,
                    "id":
                        "_:kernelci:"
                        "5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
                    "log_excerpt": None,
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
                    "log_excerpt": None,
                    "log_url": None,
                    "misc": None,
                    "origin": "google",
                    "output_files": None,
                    "start_time": None,
                    "valid": None,
                }],
                "test": [{
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
                }],
                "bug": [],
                "issue": [],
                "incident": [],
            }
        ),
        kcidb.io.schema.V4_1: dict(
            io={
                "version": {"major": 4, "minor": 1},
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
                    "contacts": None,
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
                    "git_repository_branch": None,
                    "git_repository_url": None,
                    "id":
                        "_:kernelci:"
                        "5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
                    "log_excerpt": None,
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
                    "log_excerpt": None,
                    "log_url": None,
                    "misc": None,
                    "origin": "google",
                    "output_files": None,
                    "start_time": None,
                    "valid": None,
                }],
                "test": [{
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
                }],
                "bug": [{
                    "culprit_code": True,
                    "culprit_tool": False,
                    "culprit_harness": False,
                    "url":
                        "https://bugzilla.redhat.com/show_bug.cgi"
                        "?id=873123",
                    "subject":
                        "(cups-usb-quirks) - usb printer doesn't print "
                        "(usblp0: USB Bidirectional printer dev)",
                }],
                "issue": [{
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
                    "build_valid": None,
                    "test_status": None,
                    "version": 3,
                }],
                "incident": [{
                    "build_id": None,
                    "comment": None,
                    "id": "redhat:2340981234098123409382",
                    "issue_id": "redhat:878234322",
                    "issue_version": 3,
                    "misc": None,
                    "origin": "redhat",
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
            # Upgrade the database to this I/O version
            database.upgrade(io_version)
            # Check upgrade went well
            # You're wrong, pylint: disable=unsubscriptable-object
            assert database.oo_query(kcidb.orm.Pattern.parse(">*#")) == \
                last_params["oo"]
            upgraded_io = io_version.upgrade(last_params["io"])
            assert database.dump() == upgraded_io
            assert database.query(io_version.get_ids(upgraded_io)) == \
                upgraded_io

        # For each data's I/O version and parameters
        for load_io_version, load_params in io_version_params.items():
            # If data's I/O version is newer than database's
            if load_io_version > io_version:
                # Make sure load fails
                with pytest.raises(AssertionError):
                    database.load(load_params["io"])
                continue
            # Make sure the database is initialized and empty
            if database.is_initialized():
                database.empty()
            else:
                database.init(io_version)

            # Find oldest directly-compatible version to upgrade to
            for upgrade_io_version in io_version.history:
                if upgrade_io_version >= load_io_version and \
                        upgrade_io_version.major == io_version.major:
                    break
            else:
                upgrade_io_version = io_version

            # Load (possibly-upgraded) data
            database.load(upgrade_io_version.upgrade(load_params["io"]))

            # Check we can query it in various ways
            upgraded_io = io_version.upgrade(load_params["io"])
            assert database.dump() == upgraded_io
            assert database.query(io_version.get_ids(upgraded_io)) == \
                upgraded_io
            assert database.oo_query(kcidb.orm.Pattern.parse(">*#")) == \
                load_params["oo"]

        # Remeber this I/O version and its parameters for the next round
        last_io_version = io_version
        last_params = params


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
    assert client.query(ids=dict(checkouts=["_:1"]), children=True) in \
        [
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
