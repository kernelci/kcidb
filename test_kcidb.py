"""kcidb namespace tests"""

import re
import json
import textwrap
import kcidb
from kcidb.io import SCHEMA
from kcidb.unittest import assert_executes


def test_light_asserts_are_disabled():
    """Check light asserts are disabled"""
    assert not kcidb.io.misc.LIGHT_ASSERTS, \
        "Tests must run with KCIDB_IO_HEAVY_ASSERTS " \
        "environment variable set to a non-empty string"
    assert not kcidb.misc.LIGHT_ASSERTS, \
        "Tests must run with KCIDB_HEAVY_ASSERTS " \
        "environment variable set to a non-empty string"


def test_json_output_options():
    """Check JSON output options work"""
    assert_executes("", "kcidb.schema_main",
                    stdout_re="\\{.*")
    assert_executes("", "kcidb.schema_main", "--seq",
                    stdout_re="\x1e\\{.*")
    assert_executes("", "kcidb.schema_main", "--indent=0",
                    stdout_re="\\{\".*")
    assert_executes("", "kcidb.schema_main", "--indent=4",
                    stdout_re="\\{\n    \".*")
    assert_executes("", "kcidb.schema_main", "--indent=2",
                    stdout_re="\\{\n  \".*")


def test_submit_main():
    """Check kcidb-submit works"""
    driver_source = textwrap.dedent("""
        from unittest.mock import patch, Mock
        with patch("kcidb.mq.Publisher.__init__",
                   return_value=None) as init, \
             patch("kcidb.mq.Publisher.future_publish") as future_publish:
            status = function()
            init.assert_called_once_with("project", "topic")
        return status
    """)
    argv = ["kcidb.submit_main", "-p", "project", "-t", "topic"]

    assert_executes("", *argv, driver_source=driver_source)
    assert_executes('{', *argv, driver_source=driver_source,
                    status=1, stderr_re=".*JSONParseError.*")
    assert_executes('{}', *argv, driver_source=driver_source,
                    status=1, stderr_re=".*ValidationError.*")

    empty = kcidb.io.SCHEMA.new()

    driver_source = textwrap.dedent(f"""
        from unittest.mock import patch, Mock
        future = Mock()
        future.done = lambda: True
        future.add_done_callback = lambda cb: cb(future)
        future.result = Mock(return_value="id")
        with patch("kcidb.mq.Publisher.__init__",
                   return_value=None) as init, \
             patch("kcidb.mq.Publisher.future_publish",
                   return_value=future) as future_publish:
            status = function()
            init.assert_called_once_with("project", "topic")
            future_publish.assert_called_once_with({repr(empty)})
        return status
    """)
    assert_executes(json.dumps(empty), *argv,
                    driver_source=driver_source,
                    stdout_re="id\n")

    driver_source = textwrap.dedent(f"""
        from unittest.mock import patch, Mock, call
        future = Mock()
        future.done = lambda: True
        future.add_done_callback = lambda cb: cb(future)
        future.result = Mock(return_value="id")
        with patch("kcidb.mq.Publisher.__init__",
                   return_value=None) as init, \
             patch("kcidb.mq.Publisher.future_publish",
                   return_value=future) as future_publish:
            status = function()
            init.assert_called_once_with("project", "topic")
            assert future_publish.call_count == 2
            future_publish.assert_has_calls([call({repr(empty)}),
                                             call({repr(empty)})])
        return status
    """)
    assert_executes(json.dumps(empty) + json.dumps(empty), *argv,
                    driver_source=driver_source,
                    stdout_re="id\nid\n")


def test_query_main():
    """Check kcidb-query works"""
    driver_source = textwrap.dedent("""
        from unittest.mock import patch
        with patch("kcidb.Client"):
            return function()
    """)
    argv = ["kcidb.query_main", "-d", "bigquery:project.dataset"]
    assert_executes("", *argv, driver_source=driver_source)

    argv = [
        "kcidb.query_main", "-d", "bigquery:project.dataset",
        "-c", "test:checkout:1", "-b", "test:build:1",
        "-t", "test:test:1",
        "--parents", "--children", "--objects-per-report", "10",
        "--indent=0", "-i", "test:issue:1",
        "-n", "test:incident:1",
    ]
    empty = kcidb.io.SCHEMA.new()
    driver_source = textwrap.dedent(f"""
        from unittest.mock import patch, Mock
        client = Mock()
        client.query_iter = Mock(return_value=iter((
            {repr(empty)}, {repr(empty)},
        )))
        with patch("kcidb.Client", return_value=client) as Client:
            status = function()
        Client.assert_called_once_with(
            database="bigquery:project.dataset"
        )
        client.query_iter.assert_called_once_with(
            ids=dict(checkouts=["test:checkout:1"],
                     builds=["test:build:1"],
                     tests=["test:test:1"],
                     issues=["test:issue:1"],
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


def test_schema_main():
    """Check kcidb-schema works"""
    assert_executes(
            "", "kcidb.schema_main",
            stdout_re=f'.*"const": {SCHEMA.major},.*'
    )
    assert_executes(
            "", "kcidb.schema_main",
            str(SCHEMA.major - 1),
            stdout_re=f'.*"const": {SCHEMA.major - 1},.*'
    )
    assert_executes("",
                    "kcidb.schema_main", "0",
                    status=2,
                    stderr_re=".*Invalid major version number: '0'\n")
    assert_executes("",
                    "kcidb.schema_main",
                    str(SCHEMA.major + 1),
                    status=2,
                    stderr_re=".*No schema version found for major "
                    f"number {SCHEMA.major + 1}\n")


def test_validate_main():
    """Check kcidb-validate works"""
    empty = json.dumps(dict(version=dict(major=SCHEMA.major,
                                         minor=SCHEMA.minor))) + "\n"
    second_empty = json.dumps(dict(version=dict(major=3, minor=0))) + "\n"
    third_empty = json.dumps(dict(version=dict(major=2, minor=0))) + "\n"
    dirty_empty = json.dumps(dict(version=dict(major=3, minor=1))) + "\n"
    valid_list = f"{empty}{second_empty}{third_empty}"
    invalid_list = f"{empty}{dirty_empty}{third_empty}"

    assert_executes('', "kcidb.validate_main")
    assert_executes(empty, "kcidb.merge_main", "--indent=0",
                    stdout_re=re.escape(empty))
    assert_executes(second_empty,
                    "kcidb.validate_main", "--indent=0",
                    stdout_re=re.escape(second_empty))
    assert_executes('{"version":{"major":2,"minor":0}}',
                    "kcidb.validate_main", "1",
                    status=1,
                    stderr_re=".*ValidationError: 1 was expected.*")
    assert_executes('{"version":{"major":4,"minor":0}}',
                    "kcidb.validate_main", "0",
                    status=2,
                    stderr_re=".*Invalid major version number: '0'\n")
    assert_executes('{"version":{"major":4,"minor":0}}',
                    "kcidb.validate_main",
                    str(SCHEMA.major + 1),
                    status=2,
                    stderr_re=".*No schema version found for major "
                    f"number {SCHEMA.major + 1}\n")
    assert_executes('{', "kcidb.validate_main",
                    status=1, stderr_re=".*JSONParseError.*")
    assert_executes('{}', "kcidb.validate_main",
                    status=1, stderr_re=".*ValidationError.*")
    assert_executes(valid_list,
                    "kcidb.validate_main", "--indent=0",
                    stdout_re=re.escape(valid_list))
    assert_executes(invalid_list,
                    "kcidb.validate_main", "--indent=0",
                    status=1, stdout_re=re.escape(empty),
                    stderr_re=".*ValidationError.*")


def test_upgrade_main():
    """Check kcidb-upgrade works"""
    major = SCHEMA.major
    minor = SCHEMA.minor

    prev_version = \
        json.dumps(dict(version=dict(major=major - 1, minor=0))) + "\n"
    latest_version = \
        json.dumps(dict(version=dict(major=major, minor=minor))) + "\n"

    assert_executes('', "kcidb.upgrade_main")
    assert_executes('{', "kcidb.upgrade_main",
                    status=1, stderr_re=".*JSONParseError.*")
    assert_executes('{}', "kcidb.upgrade_main",
                    status=1, stderr_re=".*ValidationError.*")
    assert_executes(latest_version, "kcidb.upgrade_main", "--indent=0",
                    stdout_re=re.escape(latest_version))
    assert_executes(latest_version + latest_version,
                    "kcidb.upgrade_main", "--indent=0",
                    stdout_re=re.escape(latest_version + latest_version))
    assert_executes(prev_version, "kcidb.upgrade_main", "--indent=0",
                    stdout_re=re.escape(latest_version))
    assert_executes(prev_version + prev_version,
                    "kcidb.upgrade_main", "--indent=0",
                    stdout_re=re.escape(latest_version +
                                        latest_version))

    assert_executes(latest_version,
                    "kcidb.upgrade_main", "0",
                    status=2,
                    stderr_re=".*Invalid major version number: '0'\n")
    assert_executes(latest_version,
                    "kcidb.upgrade_main",
                    str(SCHEMA.major + 1),
                    status=2,
                    stderr_re=".*No schema version found for major "
                    f"number {SCHEMA.major + 1}\n")

    assert_executes(latest_version,
                    "kcidb.upgrade_main", str(major - 1),
                    status=1, stderr_re=".*ValidationError.*")

    assert_executes(latest_version, "kcidb.upgrade_main",
                    "--indent=0", str(major),
                    stdout_re=re.escape(latest_version))


def test_count_main():
    """Check kcidb-count works"""
    assert_executes('', "kcidb.count_main")
    assert_executes('{', "kcidb.count_main",
                    status=1, stderr_re=".*JSONParseError.*")
    assert_executes('{}', "kcidb.count_main",
                    status=1, stderr_re=".*ValidationError.*")

    empty = json.dumps(dict(version=dict(major=4, minor=0)))
    assert_executes(empty, "kcidb.count_main", stdout_re="0\n")
    assert_executes(empty + empty, "kcidb.count_main",
                    stdout_re="0\n0\n")

    one_checkout = json.dumps(dict(
        version=dict(major=4, minor=0),
        checkouts=[dict(id="test:1", origin="test")]
    ))
    assert_executes(one_checkout, "kcidb.count_main", stdout_re="1\n")
    assert_executes(one_checkout + one_checkout,
                    "kcidb.count_main", stdout_re="1\n1\n")


def test_merge_main():
    """Check kcidb-merge works"""
    empty = json.dumps(dict(version=dict(major=SCHEMA.major,
                                         minor=SCHEMA.minor))) + "\n"

    assert_executes('', "kcidb.merge_main", "--indent=0",
                    stdout_re=re.escape(empty))
    assert_executes('{', "kcidb.merge_main",
                    status=1, stderr_re=".*JSONParseError.*")
    assert_executes('{}', "kcidb.merge_main",
                    status=1, stderr_re=".*ValidationError.*")
    assert_executes(empty, "kcidb.merge_main", "--indent=0",
                    stdout_re=re.escape(empty))
    assert_executes(empty + empty, "kcidb.merge_main", "--indent=0",
                    stdout_re=re.escape(empty))

    report_a = dict(
        version=dict(major=SCHEMA.major, minor=SCHEMA.minor),
        checkouts=[
            dict(id="test:checkout:1", origin="test")
        ],
        builds=[
            dict(id="test:build:1", origin="text",
                 checkout_id="test:checkout:1")
        ],
        tests=[
            dict(id="test:test:1", origin="text", build_id="test:build:1")
        ]
    )

    report_b = dict(
        version=dict(major=SCHEMA.major, minor=SCHEMA.minor),
        checkouts=[
            dict(id="test:checkout:1", origin="test")
        ],
        builds=[
            dict(id="test:build:2", origin="text",
                 checkout_id="test:checkout:1")
        ],
        tests=[
            dict(id="test:test:2", origin="text", build_id="test:build:2")
        ]
    )
    merged_report = dict(
        version=dict(major=SCHEMA.major, minor=SCHEMA.minor),
        checkouts=report_a["checkouts"] + report_b["checkouts"],
        builds=report_a["builds"] + report_b["builds"],
        tests=report_a["tests"] + report_b["tests"],
    )
    assert_executes(json.dumps(report_a) + json.dumps(report_b),
                    "kcidb.merge_main", "--indent=0",
                    stdout_re=re.escape(
                        json.dumps(merged_report) + "\n"
                    ))


def test_notify_main():
    """Check kcidb-notify works"""
    assert_executes('', "kcidb.notify_main", ">*#")
    assert_executes('{', "kcidb.notify_main", ">*#",
                    status=1, stderr_re=".*JSONParseError.*")
    assert_executes('{}', "kcidb.notify_main", ">*#",
                    status=1, stderr_re=".*ValidationError.*")

    empty = json.dumps(dict(version=dict(major=SCHEMA.major,
                                         minor=SCHEMA.minor)))
    assert_executes(empty, "kcidb.notify_main", ">*#")
    assert_executes(empty + empty, "kcidb.notify_main", ">*#")

    git_commit_hash1 = "4ff6a2469104218a044ff595a0c1eb469ca7ea01"
    git_commit_hash2 = "fe3fc1bc47d6333d7d06bc530c6e0c1044bab536"

    one_of_everything = json.dumps(dict(
        version=dict(major=SCHEMA.major, minor=SCHEMA.minor),
        checkouts=[
            dict(id="test:checkout:1",
                 git_commit_hash=git_commit_hash1,
                 patchset_hash="",
                 origin="test")
        ],
        builds=[
            dict(id="test:build:1",
                 origin="test",
                 checkout_id="test:checkout:1",
                 valid=False)
        ],
        tests=[
            dict(id="test:test:1",
                 origin="test",
                 build_id="test:build:1",
                 waived=False,
                 status="PASS")
        ]
    ))

    assert_executes(one_of_everything, "kcidb.notify_main", ">*#",
                    stdout_re="Subject: Test revision: .*\x00"
                              "Subject: Test checkout: .*\x00"
                              "Subject: Test build: .*\x00"
                              "Subject: Test test: .*\x00")

    two_of_everything = json.dumps(dict(
        version=dict(major=SCHEMA.major, minor=SCHEMA.minor),
        checkouts=[
            dict(id="test:checkout:1",
                 git_commit_hash=git_commit_hash1,
                 patchset_hash="",
                 origin="test"),
            dict(id="test:checkout:2",
                 git_commit_hash=git_commit_hash2,
                 patchset_hash="",
                 origin="test"),
        ],
        builds=[
            dict(id="test:build:1",
                 origin="test",
                 checkout_id="test:checkout:1",
                 valid=False),
            dict(id="test:build:2",
                 origin="test",
                 checkout_id="test:checkout:2",
                 valid=False),
        ],
        tests=[
            dict(id="test:test:1",
                 origin="test",
                 build_id="test:build:1",
                 waived=False,
                 status="PASS"),
            dict(id="test:test:2",
                 origin="test",
                 build_id="test:build:2",
                 waived=False,
                 status="FAIL"),
        ]
    ))

    assert_executes(two_of_everything,
                    "kcidb.notify_main", ">*#",
                    stdout_re="Subject: Test revision: .*\x00"
                              "Subject: Test revision: .*\x00"
                              "Subject: Test checkout: .*\x00"
                              "Subject: Test checkout: .*\x00"
                              "Subject: Test build: .*\x00"
                              "Subject: Test build: .*\x00"
                              "Subject: Test test: .*\x00"
                              "Subject: Test test: .*\x00")


def test_ingest_main():
    """Check kcidb-ingest works"""
    assert_executes('', "kcidb.ingest_main")
    assert_executes('{', "kcidb.ingest_main",
                    status=1, stderr_re=".*JSONParseError.*")
    assert_executes('{}', "kcidb.ingest_main",
                    status=1, stderr_re=".*ValidationError.*")
    empty = json.dumps(dict(version=dict(major=SCHEMA.major,
                                         minor=SCHEMA.minor)))
    assert_executes(empty, "kcidb.ingest_main")
    assert_executes(empty + empty, "kcidb.ingest_main")
    git_commit_hash1 = "4ff6a2469104218a044ff595a0c1eb469ca7ea01"
    git_commit_hash2 = "fe3fc1bc47d6333d7d06bc530c6e0c1044bab536"

    one_of_everything = json.dumps(dict(
        version=dict(major=SCHEMA.major, minor=SCHEMA.minor),
        checkouts=[
            dict(id="test:checkout:1",
                 git_commit_hash=git_commit_hash1,
                 patchset_hash="",
                 origin="test")
        ],
        builds=[
            dict(id="test:build:1",
                 origin="test",
                 checkout_id="test:checkout:1",
                 valid=False)
        ],
        tests=[
            dict(id="test:test:1",
                 origin="test",
                 build_id="test:build:1",
                 waived=False,
                 status="PASS")
        ],
        issues=[
            dict(id="test:issue:1",
                 version=1,
                 origin="test",
                 report_url="https://test.com/bug/1",
                 report_subject="Bug in kernel",
                 culprit=dict(code=True)),
        ],
        incidents=[
            dict(id="test:incident:1",
                 issue_id="test:issue:1",
                 issue_version=1,
                 origin="test",
                 test_id="test:test:1",
                 present=True),
        ],
    ))
    assert_executes(one_of_everything, "kcidb.ingest_main",
                    stdout_re="Subject: Test revision: .*\x00"
                              "Subject: Test checkout: .*\x00"
                              "Subject: Test build: .*\x00"
                              "Subject: Test test: .*\x00"
                              "Subject: Test bug: .*\x00"
                              "Subject: Test issue: .*\x00"
                              "Subject: Test incident: .*\x00")

    two_of_everything = json.dumps(dict(
        version=dict(major=SCHEMA.major, minor=SCHEMA.minor),
        checkouts=[
            dict(id="test:checkout:1",
                 git_commit_hash=git_commit_hash1,
                 patchset_hash="",
                 origin="test"),
            dict(id="test:checkout:2",
                 git_commit_hash=git_commit_hash2,
                 patchset_hash="",
                 origin="test"),
        ],
        builds=[
            dict(id="test:build:1",
                 origin="test",
                 checkout_id="test:checkout:1",
                 valid=False),
            dict(id="test:build:2",
                 origin="test",
                 checkout_id="test:checkout:2",
                 valid=False),
        ],
        tests=[
            dict(id="test:test:1",
                 origin="test",
                 build_id="test:build:1",
                 waived=False,
                 status="PASS"),
            dict(id="test:test:2",
                 origin="test",
                 build_id="test:build:2",
                 waived=False,
                 status="FAIL"),
        ],
        issues=[
            dict(id="test:issue:1",
                 version=1,
                 origin="test",
                 report_url="https://test.com/bug/1",
                 report_subject="Bug in kernel",
                 culprit=dict(code=True)),
            dict(id="test:issue:2",
                 version=1,
                 origin="test",
                 report_url="https://non-test.com/bug/1",
                 report_subject="Another bug in kernel",
                 culprit=dict(tool=True))
        ],
        incidents=[
            dict(id="test:incident:1",
                 issue_id="test:issue:1",
                 issue_version=1,
                 origin="test",
                 test_id="test:test:1",
                 present=True),
            dict(id="test:incident:2",
                 issue_id="test:issue:2",
                 issue_version=1,
                 origin="test",
                 test_id="test:test:2",
                 present=True)
        ],
    ))

    assert_executes(two_of_everything,
                    "kcidb.ingest_main",
                    stdout_re="Subject: Test revision: .*\x00"
                              "Subject: Test revision: .*\x00"
                              "Subject: Test checkout: .*\x00"
                              "Subject: Test checkout: .*\x00"
                              "Subject: Test build: .*\x00"
                              "Subject: Test build: .*\x00"
                              "Subject: Test bug: .*\x00"
                              "Subject: Test bug: .*\x00"
                              "Subject: Test issue: .*\x00"
                              "Subject: Test issue: .*\x00"
                              "Subject: Test incident: .*\x00"
                              "Subject: Test incident: .*\x00")
