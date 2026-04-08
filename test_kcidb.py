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


def test_assertions_enabled():
    """Check python assertions are enabled"""
    assert __debug__, "Tests must run with assertions enabled"


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
    argv = ["kcidb.submit_main"]

    driver_source = textwrap.dedent("""
        import os
        os.environ["KCIDB_REST"] = "https://token@example.com"
        return function()
    """)

    assert_executes("", *argv, driver_source=driver_source)
    assert_executes('{', *argv, driver_source=driver_source,
                    status=1, stderr_re=".*JSONParseError.*")
    assert_executes('{}', *argv, driver_source=driver_source,
                    status=1, stderr_re=".*ValidationError.*")

    empty = kcidb.io.SCHEMA.new()

    driver_source = textwrap.dedent("""
        import os
        from unittest.mock import patch, Mock
        os.environ["KCIDB_REST"] = "https://token@example.com"
        response = Mock()
        response.status_code = 200
        response.json = Mock(return_value={"id": "id"})
        response.text = ""
        with patch("kcidb.requests.post", return_value=response) as post:
            status = function()
            assert post.call_count == 1
        return status
    """)
    assert_executes(json.dumps(empty), *argv,
                    driver_source=driver_source,
                    stdout_re="id\n")

    driver_source = textwrap.dedent("""
        import os
        from unittest.mock import patch, Mock
        os.environ["KCIDB_REST"] = "https://token@example.com"
        response = Mock()
        response.status_code = 200
        response.json = Mock(return_value={"id": "id"})
        response.text = ""
        with patch("kcidb.requests.post", return_value=response) as post:
            status = function()
            assert post.call_count == 2
        return status
    """)
    assert_executes(json.dumps(empty) + json.dumps(empty), *argv,
                    driver_source=driver_source,
                    stdout_re="id\nid\n")

    driver_source = textwrap.dedent("""
        import os
        from unittest.mock import patch, Mock
        from itertools import count
        os.environ["KCIDB_REST"] = "http://localhost:8080/submit"
        counter = count()
        def post(*args, **kwargs):
            response = Mock()
            if next(counter) == 0:
                response.status_code = 500
                response.text = "fail"
                response.json = Mock(return_value={})
                return response
            response.status_code = 200
            response.json = Mock(return_value={"id": "id"})
            response.text = ""
            return response
        with patch("kcidb.requests.post", side_effect=post) as post:
            status = function()
            assert post.call_count == 2
        return status
    """)
    assert_executes(json.dumps(empty) + json.dumps(empty), *argv,
                    driver_source=driver_source,
                    stdout_re="id\n",
                    stderr_re=".*Error submitting report.*",
                    status=1)


def test_rest_uri_validation():
    """Check REST URI validation accepts local tokenless URIs."""
    client = kcidb.Client(rest_uri="http://localhost:8080/submit")
    # pylint: disable=protected-access
    assert client._resturi == "http://localhost:8080/submit"
    try:
        kcidb.Client(rest_uri="https://example.com/submit")
    except ValueError:
        pass
    else:
        raise AssertionError("Expected ValueError for missing token")


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
        "--indent=0", "-i", "test:issue:1", "--iv", "10",
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
                    # jsonschema error wording varies by version; keep loose.
                    stderr_re=".*ValidationError:.*")
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
                 status="FAIL")
        ],
        tests=[
            dict(id="test:test:1",
                 origin="test",
                 build_id="test:build:1",
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
                 status="FAIL"),
            dict(id="test:build:2",
                 origin="test",
                 checkout_id="test:checkout:2",
                 status="FAIL"),
        ],
        tests=[
            dict(id="test:test:1",
                 origin="test",
                 build_id="test:build:1",
                 status="PASS"),
            dict(id="test:test:2",
                 origin="test",
                 build_id="test:build:2",
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
                 status="FAIL")
        ],
        tests=[
            dict(id="test:test:1",
                 origin="test",
                 build_id="test:build:1",
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
                              "Subject: Test issue: .*\x00"
                              "Subject: Test issue_version: .*\x00"
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
                 status="FAIL"),
            dict(id="test:build:2",
                 origin="test",
                 checkout_id="test:checkout:2",
                 status="FAIL"),
        ],
        tests=[
            dict(id="test:test:1",
                 origin="test",
                 build_id="test:build:1",
                 status="PASS"),
            dict(id="test:test:2",
                 origin="test",
                 build_id="test:build:2",
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
                              "Subject: Test issue: .*\x00"
                              "Subject: Test issue: .*\x00"
                              "Subject: Test issue_version: .*\x00"
                              "Subject: Test issue_version: .*\x00"
                              "Subject: Test incident: .*\x00"
                              "Subject: Test incident: .*\x00")
