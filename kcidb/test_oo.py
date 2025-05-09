"""kcidb.oo test module"""

import sys
import json
import pytest
import kcidb
from kcidb.oo import Checkout, Build, Test, Node, Issue, Incident

# We gotta have our fixtures, pylint: disable=redefined-outer-name


@pytest.fixture
def client(empty_database):
    """OO client for a database with general test data loaded"""
    database = empty_database
    database.load(
        {
            "version": {
                "major": 5,
                "minor": 0
            },
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
                {
                    "id": "_:redhat:5acb9c2a7bc836e9619c65f9"
                          "709de72e90f2efd0",
                    "git_commit_hash": "5acb9c2a7bc836e9619c65f9"
                          "709de72e90f2efd0",
                    "origin": "redhat", "patchset_hash": ""
                },
                {
                    "id": "_:google:bd355732283c23a365f7c55206c03"
                          "85100d1c389", "origin": "google",
                    "git_commit_hash": "bd355732283c23a365f7c55206c038"
                                       "5100d1c389",
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
                {
                    "id": "kernelci:kernelci.org:619c6a30636e8fe042f2efa0",
                    "origin": "kernelci",
                    "checkout_id": "_:kernelci:5acb9c2a7bc836e9e5172"
                                   "bbcd2311499c5b4e5f1"
                },
                {
                    "id": "kernelci:kernelci.org:619c6c8d764403e3d4f2efff",
                    "origin": "kernelci",
                    "checkout_id": "_:kernelci:5acb9c2a7bc836e9e5172"
                                   "bbcd2311499c5b4e5f1"
                },
                {
                    "id": "kernelci:kernelci.org:619c7e1a2e53f8feb4f2efae",
                    "origin": "kernelci",
                    "checkout_id": "_:kernelci:5acb9c2a7bc836e9e5172"
                                   "bbcd2311499c5b4e5f1"
                },
                {
                    "id": "redhat:redhat.org:619c65f9709de72e90f2efd0",
                    "origin": "redhat",
                    "checkout_id": "_:redhat:5acb9c2a7bc836e9619c6"
                                   "5f9709de72e90f2efd0"
                }
            ],
            "tests": [
                {
                    "id": "google:google.org:a19di3j5h67f8d9475f26v11",
                    "build_id": "google:google.org:a1d993c3n4c448b2"
                                "j0l1hbf1",
                    "origin": "google",
                },
                {
                    "start_time": "2021-11-23T03:52:13.660000+00:00",
                    "path": "baseline.login",
                    "status": "PASS",
                    "id": "kernelci:kernelci.org:619c656de1fb4af479f2efaa",
                    "build_id": "kernelci:kernelci.org:619c64b171"
                                "2847eccbf2efac",
                    "origin": "kernelci"
                },
                {
                    "build_id": "redhat:redhat.org:619c65f970"
                                "9de72e90f2efd0",
                    "id": "redhat:redhat.org:b9d8be63bc2abca63165"
                          "de5fd74f0f6d2f0b0d1c",
                    "origin": "redhat",
                    "status": "DONE"
                },
            ]
        }
    )
    return kcidb.oo.Client(database)


@pytest.fixture
def traversing_client(empty_database):
    """OO client for a database with test data for traversing loaded"""
    database = empty_database
    database.load(
        {
            "version": {"major": 5, "minor": 0},
            "checkouts": [
                {
                    "id": "_:valid1",
                    "origin": "kernelci",
                    "git_commit_hash": "5acb9c2a7bc836e9e5172bb"
                                       "cd2311499c5b4e5f1",
                    "patchset_hash": "",
                    "git_repository_branch": "valid1",
                    "git_repository_url": "https://repo_valid",
                    "valid": True
                },
                {

                    "id": "_:valid2",
                    "origin": "redhat",
                    "git_commit_hash": "5acb9c2a7bc836e9e5172bb"
                                       "cd2311499c5b4e5f1",
                    "patchset_hash": "",
                    "git_repository_url": "https://repo_valid",
                    "git_repository_branch": "valid2",
                    "valid": True,

                },
                {
                    "id": "_:invalid", "origin": "google",
                    "git_commit_hash": "5acb9c2a7bc836e9e5172bb"
                                       "cd2311499c5b4e5f1",
                    "patchset_hash": "",
                    "git_repository_branch": "invalid",
                    "git_repository_url": "https://repo_invalid",
                    "valid": False,
                }
            ],
            "builds": [
                {
                    "id": "kernelci:pass1",
                    "origin": "kernelci",
                    "checkout_id": "_:valid1",
                    "architecture": "pass1",
                    "status": "PASS",
                },
                {
                    "id": "kernelci:pass2",
                    "origin": "kernelci",
                    "checkout_id": "_:valid1",
                    "architecture": "pass2",
                    "status": "PASS",
                },
                {
                    "id": "kernelci:fail",
                    "origin": "kernelci",
                    "checkout_id": "_:valid1",
                    "architecture": "fail",
                    "status": "FAIL",
                },
                {
                    "id": "redhat:pass1",
                    "checkout_id": "_:valid2",
                    "origin": "redhat",
                    "status": "PASS",
                    "architecture": "pass1",
                },
                {
                    "id": "redhat:pass2",
                    "checkout_id": "_:valid2",
                    "origin": "redhat",
                    "status": "PASS",
                    "architecture": "pass2",
                },
                {
                    "id": "redhat:fail",
                    "checkout_id": "_:valid2",
                    "origin": "redhat",
                    "status": "FAIL",
                    "architecture": "fail",
                },
            ],
            "tests": [
                {
                    "id": "kernelci:pass1_1",
                    "build_id": "kernelci:pass1",
                    "origin": "kernelci", "status": "PASS", "path": "pass1"
                },
                {
                    "id": "kernelci:pass1_2",
                    "build_id": "kernelci:pass1",
                    "origin": "kernelci", "status": "PASS", "path": "pass2"
                },
                {
                    "id": "kernelci:pass1_3",
                    "build_id": "kernelci:pass1",
                    "origin": "kernelci", "status": "FAIL", "path": "fail",
                },
                {
                    "id": "kernelci:pass2_1",
                    "build_id": "kernelci:pass2",
                    "origin": "kernelci", "status": "PASS", "path": "pass1"
                },
                {
                    "id": "kernelci:pass2_2",
                    "build_id": "kernelci:pass2",
                    "origin": "kernelci", "status": "PASS", "path": "pass2"
                },
                {
                    "id": "kernelci:pass2_3",
                    "build_id": "kernelci:pass2",
                    "origin": "kernelci", "status": "FAIL", "path": "fail"
                },
                {
                    "id": "redhat:pass1_1",
                    "build_id": "redhat:pass1",
                    "origin": "redhat", "status": "PASS", "path": "pass1"
                },
                {
                    "id": "redhat:pass1_2",
                    "build_id": "redhat:pass1",
                    "origin": "redhat", "status": "PASS", "path": "pass2",
                },
                {
                    "id": "redhat:pass1_3",
                    "build_id": "redhat:pass1",
                    "origin": "redhat", "status": "FAIL", "path": "fail",
                },
                {
                    "id": "redhat:pass2_1",
                    "build_id": "redhat:pass2",
                    "origin": "redhat", "status": "PASS", "path": "pass1"
                },
                {
                    "id": "redhat:pass2_2",
                    "build_id": "redhat:pass2",
                    "origin": "redhat", "status": "PASS", "path": "pass2",
                },
                {
                    "id": "redhat:pass2_3",
                    "build_id": "redhat:pass2",
                    "origin": "redhat", "status": "FAIL", "path": "fail",
                },
            ],
            "issues": [
                {
                    "id": "kernelci:1",
                    "origin": "kernelci",
                    "version": 1,
                    "report_url": "https://kernelci.org/issue/1",
                    "report_subject": "Bad issue",
                },
                {
                    "id": "redhat:1",
                    "origin": "redhat",
                    "version": 1,
                    "report_url": "https://bugzilla.redhat.com/1",
                    "report_subject": "Worse issue",
                },
            ],
            "incidents": [
                {
                    "id": "kernelci:pass1_1_1",
                    "origin": "kernelci",
                    "issue_id": "kernelci:1",
                    "issue_version": 1,
                    "present": True,
                    "test_id": "kernelci:pass1_1",
                },
                {
                    "id": "kernelci:pass1_3_1",
                    "origin": "kernelci",
                    "issue_id": "kernelci:1",
                    "issue_version": 1,
                    "present": True,
                    "test_id": "kernelci:pass1_3",
                },
                {
                    "id": "redhat:pass1",
                    "origin": "redhat",
                    "issue_id": "redhat:1",
                    "issue_version": 1,
                    "present": True,
                    "build_id": "redhat:pass1",
                },
                {
                    "id": "redhat:pass2",
                    "origin": "redhat",
                    "issue_id": "redhat:1",
                    "issue_version": 1,
                    "present": True,
                    "build_id": "redhat:pass2",
                },
            ],
        }
    )
    return kcidb.oo.Client(database)


def query_str(client, pattern_string):
    """Run OO client query with a parsed string."""
    return client.query(kcidb.orm.query.Pattern.parse(pattern_string))


def filter_pass(container):
    """
    Filter out the PASS-status items in a container.
    Args:
        container:  A list whose items are object(s) that
                    have "status" property.

    Returns:
        The list of object(s) with status property "PASS".
    """
    assert all(hasattr(i, "status") for i in container)
    return list(filter(
        lambda item: item.status == "PASS",
        container
    ))


def filter_valid(container):
    """
    Filter out the valid items in a container.
    Args:
        container:  A list whose items are object(s) that
                    have "valid" property.

    Returns:
        The list of object(s) with valid property true.
    """
    assert all(hasattr(i, "valid") for i in container)
    return list(filter(
        lambda item: item.valid is True,
        container
    ))


def assert_response_contains(client, pattern_str, types, num_instances):
    """
    Check that the query for the specified pattern string returns
    objects of types with specified names, in specified quantity,
    and nothing else.
    Args:
        client:         The client to query.
        pattern_str:    A string which depicts the object to be retrieved.
        types:          A set of specified schema type name(s).
        num_instances:  The number of instance of object returned for
                        each schema type.
    """
    assert isinstance(pattern_str, str)
    assert isinstance(types, set)
    assert types <= set(kcidb.oo.CLASSES)
    assert isinstance(num_instances, int)

    data = client.query(kcidb.orm.query.Pattern.parse(pattern_str))

    assert set(data) == types
    for type_name, type_objs in data.items():
        assert len(type_objs) == num_instances, \
            f"Invalid number of {type_name} instances"
        for i, obj in enumerate(type_objs):
            assert isinstance(obj, kcidb.oo.CLASSES[type_name]), \
                f"Invalid type of {type_name} #{i}"


def assert_contains(container, contents):
    """
    Check that a container has the specified contents.

    Args:
        container:  The container to check the contents of.
        contents:   The contents pattern to check against. The pattern
                    matches either a list of certain number of objects
                    of particular type, or a dictionary with string keys
                    and values matching specified patterns.

                    A list pattern is a tuple containing the type of each
                    contained instance, and the number of those instances.

                    A dictionary pattern is a dictionary containing string
                    keys for the matched dictionary to have, with values
                    being patterns for the corresponding values in the
                    matched dictionary.
    """
    assert isinstance(container, (list, dict))
    assert isinstance(contents, (tuple, dict))

    if isinstance(contents, tuple):
        assert len(contents) == 2, \
            "Invalid number of element in contents"
        assert len(container) == contents[1], \
            f"Invalid number of {contents[0]} instances"
        for i, obj in enumerate(container):
            assert isinstance(obj, contents[0]), \
                f"Invalid type of {contents[0]} #{i}"

    elif isinstance(contents, dict):
        for key in {**container, **contents}:
            if key not in contents:
                assert key not in container, \
                    f"Key({key}) missing in content."
            if key not in container:
                assert key not in contents, \
                    f"Extra key({key}) found in content."

            assert_contains(container[key], contents[key])


def test_response_zero_object(client):
    """Check that empty object is returned."""
    assert_response_contains(client, "", set(), 0)


def test_response_one_object(client):
    """Check that one object is returned for every type"""

    assert_response_contains(
        client,
        ">test[kernelci:kernelci.org:619c656de1fb4af479f2efaa]#",
        {"test"}, 1
    )

    assert_response_contains(
        client,
        ">build[kernelci:kernelci.org:619c6a30636e8fe042f2efa0]#",
        {"build"}, 1
    )

    assert_response_contains(
        client,
        ">checkout[_:redhat:5acb9c2a7bc836e9619c65f9709de72e90f2efd0]#",
        {"checkout"}, 1
    )

    assert_response_contains(
        client,
        '>revision["5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1", ""]#',
        {"revision"}, 1
    )

    assert_response_contains(
        client,
        '>revision["bd355732283c23a365f7c55206c0385100d1c389", ""]#'
        '>checkout#>build#>test#',
        {"revision", "checkout", "build", "test"}, 1
    )


def test_response_two_object(client):
    """Check that two objects are returned for every type"""

    assert_response_contains(
        client,
        ">test[kernelci:kernelci.org:619c656de1fb4af479f2efaa;"
        "redhat:redhat.org:b9d8be63bc2abca63165de5fd74f0f6d2f0b0d1c]#",
        {"test"}, 2
    )

    assert_response_contains(
        client,
        ">build[redhat:redhat.org:619c65f9709de72e90f2efd0;"
        "kernelci:kernelci.org:619c6c8d764403e3d4f2efff]#",
        {"build"}, 2
    )

    assert_response_contains(
        client,
        ">checkout[_:kernelci:5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1;"
        "_:redhat:5acb9c2a7bc836e9619c65f9709de72e90f2efd0]#",
        {"checkout"}, 2
    )

    assert_response_contains(
        client,
        '>revision["5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1", "";'
        '"bd355732283c23a365f7c55206c0385100d1c389", ""]#',
        {"revision"}, 2
    )

    assert_response_contains(
        client,
        '>revision["bd355732283c23a365f7c55206c0385100d1c389", "";'
        '"5acb9c2a7bc836e9619c65f9709de72e90f2efd0", ""]#'
        '>checkout#>build#>test#',
        {"test", "build", "checkout", "revision"}, 2
    )


def test_traversing_revision_links(traversing_client):
    """Check that revision's links are successfully traversed."""

    revision = query_str(
        traversing_client,
        '>revision["5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1", ""]#'
    )['revision'][0]

    assert_contains(revision.checkouts, (Checkout, 3))
    assert_contains(revision.builds, (Build, 6))
    assert_contains(revision.tests, (Test, 12))

    assert_contains(revision.repo_branch_checkouts, {
        "https://repo_valid": {
            "valid1": (Checkout, 1),
            "valid2": (Checkout, 1),
        },
        "https://repo_invalid": {
            "invalid": (Checkout, 1),
        },
    })
    assert_contains(revision.architecture_status_builds, {
        "fail": {
            "FAIL": (Build, 2),
            "ERROR": (Build, 0),
            "MISS": (Build, 0),
            "PASS": (Build, 0),
            "DONE": (Build, 0),
            "SKIP": (Build, 0),
            None: (Build, 0),
        },
        "pass1": {
            "FAIL": (Build, 0),
            "ERROR": (Build, 0),
            "MISS": (Build, 0),
            "PASS": (Build, 2),
            "DONE": (Build, 0),
            "SKIP": (Build, 0),
            None: (Build, 0)
        },
        "pass2": {
            "FAIL": (Build, 0),
            "ERROR": (Build, 0),
            "MISS": (Build, 0),
            "PASS": (Build, 2),
            "DONE": (Build, 0),
            "SKIP": (Build, 0),
            None: (Build, 0)
        }
    })

    assert not revision.checkouts_valid
    assert revision.builds_status == "FAIL"

    assert isinstance(revision.tests_root, Node)

    assert_contains(revision.issues, (Issue, 2))
    assert {revision.issues[0].id, revision.issues[1].id} == \
        {"redhat:1", "kernelci:1"}

    assert_contains(revision.incidents, (Incident, 4))
    assert {i.id for i in revision.incidents} == \
        {"kernelci:pass1_1_1", "kernelci:pass1_3_1",
         "redhat:pass1", "redhat:pass2"}


def test_traversing_valid_checkout_links(traversing_client):
    """Check that valid checkout links are successfully traversed."""
    checkouts = filter_valid(
        query_str(
            traversing_client,
            '>revision["5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1", ""]'
            '>checkout#'
        )["checkout"]
    )
    assert_contains(checkouts, (Checkout, 2))
    assert checkouts[0].get_parent_id("revision") == \
        checkouts[0].revision.get_id()
    assert_contains(checkouts[0].builds, (Build, 3))
    assert_contains(checkouts[0].tests, (Test, 6))
    assert isinstance(checkouts[0].tests_root, Node)

    assert_contains(checkouts[0].issues, (Issue, 1))
    assert_contains(checkouts[1].issues, (Issue, 1))
    assert {checkouts[0].issues[0].id, checkouts[1].issues[0].id} == \
        {"redhat:1", "kernelci:1"}

    assert_contains(checkouts[0].incidents, (Incident, 2))
    assert_contains(checkouts[1].incidents, (Incident, 2))
    assert {i.id for c in checkouts for i in c.incidents} == \
        {"kernelci:pass1_1_1", "kernelci:pass1_3_1",
         "redhat:pass1", "redhat:pass2"}


def test_traversing_passing_build_links(traversing_client):
    """Check that passing build links are successfully traversed."""
    builds = filter_pass(
        query_str(
            traversing_client,
            '>revision["5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1", ""]'
            '>checkout>build#'
        )["build"]
    )
    assert_contains(builds, (Build, 4))
    assert builds[3].get_parent_id("checkout") == builds[3].checkout.get_id()
    assert_contains(builds[0].tests, (Test, 3))
    assert isinstance(builds[0].tests_root, Node)


def test_traversing_test_links(traversing_client):
    """Check that test links are successfully traversed."""
    tests = query_str(
        traversing_client,
        '>revision["5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1", ""]'
        '>checkout>build>test#'
    )["test"]
    assert_contains(tests, (Test, 12))
    assert tests[2].get_parent_id("build") == tests[2].build.get_id()


def test_traversing_revision_root_test_node(traversing_client):
    """
    Check that valid revision's root test node links are
    successfully traversed.
    """
    revision = query_str(
        traversing_client,
        '>revision["5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1", ""]#'
    )['revision'][0]

    assert revision == revision.tests_root.parent

    assert_contains(
        revision.tests_root.tests,
        (Test, 12)
    )

    path = []
    for key in revision.tests_root.nodes:
        node = revision.tests_root.nodes[key]
        path.append(node.path)

        assert node.parent == revision.tests_root

        assert_contains(node.tests, (Test, 4))
        # Check that each tests have the same path.
        assert len({obj.path for obj in node.tests}) == 1

        assert node.nodes == {}

    assert set(path) == {"pass1", "pass2", "fail"}


@pytest.fixture
def status_client(empty_database):
    """OO client for a database with data for status testing loaded"""
    database = empty_database
    # It's about consistency, pylint: disable=use-dict-literal
    data = {
        "version": {"major": 5, "minor": 0},
        "checkouts": [
            {
                "id": "_:1",
                "origin": "_",
                "git_commit_hash": "5acb9c2a7bc836e9e5172bb"
                                   "cd2311499c5b4e5f1",
                "patchset_hash": "",
                "git_repository_branch": "valid1",
                "git_repository_url": "https://repo_valid",
                "valid": True
            },
        ],
        "builds": [
            {
                "id": f"_:status_{status}_incident_{incident}",
                "origin": "_",
                "checkout_id": "_:1",
                "architecture": "x86_64",
                **fields
            }
            for status, fields in (("none", dict()),
                                   ("fail", dict(status="FAIL")),
                                   ("pass", dict(status="PASS")),
                                   ("error", dict(status="ERROR")))
            for incident in (
                "none", "present_true", "present_false", "present_missing"
            )
        ],
        "tests": [
            {
                "id":
                f"_:status_{status}_incident_{incident}",
                "build_id": "_:status_pass_incident_none",
                "origin": "_",
                "path": "test",
                **fields,
            }
            for status, fields in (
                ("none", dict()),
                ("pass", dict(status="PASS")),
                ("fail", dict(status="FAIL")),
                ("error", dict(status="ERROR")),
            )
            for incident in (
                "none", "present_true", "present_false", "present_missing"
            )
        ],
        "issues": [
            {
                "id": "_:issue",
                "origin": "_",
                "version": 1,
                "report_url": "https://kernelci.org/bug",
            }
        ],
        "incidents": [
            {
                "id":
                f"_:build_status_{build_status}_{present}",
                "origin": "_",
                "issue_id": "_:issue",
                "issue_version": 1,
                "build_id": f"_:status_{build_status}_incident_{present}",
                **fields,
            }
            for build_status in ("none", "fail", "pass", "error")
            for present, fields in (
                ("present_none", dict()),
                ("present_false", dict(present=False)),
                ("present_true", dict(present=True))
            )
        ] + [
            {
                "id":
                f"_:test_status_{test_status}_{present}",
                "origin": "_",
                "issue_id": "_:issue",
                "issue_version": 1,
                "test_id": f"_:status_{test_status}_incident_{present}",
                **fields,
            }
            for test_status in ("none", "pass", "fail", "error")
            for present, fields in (
                ("present_none", dict()),
                ("present_false", dict(present=False)),
                ("present_true", dict(present=True))
            )
        ],
    }
    print("DATA:")
    json.dump(data, sys.stdout, indent=4)
    print("")
    database.load(data)
    return kcidb.oo.Client(database)


def test_status_builds(status_client):
    """Check build status effects"""
    for status_name, status_value in (
        ("none", None),
        ("error", "ERROR"),
        ("fail", "FAIL"),
        ("pass", "PASS"),
    ):
        for present_name in (
            "none", "present_true", "present_false", "present_missing",
        ):
            build_id = f"_:status_{status_name}_incident_{present_name}"
            result = status_client.query(kcidb.orm.query.Pattern.parse(
                ">build%#", [{(build_id,)}]
            ))
            assert set(result.keys()) == {"build"}, \
                "Unexpected object types returned"
            assert len(result["build"]) == 1, \
                "Incorrect number of builds returned " \
                f"for ID {build_id!r}"
            build = result["build"][0]
            assert build.status == status_value, \
                f"Build {build_id!r} has incorrect " \
                f"\"status\" value {build.status!r}"


def test_status_tests(status_client):
    """Check test status effects"""

    for status_name, status_value in (
        ("none", None),
        ("pass", "PASS"),
        ("fail", "FAIL"),
        ("error", "ERROR"),
    ):
        for present_name in (
            "none", "present_true", "present_false", "present_missing",
        ):
            test_id = f"_:status_{status_name}_incident_{present_name}"
            result = status_client.query(kcidb.orm.query.Pattern.parse(
                ">test%#", [{(test_id,)}]
            ))
            assert set(result.keys()) == {"test"}, \
                "Unexpected object types returned"
            assert len(result["test"]) == 1, \
                "Incorrect number of tests returned " \
                f"for ID {test_id!r}"
            test = result["test"][0]
            assert test.status == status_value, \
                f"Test {test_id!r} has incorrect " \
                f"\"status\" value {test.status!r}"
