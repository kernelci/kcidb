"""kcdib.orm module tests"""

# Over 1000 lines, pylint: disable=too-many-lines

import re
from jinja2 import Template
import pytest
import kcidb

# We gotta have our fixtures, pylint: disable=redefined-outer-name


EMPTY_TEMPLATE = Template("")
SCHEMA = kcidb.orm.Schema(
    {},
    dict(
        revision=dict(
            field_json_schemas=dict(
                git_commit_hash=dict(type="string"),
                patchset_hash=dict(type="string"),
            ),
            required_fields=set(),
            id_fields=("git_commit_hash", "patchset_hash"),
            children=dict(
                checkout=("git_commit_hash", "patchset_hash",)
            ),
        ),
        checkout=dict(
            field_json_schemas=dict(
                id=dict(type="string"),
            ),
            required_fields=set(),
            id_fields=("id",),
            children=dict(
                build=("checkout_id",)
            ),
        ),
        build=dict(
            field_json_schemas=dict(
                id=dict(type="string"),
            ),
            required_fields=set(),
            id_fields=("id",),
            children=dict(
                test=("build_id",),
                build_test_environment=("build_id",),
                incident=("build_id",),
            ),
        ),
        test_environment=dict(
            field_json_schemas=dict(
                id=dict(type="string"),
            ),
            required_fields=set(),
            id_fields=("id",),
            children=dict(
                test=("environment_id",),
                build_test_environment=("environment_id",),
            ),
        ),
        build_test_environment=dict(
            field_json_schemas=dict(
                build_id=dict(type="string"),
                environment_id=dict(type="string"),
            ),
            required_fields=set(),
            id_fields=("build_id", "environment_id"),
            children=dict(
                test=("build_id", "environment_id"),
            ),
        ),
        test=dict(
            field_json_schemas=dict(
                id=dict(type="string"),
            ),
            required_fields=set(),
            id_fields=("id",),
            children=dict(
                incident=("test_id",),
            ),
        ),
        bug=dict(
            field_json_schemas=dict(
                url=dict(type="string"),
            ),
            required_fields={'url'},
            id_fields=("url",),
            children=dict(
                issue=("report_url",),
            ),
        ),
        issue=dict(
            field_json_schemas=dict(
                id=dict(type="string"),
                version=dict(type="integer"),
                origin=dict(type="string"),
                report_url=dict(type="string"),
            ),
            required_fields={'id', 'version', 'origin'},
            id_fields=("id",),
            children=dict(
                incident=("issue_id",),
            ),
        ),
        incident=dict(
            field_json_schemas=dict(
                id=dict(type="string"),
                origin=dict(type="string"),
                issue_id=dict(type="string"),
                issue_version=dict(type="integer"),
                build_id=dict(type="string"),
                test_id=dict(type="string"),
            ),
            required_fields={'id', 'origin', 'issue_id', 'issue_version'},
            id_fields=("id",),
        ),
    )
)


def parse(string, obj_id_set_list=None):
    """Parse a pattern string using test schema"""
    return kcidb.orm.Pattern.parse(string, obj_id_set_list,
                                   schema=SCHEMA)


def pattern(base, child, obj_type_name, obj_id_set=None):
    """Create a pattern with test schema"""
    return kcidb.orm.Pattern(base, child,
                             SCHEMA.types[obj_type_name], obj_id_set)


def from_io(io_data, max_objs=0):
    """Create a pattern from I/O with test schema"""
    assert isinstance(max_objs, int) and max_objs >= 0
    return kcidb.orm.Pattern.from_io(io_data, schema=SCHEMA,
                                     max_objs=max_objs)


def test_pattern_parse_misc():
    """Check miscellaneous pattern parsing works"""
    assert parse("") == set()
    assert parse("<*") == set()
    assert parse("<*$") == set()
    assert parse("<*#") == set()
    assert parse(">revision") == set()
    assert parse(">revision$") == {pattern(None, True, "revision")}
    assert parse(">revision#") == {pattern(None, True, "revision")}
    assert parse(">checkout") == set()
    assert parse(">checkout$") == {pattern(None, True, "checkout")}
    assert parse(">checkout#") == {pattern(None, True, "checkout")}
    assert parse(">build") == set()
    assert parse(">build$") == {pattern(None, True, "build")}
    assert parse(">build#") == {pattern(None, True, "build")}
    assert parse(">build_test_environment") == set()
    assert parse(">build_test_environment$") == \
        {pattern(None, True, "build_test_environment")}
    assert parse(">build_test_environment#") == \
        {pattern(None, True, "build_test_environment")}
    assert parse(">test_environment") == set()
    assert parse(">test_environment$") == \
        {pattern(None, True, "test_environment")}
    assert parse(">test_environment#") == \
        {pattern(None, True, "test_environment")}
    assert parse(">test") == set()
    assert parse(">test$") == {pattern(None, True, "test")}
    assert parse(">test#") == {pattern(None, True, "test")}

    assert parse(">revision%", [{("abc", "def")}]) == set()
    assert parse(">revision%$", [{("abc", "def")}]) == \
        {pattern(None, True, "revision", {("abc", "def")})}
    assert parse(">revision%#", [{("abc", "def")}]) == \
        {pattern(None, True, "revision", {("abc", "def")})}
    assert \
        parse(">revision%>checkout>build#", [{("abc", "def")}]) == \
        {pattern(
            pattern(
                pattern(None, True, "revision", {("abc", "def")}),
                True, "checkout"
            ),
            True, "build"
        )}
    assert \
        parse(">revision%>checkout%>build#",
              [{("abc", "def")}, {("123",)}]) == \
        {pattern(
            pattern(
                pattern(None, True, "revision", {("abc", "def")}),
                True, "checkout", {("123",)}
            ),
            True, "build"
        )}
    assert \
        parse(">build>*#") == \
        {
            pattern(
                pattern(None, True, "build"),
                True, "test"
            ),
            pattern(
                pattern(None, True, "build"),
                True, "incident"
            ),
            pattern(
                pattern(None, True, "build"),
                True, "build_test_environment"
            ),
            pattern(
                pattern(
                    pattern(None, True, "build"),
                    True, "build_test_environment"
                ),
                True, "test"
            ),
            pattern(
                pattern(
                    pattern(None, True, "build"),
                    True, "test"
                ),
                True, "incident"
            ),
            pattern(
                pattern(
                    pattern(
                        pattern(None, True, "build"),
                        True, "build_test_environment"
                    ),
                    True, "test"
                ),
                True, "incident"
            ),
        }
    assert \
        parse(">build#>*#") == \
        {
            pattern(None, True, "build"),
            pattern(
                pattern(None, True, "build"),
                True, "test"
            ),
            pattern(
                pattern(None, True, "build"),
                True, "incident"
            ),
            pattern(
                pattern(None, True, "build"),
                True, "build_test_environment"
            ),
            pattern(
                pattern(
                    pattern(None, True, "build"),
                    True, "build_test_environment"
                ),
                True, "test"
            ),
            pattern(
                pattern(
                    pattern(None, True, "build"),
                    True, "test"
                ),
                True, "incident"
            ),
            pattern(
                pattern(
                    pattern(
                        pattern(None, True, "build"),
                        True, "build_test_environment"
                    ),
                    True, "test"
                ),
                True, "incident"
            ),
        }
    assert \
        parse(">build%<*$", [{("abc",)}]) == \
        {
            pattern(
                pattern(
                    pattern(None, True, "build", {("abc",)}),
                    False, "checkout"
                ),
                False, "revision"
            )
        }

    revision_pattern = pattern(
        pattern(
            pattern(None, True, "build", {("abc",)}),
            False, "checkout"
        ),
        False, "revision"
    )
    checkout_pattern = pattern(revision_pattern, True, "checkout")
    build_pattern = pattern(checkout_pattern, True, "build")
    environment_pattern = pattern(
        build_pattern, True, "build_test_environment"
    )
    build_test_pattern = pattern(build_pattern, True, "test")
    environment_test_pattern = pattern(environment_pattern, True, "test")
    environment_test_incident_pattern = pattern(
            environment_test_pattern, True, "incident"
    )
    build_incident_pattern = pattern(build_pattern, True, "incident")
    build_test_incident_pattern = pattern(
        build_test_pattern, True, "incident"
    )
    assert \
        parse(">build%<*$>*#", [{("abc",)}]) == \
        {
            revision_pattern,
            checkout_pattern,
            build_pattern,
            build_incident_pattern,
            build_test_pattern,
            build_test_incident_pattern,
            environment_pattern,
            environment_test_pattern,
            environment_test_incident_pattern,
        }


def test_pattern_parse_id_list():
    """Check pattern inline ID list parsing works"""
    assert parse(">revision[]#") == \
        {pattern(None, True, "revision", set())}
    assert parse(">revision[abc,def]#") == \
        {pattern(None, True, "revision", {("abc", "def")})}
    assert parse(">checkout[123]#") == \
        {pattern(None, True, "checkout", {("123",)})}
    assert \
        parse(">revision[abc, def]>checkout[123]>build#") == \
        {pattern(
            pattern(
                pattern(None, True, "revision", {("abc", "def")}),
                True, "checkout", {("123",)}
            ),
            True, "build"
        )}
    assert parse(">revision[abc,def; ghi, jkl]#") == \
        {pattern(None, True, "revision",
                 {("abc", "def"), ("ghi", "jkl")})}
    assert parse('>checkout["123"]#') == \
        {pattern(None, True, "checkout", {("123",)})}
    assert parse('>checkout["1 2 3"]#') == \
        {pattern(None, True, "checkout", {("1 2 3",)})}
    assert parse('>checkout["1,2;3"]#') == \
        {pattern(None, True, "checkout", {("1,2;3",)})}
    assert parse('>checkout["1\\"2\\"3"]#') == \
        {pattern(None, True, "checkout", {("1\"2\"3",)})}
    assert parse('>checkout["1\\\\2\\\\3"]#') == \
        {pattern(None, True, "checkout", {("1\\2\\3",)})}
    assert parse('>revision["abc","def"; "ghi", "jkl"]#') == \
        {pattern(None, True, "revision",
                 {("abc", "def"), ("ghi", "jkl")})}
    assert \
        parse(' > revision [ "abc" , "def" ; "ghi" , "jkl" ] #') == \
        {pattern(None, True, "revision",
                 {("abc", "def"), ("ghi", "jkl")})}


def test_pattern_parse_trail_discard():
    """Check pattern parsing discards unmatched trail specifications"""
    assert \
        parse(">checkout[123]>build#>test>*") == \
        {
            pattern(
                pattern(None, True, "checkout", {("123",)}),
                True, "build"
            )
        }

    assert \
        parse(">checkout[123]>build$>test>*") == \
        {
            pattern(
                pattern(None, True, "checkout", {("123",)}),
                True, "build"
            )
        }


def test_pattern_parse_failures():
    """Check pattern parsing failures are handled appropriately"""
    with pytest.raises(Exception, match="Failed expanding") as excinfo:
        parse(">foobar")
    exc = excinfo.value.__context__
    assert exc is not None
    assert isinstance(exc, Exception)
    assert re.match("Cannot find type 'foobar'", str(exc))

    with pytest.raises(Exception, match="Failed expanding") as excinfo:
        parse(">revision[abc]")
    exc = excinfo.value.__context__
    assert exc is not None
    assert isinstance(exc, Exception)
    assert re.match("Invalid number of ID fields", str(exc))


def test_pattern_from_io():
    """
    Check Pattern.from_io() works correctly.
    """
    io_data = kcidb.io.SCHEMA.new()
    assert from_io(io_data) == set()

    io_data = {
        "checkouts": [
        ],
        "builds": [
        ],
        "tests": [
        ],
        **kcidb.io.SCHEMA.new()
    }
    assert from_io(io_data) == set()

    io_data = {
        "checkouts": [
            {
                "git_commit_hash":
                "5e29d1443c46b6ca70a4c940a67e8c09f05dcb7e",
                "patchset_hash": "",
                "id": "origin:1",
                "origin": "origin",
            },
        ],
        **kcidb.io.SCHEMA.new()
    }
    assert from_io(io_data) == {
        pattern(None, True, "checkout", {("origin:1",)})
    }

    io_data = {
        "checkouts": [
            {
                "git_commit_hash":
                "5e29d1443c46b6ca70a4c940a67e8c09f05dcb7e",
                "patchset_hash": "",
                "id": "origin:1",
                "origin": "origin",
            },
            {
                "git_commit_hash":
                "5e29d1443c46b6ca70a4c940a67e8c09f05dcb7e",
                "patchset_hash": "",
                "id": "origin:2",
                "origin": "origin",
            },
            {
                "git_commit_hash":
                "5e29d1443c46b6ca70a4c940a67e8c09f05dcb7e",
                "patchset_hash": "",
                "id": "origin:3",
                "origin": "origin",
            },
        ],
        **kcidb.io.SCHEMA.new()
    }
    assert from_io(io_data) == {
        pattern(
            None, True, "checkout",
            {("origin:1",), ("origin:2",), ("origin:3",)}
        )
    }
    assert from_io(io_data, max_objs=3) == {
        pattern(
            None, True, "checkout",
            {("origin:1",), ("origin:2",), ("origin:3",)}
        )
    }
    assert from_io(io_data, max_objs=2) == {
        pattern(None, True, "checkout", {("origin:1",), ("origin:2",)}),
        pattern(None, True, "checkout", {("origin:3",)})
    }
    assert from_io(io_data, max_objs=1) == {
        pattern(None, True, "checkout", {("origin:1",)}),
        pattern(None, True, "checkout", {("origin:2",)}),
        pattern(None, True, "checkout", {("origin:3",)})
    }

    io_data = {
        "builds": [
            {
                "checkout_id": "origin:1",
                "id": "origin:2",
                "origin": "origin",
            },
        ],
        **kcidb.io.SCHEMA.new()
    }
    assert from_io(io_data) == {
        pattern(None, True, "build", {("origin:2",)})
    }

    io_data = {
        "tests": [
            {
                "build_id": "origin:2",
                "id": "origin:3",
                "origin": "origin",
            },
        ],
        **kcidb.io.SCHEMA.new()
    }
    assert from_io(io_data) == {
        pattern(None, True, "test", {("origin:3",)})
    }

    io_data = {
        "checkouts": [
            {
                "git_commit_hash":
                "5e29d1443c46b6ca70a4c940a67e8c09f05dcb7e",
                "patchset_hash": "",
                "id": "origin:1",
                "origin": "origin",
            },
        ],
        "builds": [
            {
                "checkout_id": "origin:1",
                "id": "origin:2",
                "origin": "origin",
            },
        ],
        "tests": [
            {
                "build_id": "origin:2",
                "id": "origin:3",
                "origin": "origin",
            },
        ],
        **kcidb.io.SCHEMA.new()
    }
    assert from_io(io_data) == {
        pattern(None, True, "checkout", {("origin:1",)}),
        pattern(None, True, "build", {("origin:2",)}),
        pattern(None, True, "test", {("origin:3",)}),
    }
    assert from_io(io_data, max_objs=1) == {
        pattern(None, True, "checkout", {("origin:1",)}),
        pattern(None, True, "build", {("origin:2",)}),
        pattern(None, True, "test", {("origin:3",)}),
    }
    assert from_io(io_data, max_objs=2) == {
        pattern(None, True, "checkout", {("origin:1",)}),
        pattern(None, True, "build", {("origin:2",)}),
        pattern(None, True, "test", {("origin:3",)}),
    }

    io_data = {
        "checkouts": [
            {
                "git_commit_hash":
                "5e29d1443c46b6ca70a4c940a67e8c09f05dcb7e",
                "patchset_hash": "",
                "id": "origin:1",
                "origin": "origin",
            },
        ],
        "builds": [
            {
                "checkout_id": "origin:1",
                "id": "origin:2",
                "origin": "origin",
            },
        ],
        "tests": [
            {
                "build_id": "origin:2",
                "id": "origin:3",
                "origin": "origin",
            },
        ],
        "issues": [
            {
                "id": "origin:4",
                "version": 2,
                "origin": "origin",
            },
        ],
        "incidents": [
            {
                "id": "origin:5",
                "origin": "origin",
                "issue_id": "origin:4",
                "issue_version": 2,
            },
        ],
        **kcidb.io.SCHEMA.new()
    }
    assert from_io(io_data) == {
        pattern(None, True, "checkout", {("origin:1",)}),
        pattern(None, True, "build", {("origin:2",)}),
        pattern(None, True, "test", {("origin:3",)}),
        pattern(None, True, "issue", {("origin:4",)}),
        pattern(None, True, "incident", {("origin:5",)}),
    }


def test_pattern_repr():
    """Check various patterns can be converted to strings"""
    assert repr(pattern(None, True, "revision")) == ">revision#"
    assert repr(pattern(None, True, "revision", {("abc", "def")})) == \
        ">revision[abc, def]#"
    assert repr(pattern(None, True, "revision", {("a b c", "def")})) == \
        '>revision["a b c", def]#'
    assert repr(pattern(None, True, "revision", {("a b c", " def ")})) == \
        '>revision["a b c", " def "]#'
    assert repr(pattern(None, True, "revision", {("", "")})) == \
        '>revision["", ""]#'
    assert repr(pattern(None, True, "revision", {("\\", "\"")})) == \
        '>revision["\\\\", "\\""]#'

    assert repr(pattern(None, True, "checkout")) == ">checkout#"
    assert repr(pattern(None, True, "checkout", {("abc",)})) == \
        ">checkout[abc]#"
    assert repr(pattern(None, True, "checkout", {("a b c",)})) == \
        '>checkout["a b c"]#'
    assert repr(pattern(None, True, "checkout", {('"',)})) == \
        '>checkout["\\""]#'
    assert repr(pattern(None, True, "checkout", {('""',)})) == \
        '>checkout["\\"\\""]#'
    assert repr(pattern(None, True, "checkout", {('" "',)})) == \
        '>checkout["\\" \\""]#'
    assert repr(pattern(None, True, "checkout", {(' " " ',)})) == \
        '>checkout[" \\" \\" "]#'
    assert repr(pattern(None, True, "checkout", {('a"b"c',)})) == \
        '>checkout["a\\"b\\"c"]#'
    assert repr(pattern(None, True, "checkout", {("",)})) == \
        ">checkout[\"\"]#"


def raw_data(type_name, **kwargs):
    """
    Generate raw data for a given type_name.
    Args:
        type_name: The schema type name.
        kwargs:    A dictionary containing non-null fields.

    Returns:
        The dictionary for a given type name
         with the missing fields set to None.
    """
    _data = {}
    for item in kcidb.orm.SCHEMA.types[type_name] \
            .json_schema['properties'].keys():
        _data[item] = kwargs.get(item, None)
    return _data


def raw_checkout(**kwargs):
    """Generate raw data for a checkout from values for some fields."""
    return raw_data("checkout", **kwargs)


def raw_build(**kwargs):
    """Generate raw data for a build from values for some fields."""
    return raw_data("build", **kwargs)


def raw_test(**kwargs):
    """Generate raw data for a test from values for some fields."""
    return raw_data("test", **kwargs)


def raw_revision(**kwargs):
    """Generate raw data for a revision from values for some fields."""
    return raw_data("revision", **kwargs)


def raw_bug(**kwargs):
    """Generate raw data for a bug from values for some fields."""
    return raw_data("bug", **kwargs)


def raw_issue(**kwargs):
    """Generate raw data for an issue from values for some fields."""
    return raw_data("issue", **kwargs)


def raw_incident(**kwargs):
    """Generate raw data for an incident from values for some fields."""
    return raw_data("incident", **kwargs)


@pytest.fixture
def source(empty_database):
    """An ORM source with data loaded"""
    source = empty_database
    source.load({
        "version": {"major": 4, "minor": 1},
        "checkouts": [
            {
                "id": "_:kernelci:5acb9c2a7bc836e"
                      "9e5172bbcd2311499c5b4e5f1",
                "origin": "kernelci", "patchset_hash": "",
                "git_commit_hash": "5acb9c2a7bc836e9e5172bb"
                                   "cd2311499c5b4e5f1",
                "git_commit_name": "v5.15-4077-g5acb9c2a7bc8",
            },
            {
                "id": "_:kernelci:82bcf49e5e6ad570ff61ffcd"
                      "210cf85c5ec8d896",
                "git_commit_hash": "82bcf49e5e6ad570ff61ffcd"
                                   "210cf85c5ec8d896",
                "origin": "kernelci", "patchset_hash": ""
            },
            {
                "id": "_:redhat:5acb9c2a7bc836e9619c65f9"
                      "709de72e90f2efd0",
                "origin": "redhat", "patchset_hash": ""
            }
        ],
        "builds": [
            {
                "id": "kernelci:kernelci.org:619c64b1712847eccbf2efac",
                "origin": "kernelci",
                "checkout_id": "_:kernelci:82bcf49e5e6ad57"
                               "0ff61ffcd210cf85c5ec8d896"
            },
            {
                "id": "kernelci:kernelci.org:619c65d3c1b0a764f3f2efa0",
                "origin": "kernelci",
                "checkout_id": "_:kernelci:5acb9c2a7bc836e9e"
                               "5172bbcd2311499c5b4e5f1"
            },
            {
                "id": "kernelci:kernelci.org:619c65f9709de72e90f2efd0",
                "origin": "kernelci",
                "checkout_id": "_:kernelci:5acb9c2a7bc836e9e5"
                               "172bbcd2311499c5b4e5f1"
            },
            {
                "id": "kernelci:kernelci.org:619c6605238f699505f2efa2",
                "origin": "kernelci",
                "checkout_id": "_:kernelci:5acb9c2a7bc836e9e51"
                               "72bbcd2311499c5b4e5f1"
            },
            {
                "id": "kernelci:kernelci.org:619c6612238f699505f2efa8",
                "origin": "kernelci",
                "checkout_id": "_:kernelci:5acb9c2a7bc836e9e517"
                               "2bbcd2311499c5b4e5f1"
            },
            {
                "id": "kernelci:kernelci.org:619c668f584ce0bc17f2efae",
                "origin": "kernelci",
                "checkout_id": "_:kernelci:5acb9c2a7bc836e9e517"
                               "2bbcd2311499c5b4e5f1"
            },
            {
                "id": "kernelci:kernelci.org:619c6a30636e8fe042f2efa0",
                "origin": "kernelci",
                "checkout_id": "_:kernelci:5acb9c2a7bc836e9e5172"
                               "bbcd2311499c5b4e5f1"
            },
            {
                "id": "kernelci:kernelci.org:619c6c73764403e3d4f2eff7",
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
                "id": "kernelci:kernelci.org:619c6cac96505c9d6df2efb0",
                "origin": "kernelci",
                "checkout_id": "_:kernelci:5acb9c2a7bc836e9e5172"
                               "bbcd2311499c5b4e5f1"
            },
            {
                "id": "kernelci:kernelci.org:619c6cba183ad89b53f2efaf",
                "origin": "kernelci",
                "checkout_id": "_:kernelci:5acb9c2a7bc836e9e5172"
                               "bbcd2311499c5b4e5f1"
            },
            {
                "id": "kernelci:kernelci.org:619c6ceb1cfc11e17af2efdb",
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
                "id": "kernelci:kernelci.org:619c814ebd9d22e42ef2efa8",
                "origin": "kernelci",
                "checkout_id": "_:kernelci:5acb9c2a7bc836e9e5172"
                               "bbcd2311499c5b4e5f1"
            },
            {
                "id": "kernelci:kernelci.org:619c8e4eaf90a4a9e3f2efa0",
                "origin": "kernelci",
                "checkout_id": "_:kernelci:5acb9c2a7bc836e9e5172"
                               "bbcd2311499c5b4e5f1"
            },
            {
                "id": "kernelci:kernelci.org:619ca12f3ced3b6990f2efad",
                "origin": "kernelci",
                "checkout_id": "_:kernelci:5acb9c2a7bc836e9e5172"
                               "bbcd2311499c5b4e5f1"
            },
            {
                "id": "kernelci:kernelci.org:619cd8691bef90a32bf2efa1",
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
                "waived": False,
                "start_time": "2021-11-23T03:52:13.660000+00:00",
                "path": "baseline.login", "status": "PASS",
                "id": "kernelci:kernelci.org:619c656de1fb4af479f2efaa",
                "build_id": "kernelci:kernelci.org:619c64b171"
                            "2847eccbf2efac", "origin": "kernelci"
            },
            {
                "waived": False,
                "start_time": "2021-11-23T03:52:13.666000+00:00",
                "path": "baseline.dmesg.crit", "status": "PASS",
                "id": "kernelci:kernelci.org:619c656de1fb4af479f2efac",
                "build_id": "kernelci:kernelci.org:619c64b171"
                            "2847eccbf2efac", "origin": "kernelci"
            },
            {
                "waived": False,
                "start_time": "2021-11-23T03:52:13.668000+00:00",
                "path": "baseline.dmesg.alert", "status": "PASS",
                "id": "kernelci:kernelci.org:619c656de1fb4af479f2efad",
                "build_id": "kernelci:kernelci.org:619c64b171"
                            "2847eccbf2efac", "origin": "kernelci"
            },
            {
                "waived": False,
                "start_time": "2021-11-23T03:52:13.671000+00:00",
                "path": "baseline.dmesg.emerg", "status": "PASS",
                "id": "kernelci:kernelci.org:619c656de1fb4af479f2efae",
                "build_id": "kernelci:kernelci.org:619c64b171"
                            "2847eccbf2efac", "origin": "kernelci"
            },
            {
                "build_id": "kernelci:kernelci.org:619c65f9709"
                            "de72e90f2efd0",
                "id": "kernelci:kernelci.org:b9d8be63bc2abca6316"
                      "5de5fd74f0f6d2f0b0d1c", "origin": "kernelci"
            },
            {
                "build_id": "redhat:redhat.org:619c65f970"
                            "9de72e90f2efd0",
                "id": "redhat:redhat.org:b9d8be63bc2abca63165"
                      "de5fd74f0f6d2f0b0d1c",
                "origin": "redhat", "waived": True, "status": "DONE"
            },
            {
                "build_id": "kernelci:kernelci.org:619c65d3c1"
                            "b0a764f3f2efa0",
                "id": "kernelci:kernelci.org:b9d8be63bc2abca63165"
                      "de5fd74f0f6d2f0b0e2b",
                "origin": "kernelci", "status": "PASS", "waived": False,
                "start_time": "2021-11-23T03:52:13.666000+00:00",
                "path": "baseline.dmesg.crit"
            }
        ],
        "issues": [
            {
                "id": "redhat:987987da98798f987c",
                "origin": "redhat",
                "version": 80,
                "report_url": "https://bugzilla/207065",
                "report_subject": "Plintel doesn't plint",
            },
            {
                "id": "redhat:987987da98798f987c",
                "origin": "redhat",
                "version": 100,
                "report_url": "https://bugzilla/207065",
                "report_subject": "Printer doesn't print",
            },
            {
                "id": "kernelci:1987934987",
                "origin": "kernelci",
                "version": 1,
                "report_url": "https://bugzilla/207065",
                "report_subject": "Printer doesn't print",
            },
            {
                "id": "redhat:987987da98798aa233",
                "origin": "redhat",
                "version": 10,
                "report_url": "https://bugzilla/1201011",
                "report_subject": "Compiler compiles wrong",
            },
            {
                "id": "kernelci:1209203344",
                "origin": "kernelci",
                "version": 0,
                "report_url": "https://maillist/498232",
                "report_subject": "LED boesn't link",
            },
            {
                "id": "kernelci:1209203344",
                "origin": "kernelci",
                "version": 1,
                "report_url": "https://maillist/498232",
                "report_subject": "LED doesn't blink",
            },
        ],
        "incidents": [
            {
                "id": "redhat:987987da98798aa233-12",
                "origin": "redhat",
                "issue_id": "redhat:987987da98798aa233",
                "issue_version": 10,
                "build_id": "redhat:redhat.org:619c65f9709de72e90f2efd0",
                "test_id": "redhat:redhat.org:b9d8be63bc2abca63165"
                "de5fd74f0f6d2f0b0d1c",
                "present": True,
            },
            {
                "id": "redhat:987987da98798f987c-29874",
                "origin": "redhat",
                "issue_id": "redhat:987987da98798f987c",
                "issue_version": 100,
                "test_id": "redhat:redhat.org:b9d8be63bc2abca63165"
                "de5fd74f0f6d2f0b0d1c",
                "present": True,
            },
            {
                "id": "_:2987d298712",
                "origin": "_",
                "issue_id": "kernelci:1987934987",
                "issue_version": 1,
                "test_id": "redhat:redhat.org:b9d8be63bc2abca63165"
                "de5fd74f0f6d2f0b0d1c",
                "present": True,
            },
            {
                "id": "kernelci:29871398212",
                "origin": "kernelci",
                "issue_id": "kernelci:1209203344",
                "issue_version": 1,
                "test_id": "kernelci:kernelci.org:b9d8be63bc2abca63165"
                "de5fd74f0f6d2f0b0e2b",
                "present": True,
            },
            {
                "id": "kernelci:29871398232",
                "origin": "kernelci",
                "issue_id": "kernelci:1209203344",
                "issue_version": 1,
                "test_id": "kernelci:kernelci.org:"
                "619c656de1fb4af479f2efae",
                "present": True,
            },
            {
                "id": "_:908812340982340",
                "origin": "_",
                "issue_id": "kernelci:1209203344",
                "issue_version": 0,
                "test_id": "redhat:redhat.org:b9d8be63bc2abca63165"
                "de5fd74f0f6d2f0b0d1c",
                "present": True,
            },
            {
                "id": "_:908812340982345",
                "origin": "_",
                "issue_id": "kernelci:1209203344",
                "issue_version": 1,
                "test_id": "redhat:redhat.org:b9d8be63bc2abca63165"
                "de5fd74f0f6d2f0b0d1c",
                "present": False,
            },
        ],
    })
    return source


def query_str(source, pattern_string):
    """Run OO query with a parsed string."""
    return source.oo_query(
        kcidb.orm.Pattern.parse(pattern_string)
    )


def test_run(source):
    """Check data returned from query that starts with '>test' """

    assert query_str(
        source,
        ">test[kernelci:kernelci.org:619c656def14baf479f2efac]#"
    ) == {"test": []}

    assert query_str(
        source,
        ">test[kernelci:kernelci.org:619c656de1fb4af479f2efac]"
    ) == {}

    test_of_origin_kernelci_and_redhat = query_str(
        source,
        ">test[kernelci:kernelci.org:619c656de1fb4af479f2efac;"
        "redhat:redhat.org:b9d8be63bc2abca63165de5fd74f0f6d2f0b0d1c]#")
    assert \
        raw_test(
            id="kernelci:kernelci.org:619c656de1fb4af479f2efac",
            build_id="kernelci:kernelci.org:619c64"
                     "b1712847eccbf2efac",
            origin="kernelci",
            waived=False,
            start_time="2021-11-23T03:52:13.666000+00:00",
            path="baseline.dmesg.crit",
            status="PASS") in \
        test_of_origin_kernelci_and_redhat["test"]
    assert \
        raw_test(
            id="kernelci:kernelci.org:619c656de1fb4af479f2efac",
            build_id="kernelci:kernelci.org:619c64b171"
                     "2847eccbf2efac",
            origin="kernelci",
            waived=False,
            start_time="2021-11-23T03:52:13.666000+00:00",
            path="baseline.dmesg.crit",
            status="PASS") in \
        test_of_origin_kernelci_and_redhat["test"]

    assert query_str(
        source,
        '>test[kernelci:kernelci.org:619c656de1fb4af479f2efaa]<build#'
    ) == {
            "build": [
                raw_build(
                    id="kernelci:kernelci.org:619c64b1712847eccbf2efac",
                    git_commit_hash="82bcf49e5e6ad570ff61ffcd210c"
                                    "f85c5ec8d896",
                    origin="kernelci",
                    checkout_id="_:kernelci:82bcf49e5e6ad57"
                                "0ff61ffcd210cf85c5ec8d896")
            ]
    }

    assert len(query_str(
        source,
        ">test[kernelci:kernelci.org:619c656de1fb4af479f2efaa]#<build#"
    )) == 2

    checkout_of_build_of_test_all_inclusive = query_str(
        source,
        ">test[kernelci:kernelci.org:619c656de1fb4af479f2efaa]#"
        "<build#<checkout#")
    assert len(checkout_of_build_of_test_all_inclusive) == 3
    assert \
        checkout_of_build_of_test_all_inclusive["checkout"] == \
        [
            raw_checkout(
                id="_:kernelci:82bcf49e5e6ad570ff61ffcd210cf85c5ec8d896",
                git_commit_hash="82bcf49e5e6ad570ff61ffcd210cf85c5ec8d896",
                patchset_hash="",
                origin="kernelci")
        ]

    assert query_str(
        source,
        ">test[redhat:redhat.org:b9d8be63bc2abca63165de5fd74f0f6d2f0b0d1c]"
        "<build<checkout#"
    ) == {
        "checkout": [
            raw_checkout(
                id="_:redhat:5acb9c2a7bc836e9619c65f9709de72e90f2efd0",
                origin="redhat",
                patchset_hash="")
        ]
    }

    checkout_of_build_of_test_dollar_sign = query_str(
        source,
        ">test[kernelci:kernelci.org:619c656de1fb4af479f2efaa]<build"
        "<checkout$")
    assert len(checkout_of_build_of_test_dollar_sign) == 1
    assert \
        checkout_of_build_of_test_dollar_sign["checkout"] == \
        checkout_of_build_of_test_all_inclusive["checkout"]

    assert query_str(
        source,
        ">test[kernelci:kernelci.org:619c656de1fb4af479f2efaa]<build"
        "<checkout<revision#"
    ) == {
        "revision": [
            raw_revision(
                git_commit_hash="82bcf49e5e6ad570ff61ffcd21"
                                "0cf85c5ec8d896",
                patchset_hash="",
                origin="kernelci")
        ]
    }

    bug_x = raw_bug(
        url="https://bugzilla/1201011",
        subject="Compiler compiles wrong",
    )
    bug_y = raw_bug(
        url="https://bugzilla/207065",
        subject="Printer doesn't print",
    )
    assert \
        query_str(
            source,
            '>test["redhat:redhat.org:'
            'b9d8be63bc2abca63165de5fd74f0f6d2f0b0d1c"]'
            '>incident<issue<bug#'
        ) in [
            dict(bug=[bug_x, bug_y]),
            dict(bug=[bug_y, bug_x]),
        ]


def test_build(source):
    """Check data returned from query that starts with '>build' """

    assert query_str(
        source,
        ">build[kernelci:kernelci.org:916c6a30636e8fe042f2efa0]#"
    ) == {"build": []}

    assert query_str(
        source,
        ">build[kernelci:kernelci.org:619c6a30636e8fe042f2efa0]"
    ) == {}

    assert query_str(
        source,
        ">build[kernelci:kernelci.org:619c6a30636e8fe042f2efa0]#"
    ) == {
        "build": [
            raw_build(
                checkout_id="_:kernelci:5acb9c2a7bc836e9e5172bb"
                            "cd2311499c5b4e5f1",
                id="kernelci:kernelci.org:619c6a30636e8fe042f2efa0",
                origin="kernelci")
        ]
    }

    assert query_str(
        source,
        ">build[kernelci:kernelci.org:619c65d3c1b0a764f3f2efa0]>test#"
    ) == {
        "test": [
            raw_test(
                build_id="kernelci:kernelci.org:619c65d3c1b0a764f3f2efa0",
                id="kernelci:kernelci.org:"
                   "b9d8be63bc2abca63165de5fd74f0f6d2f0b0e2b",
                origin="kernelci",
                waived=False,
                status="PASS",
                start_time="2021-11-23T03:52:13.666000+00:00",
                path="baseline.dmesg.crit")
        ]
    }

    assert query_str(
        source,
        '>build[redhat:redhat.org:619c65f9709de72e90f2efd0]>test#'
    ) == {
        "test": [
            raw_test(
                build_id="redhat:redhat.org:619c65f9709de72e90f2efd0",
                id="redhat:redhat.org:b9d8be63bc"
                   "2abca63165de5fd74f0f6d2f0b0d1c",
                origin="redhat",
                status="DONE",
                waived=True)
        ]
    }

    assert query_str(
        source,
        '>build[redhat:redhat.org:619c65f9709de72e90f2efd0]<checkout#'
    ) == {
        "checkout": [
            raw_checkout(
                id="_:redhat:5acb9c2a7bc836e9619c65f9709de72e90f2efd0",
                patchset_hash="",
                origin="redhat")
        ]
    }

    assert query_str(
        source,
        '>build[kernelci:kernelci.org:619c64b1712847eccbf2efac]'
        '<checkout<revision#'
    ) == {
        "revision": [
            raw_revision(
                git_commit_hash="82bcf49e5e6ad570ff61ffcd210cf85c5ec8d896",
                patchset_hash="",
                origin="kernelci")
        ]
    }

    assert \
        query_str(
            source,
            '>build["redhat:redhat.org:619c65f9709de72e90f2efd0"]'
            '>incident<issue<bug#'
        ) == \
        dict(bug=[raw_bug(
            url="https://bugzilla/1201011",
            subject="Compiler compiles wrong",
        )])


def test_checkout(source):
    """Check data returned from query that starts with '>checkout' """

    assert query_str(
        source,
        ">checkout[_:kernelci:5acb9c2a7bce6839e5172bbcd2311499c5b4e5f1]#"
    ) == {"checkout": []}

    assert query_str(
        source,
        ">checkout[_:kernelci:5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1]"
    ) == {}

    assert query_str(
        source,
        ">checkout[_:kernelci:5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1]#"
    ) == {
        "checkout": [
            raw_checkout(
                id="_:kernelci:5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
                git_commit_hash="5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
                patchset_hash="",
                origin="kernelci"
            )
        ]
    }

    assert query_str(
        source,
        ">checkout[_:kernelci:5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1]"
        "<revision#"
    ) == {
        "revision": [
            raw_revision(
                git_commit_hash="5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
                patchset_hash="",
                origin="kernelci",
                git_commit_name="v5.15-4077-g5acb9c2a7bc8"
            )
        ]
    }

    builds = query_str(
        source,
        ">checkout[_:kernelci:5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1]"
        ">build#"
    )

    compare_builds = list(map(lambda x: raw_build(**x), [
        {
            "id": "kernelci:kernelci.org:619c65d3c1b0a764f3f2efa0",
            "checkout_id": "_:kernelci:"
                           "5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
            "origin": "kernelci",
        },
        {
            "id": "kernelci:kernelci.org:619c65f9709de72e90f2efd0",
            "checkout_id": "_:kernelci:"
                           "5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
            "origin": "kernelci",
        },
        {
            "id": "kernelci:kernelci.org:619c6605238f699505f2efa2",
            "checkout_id": "_:kernelci:"
                           "5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
            "origin": "kernelci",
        },
        {
            "id": "kernelci:kernelci.org:619c6612238f699505f2efa8",
            "checkout_id": "_:kernelci:"
                           "5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
            "origin": "kernelci",
        },
        {
            "id": "kernelci:kernelci.org:619c668f584ce0bc17f2efae",
            "checkout_id": "_:kernelci:"
                           "5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
            "origin": "kernelci",
        },
        {
            "id": "kernelci:kernelci.org:619c6a30636e8fe042f2efa0",
            "checkout_id": "_:kernelci:"
                           "5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
            "origin": "kernelci",
        },
        {
            "id": "kernelci:kernelci.org:619c6c73764403e3d4f2eff7",
            "checkout_id": "_:kernelci:"
                           "5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
            "origin": "kernelci",
        },
        {
            "id": "kernelci:kernelci.org:619c6c8d764403e3d4f2efff",
            "checkout_id": "_:kernelci:"
                           "5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
            "origin": "kernelci",
        },
        {
            "id": "kernelci:kernelci.org:619c6cac96505c9d6df2efb0",
            "checkout_id": "_:kernelci:"
                           "5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
            "origin": "kernelci",
        },
        {
            "id": "kernelci:kernelci.org:619c6cba183ad89b53f2efaf",
            "checkout_id": "_:kernelci:"
                           "5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
            "origin": "kernelci",
        },
        {
            "id": "kernelci:kernelci.org:619c6ceb1cfc11e17af2efdb",
            "checkout_id": "_:kernelci:"
                           "5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
            "origin": "kernelci",
        },
        {
            "id": "kernelci:kernelci.org:619c7e1a2e53f8feb4f2efae",
            "checkout_id": "_:kernelci:"
                           "5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
            "origin": "kernelci",
        },
        {
            "id": "kernelci:kernelci.org:619c814ebd9d22e42ef2efa8",
            "checkout_id": "_:kernelci:"
                           "5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
            "origin": "kernelci",
        },
        {
            "id": "kernelci:kernelci.org:619c8e4eaf90a4a9e3f2efa0",
            "checkout_id": "_:kernelci:"
                           "5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
            "origin": "kernelci",
        },
        {
            "id": "kernelci:kernelci.org:619ca12f3ced3b6990f2efad",
            "checkout_id": "_:kernelci:"
                           "5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
            "origin": "kernelci",
        },
        {
            "id": "kernelci:kernelci.org:619cd8691bef90a32bf2efa1",
            "checkout_id": "_:kernelci:"
                           "5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
            "origin": "kernelci",
        }
    ]))

    for build in builds["build"]:
        assert build in compare_builds
    for build in compare_builds:
        assert build in builds["build"]

    test_of_build_of_revision_dollar_sign = query_str(
        source,
        ">checkout[_:kernelci:5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1]"
        ">build>test$")

    assert \
        raw_test(
            build_id="kernelci:kernelci.org:619c65f9709de72e90f2efd0",
            id="kernelci:kernelci.org:"
               "b9d8be63bc2abca63165de5fd74f0f6d2f0b0d1c",
            origin="kernelci",
        ) in \
        test_of_build_of_revision_dollar_sign['test']
    assert \
        raw_test(
            build_id="kernelci:kernelci.org:619c65d3c1b0a764f3f2efa0",
            id="kernelci:kernelci.org:"
               "b9d8be63bc2abca63165de5fd74f0f6d2f0b0e2b",
            status="PASS", waived=False, origin="kernelci",
            start_time="2021-11-23T03:52:13.666000+00:00",
            path="baseline.dmesg.crit",
        ) in \
        test_of_build_of_revision_dollar_sign["test"]
    tests = query_str(
        source,
        ">checkout[_:kernelci:5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1]"
        ">build>test#")
    assert len(tests["test"]) == 2


def test_revision(source):
    """Check data returned from query that starts with '>revision' """

    assert query_str(
        source,
        '>revision["5acb9c2a7bc0d9ff75172bbcd2311499c5b4e5f1", ""]#'
    ) == {"revision": []}

    assert query_str(
        source,
        '>revision["5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1", ""]'
    ) == {}

    assert query_str(
        source,
        '>revision["5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1", ""]#'
    ) == {
        "revision": [
            raw_revision(
                git_commit_hash="5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
                patchset_hash="",
                git_commit_name="v5.15-4077-g5acb9c2a7bc8")
        ]
    }

    assert query_str(
        source,
        '>revision["5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1", ""]'
        '>checkout#'
    ) == {
        "checkout": [
            raw_checkout(
                id="_:kernelci:5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
                git_commit_hash="5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
                patchset_hash="",
                origin="kernelci")
        ]
    }

    assert query_str(
        source,
        '>revision["82bcf49e5e6ad570ff61ffcd210cf85c5ec8d896", ""]'
        '>checkout>build#'
    ) == {
        "build": [
            raw_build(
                id="kernelci:kernelci.org:619c64b1712847eccbf2efac",
                origin="kernelci",
                checkout_id="_:kernelci:82bcf49e5e6ad57"
                            "0ff61ffcd210cf85c5ec8d896")
        ]
    }

    test_of_revision_dollar_sign = query_str(
        source,
        '>revision["5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1", ""]'
        '>checkout>build>test$')["test"]

    assert \
        raw_test(
            build_id="kernelci:kernelci.org:619c65f9709de72e90f2efd0",
            id="kernelci:kernelci.org:"
               "b9d8be63bc2abca63165de5fd74f0f6d2f0b0d1c",
            origin="kernelci") in \
        test_of_revision_dollar_sign
    assert \
        raw_test(
            build_id="kernelci:kernelci.org:619c65d3c1b0a764f3f2efa0",
            id="kernelci:kernelci.org:"
               "b9d8be63bc2abca63165de5fd74f0f6d2f0b0e2b",
            origin="kernelci",
            status="PASS",
            waived=False,
            start_time="2021-11-23T03:52:13.666000+00:00",
            path="baseline.dmesg.crit") in \
        test_of_revision_dollar_sign

    assert query_str(
        source,
        '>revision["5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1", ""]'
        '>checkout>build>test#'
    )["test"] == test_of_revision_dollar_sign

    assert \
        query_str(
            source,
            '>revision["5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1", ""]'
            '>checkout>build>test>incident<issue<bug#'
        ) == \
        dict(bug=[raw_bug(
            url="https://maillist/498232",
            subject="LED doesn't blink",
        )])

    assert \
        query_str(
            source,
            '>revision["82bcf49e5e6ad570ff61ffcd210cf85c5ec8d896", ""]'
            '>checkout>build>test>incident<issue<bug#'
        ) == \
        dict(bug=[raw_bug(
            url="https://maillist/498232",
            subject="LED doesn't blink",
        )])

    assert \
        query_str(
            source,
            '>revision["82bcf49e5e6ad570ff61ffcd210cf85c5ec8d896", ""; '
            '"5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1", ""]'
            '>checkout>build>test>incident<issue<bug#'
        ) == \
        dict(bug=[raw_bug(
            url="https://maillist/498232",
            subject="LED doesn't blink",
        )])

    assert \
        query_str(
            source,
            '>revision["82bcf49e5e6ad570ff61ffcd210cf85c5ec8d896", ""; '
            '"5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1", ""]'
            '>checkout>build>incident<issue<bug#'
        ) == \
        dict(bug=[])


def test_bug(source):
    """Check data returned from query that starts with '>bug'"""

    assert \
        query_str(source, '>bug["noone:non-existent"]#') == \
        {"bug": []}

    assert \
        query_str(source, '>bug["https://bugzilla/207065"]#') == \
        {
            "bug": [
                raw_bug(
                    url="https://bugzilla/207065",
                    subject="Printer doesn't print",
                )
            ]
        }

    assert \
        query_str(source, '>bug["https://maillist/498232"]#') == \
        {
            "bug": [
                raw_bug(
                    url="https://maillist/498232",
                    subject="LED doesn't blink",
                )
            ]
        }

    assert \
        query_str(source, '>bug["https://maillist/498232"]>issue#') == \
        {
            "issue": [
                raw_issue(
                    id="kernelci:1209203344",
                    origin="kernelci",
                    report_url="https://maillist/498232",
                    report_subject="LED doesn't blink",
                    version=1,
                )
            ]
        }

    data = query_str(source, '>bug["https://bugzilla/207065"]>issue#')
    assert ["issue"] == list(data.keys())
    issues = data["issue"]
    assert len(issues) == 2
    assert \
        raw_issue(
            id="kernelci:1987934987",
            origin="kernelci",
            report_url="https://bugzilla/207065",
            report_subject="Printer doesn't print",
            version=1,
        ) in \
        issues
    assert \
        raw_issue(
            id="redhat:987987da98798f987c",
            origin="redhat",
            report_url="https://bugzilla/207065",
            report_subject="Printer doesn't print",
            version=100,
        ) in \
        issues

    assert \
        query_str(source, '>bug["https://bugzilla/1201011"]>issue#') == \
        {
            "issue": [
                raw_issue(
                    id="redhat:987987da98798aa233",
                    origin="redhat",
                    report_url="https://bugzilla/1201011",
                    report_subject="Compiler compiles wrong",
                    version=10,
                )
            ]
        }

    data = query_str(
        source,
        '>bug["https://bugzilla/207065"]>issue>incident#'
    )
    assert ["incident"] == list(data.keys())
    incidents = data["incident"]
    assert len(incidents) == 2
    assert \
        raw_incident(
            id="_:2987d298712",
            issue_id="kernelci:1987934987",
            issue_version=1,
            origin="_",
            test_id="redhat:redhat.org:"
            "b9d8be63bc2abca63165de5fd74f0f6d2f0b0d1c",
        ) in \
        incidents
    assert \
        raw_incident(
            id="redhat:987987da98798f987c-29874",
            issue_id="redhat:987987da98798f987c",
            issue_version=100,
            origin="redhat",
            test_id="redhat:redhat.org:"
            "b9d8be63bc2abca63165de5fd74f0f6d2f0b0d1c",
        ) in \
        incidents

    data = query_str(
        source,
        '>bug["https://maillist/498232"]>issue>incident#'
    )
    assert ["incident"] == list(data.keys())
    incidents = data["incident"]
    assert len(incidents) == 2
    assert \
        raw_incident(
            id="kernelci:29871398212",
            issue_id="kernelci:1209203344",
            issue_version=1,
            origin="kernelci",
            test_id="kernelci:kernelci.org:"
            "b9d8be63bc2abca63165de5fd74f0f6d2f0b0e2b",
        ) in \
        incidents
    assert \
        raw_incident(
            id="kernelci:29871398232",
            issue_id="kernelci:1209203344",
            issue_version=1,
            origin="kernelci",
            test_id="kernelci:kernelci.org:619c656de1fb4af479f2efae",
        ) in \
        incidents

    assert \
        query_str(
            source,
            '>bug["https://bugzilla/1201011"]>issue>incident#'
        ) == \
        dict(incident=[raw_incident(
            id="redhat:987987da98798aa233-12",
            issue_id="redhat:987987da98798aa233",
            issue_version=10,
            origin="redhat",
            test_id="redhat:redhat.org:"
            "b9d8be63bc2abca63165de5fd74f0f6d2f0b0d1c",
            build_id="redhat:redhat.org:619c65f9709de72e90f2efd0",
        )])

    assert \
        query_str(
            source,
            '>bug["https://bugzilla/207065"]>issue>incident<test#'
        ) == \
        dict(test=[raw_test(
            build_id="redhat:redhat.org:619c65f9709de72e90f2efd0",
            id="redhat:redhat.org:"
            "b9d8be63bc2abca63165de5fd74f0f6d2f0b0d1c",
            origin="redhat",
            status="DONE",
            waived=True,
        )])

    data = query_str(
        source,
        '>bug["https://maillist/498232"]>issue>incident<test#'
    )
    assert ["test"] == list(data.keys())
    tests = data["test"]
    assert len(tests) == 2
    assert \
        raw_test(
            build_id="kernelci:kernelci.org:619c64b1712847eccbf2efac",
            id="kernelci:kernelci.org:619c656de1fb4af479f2efae",
            origin="kernelci",
            path="baseline.dmesg.emerg",
            start_time="2021-11-23T03:52:13.671000+00:00",
            status="PASS",
            waived=False,
        ) in \
        tests
    assert \
        raw_test(
            build_id="kernelci:kernelci.org:619c65d3c1b0a764f3f2efa0",
            id="kernelci:kernelci.org:"
            "b9d8be63bc2abca63165de5fd74f0f6d2f0b0e2b",
            origin="kernelci",
            path="baseline.dmesg.crit",
            start_time="2021-11-23T03:52:13.666000+00:00",
            status="PASS",
            waived=False,
        ) in \
        tests

    assert \
        query_str(
            source,
            '>bug["https://bugzilla/1201011"]>issue>incident<test#'
        ) == \
        dict(test=[raw_test(
            build_id="redhat:redhat.org:619c65f9709de72e90f2efd0",
            id="redhat:redhat.org:"
            "b9d8be63bc2abca63165de5fd74f0f6d2f0b0d1c",
            origin="redhat",
            status="DONE",
            waived=True,
        )])

    assert \
        query_str(
            source,
            '>bug["https://bugzilla/1201011"]>issue>incident<build#'
        ) == \
        dict(build=[raw_build(
            checkout_id="_:redhat:"
            "5acb9c2a7bc836e9619c65f9709de72e90f2efd0",
            id="redhat:redhat.org:619c65f9709de72e90f2efd0",
            origin="redhat",
        )])

    revision_x = raw_revision(
        git_commit_hash="5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1",
        git_commit_name="v5.15-4077-g5acb9c2a7bc8",
        patchset_hash="",
    )
    revision_y = raw_revision(
        git_commit_hash="82bcf49e5e6ad570ff61ffcd210cf85c5ec8d896",
        patchset_hash="",
    )
    assert \
        query_str(
            source,
            '>bug["https://maillist/498232"]>issue>incident'
            '<test<build<checkout<revision#'
        ) in [
            dict(revision=[revision_x, revision_y]),
            dict(revision=[revision_y, revision_x]),
        ]


def test_issue(source):
    """Check data returned from query that starts with '>issue'"""

    assert \
        query_str(source, '>issue["noone:non-existent"]#') == \
        {"issue": []}

    assert \
        query_str(source, '>issue["kernelci:1209203344"]#') == \
        dict(issue=[raw_issue(
            id="kernelci:1209203344",
            origin="kernelci",
            report_url="https://maillist/498232",
            report_subject="LED doesn't blink",
            version=1,
        )])

    assert \
        query_str(source, '>issue["kernelci:1987934987"]#') == \
        dict(issue=[raw_issue(
            id="kernelci:1987934987",
            origin="kernelci",
            report_url="https://bugzilla/207065",
            report_subject="Printer doesn't print",
            version=1,
        )])

    assert \
        query_str(source, '>issue["redhat:987987da98798f987c"]#') == \
        dict(issue=[raw_issue(
            id="redhat:987987da98798f987c",
            origin="redhat",
            report_url="https://bugzilla/207065",
            report_subject="Printer doesn't print",
            version=100,
        )])

    assert \
        query_str(source, '>issue["redhat:987987da98798aa233"]#') == \
        dict(issue=[raw_issue(
            id="redhat:987987da98798aa233",
            origin="redhat",
            report_url="https://bugzilla/1201011",
            report_subject="Compiler compiles wrong",
            version=10,
        )])

    assert \
        query_str(
            source,
            '>issue["kernelci:1987934987"]>incident#'
        ) == \
        dict(incident=[raw_incident(
            id="_:2987d298712",
            issue_id="kernelci:1987934987",
            issue_version=1,
            origin="_",
            test_id="redhat:redhat.org:"
            "b9d8be63bc2abca63165de5fd74f0f6d2f0b0d1c",
        )])

    data = query_str(
        source,
        '>issue["kernelci:1209203344"]>incident#'
    )
    assert ["incident"] == list(data.keys())
    incidents = data["incident"]
    assert len(incidents) == 2
    assert \
        raw_incident(
            id="kernelci:29871398212",
            issue_id="kernelci:1209203344",
            issue_version=1,
            origin="kernelci",
            test_id="kernelci:kernelci.org:"
            "b9d8be63bc2abca63165de5fd74f0f6d2f0b0e2b",
        ) in \
        incidents
    assert \
        raw_incident(
            id="kernelci:29871398232",
            issue_id="kernelci:1209203344",
            issue_version=1,
            origin="kernelci",
            test_id="kernelci:kernelci.org:619c656de1fb4af479f2efae",
        ) in \
        incidents

    assert \
        query_str(
            source,
            '>issue["redhat:987987da98798aa233"]>incident#'
        ) == \
        dict(incident=[raw_incident(
            id="redhat:987987da98798aa233-12",
            issue_id="redhat:987987da98798aa233",
            issue_version=10,
            origin="redhat",
            test_id="redhat:redhat.org:"
            "b9d8be63bc2abca63165de5fd74f0f6d2f0b0d1c",
            build_id="redhat:redhat.org:619c65f9709de72e90f2efd0",
        )])

    assert \
        query_str(source, '>issue["kernelci:1209203344"]<bug#') == \
        dict(bug=[raw_bug(
            url="https://maillist/498232",
            subject="LED doesn't blink",
        )])

    assert \
        query_str(
            source,
            '>issue["kernelci:1987934987"; "redhat:987987da98798f987c"]'
            '<bug#'
        ) == \
        dict(bug=[raw_bug(
            url="https://bugzilla/207065",
            subject="Printer doesn't print",
        )])

    assert \
        query_str(source, '>issue["redhat:987987da98798aa233"]<bug#') == \
        dict(bug=[raw_bug(
            url="https://bugzilla/1201011",
            subject="Compiler compiles wrong",
        )])


def test_incident(source):
    """Check data returned from query that starts with '>incident'"""

    assert \
        query_str(source, '>incident["noone:non-existent"]#') == \
        {"incident": []}

    # Old incidents shouldn't be returned
    assert \
        query_str(
            source,
            '>incident["_:908812340982340"; "_:908812340982340"]#'
        ) == \
        dict(incident=[])

    assert \
        query_str(source, '>incident["_:2987d298712"]#') == \
        dict(incident=[raw_incident(
            id="_:2987d298712",
            issue_id="kernelci:1987934987",
            issue_version=1,
            origin="_",
            test_id="redhat:redhat.org:"
            "b9d8be63bc2abca63165de5fd74f0f6d2f0b0d1c",
        )])

    assert \
        query_str(source,
                  '>incident["redhat:987987da98798f987c-29874"]#') == \
        dict(incident=[raw_incident(
            id="redhat:987987da98798f987c-29874",
            issue_id="redhat:987987da98798f987c",
            issue_version=100,
            origin="redhat",
            test_id="redhat:redhat.org:"
            "b9d8be63bc2abca63165de5fd74f0f6d2f0b0d1c",
        )])

    assert \
        query_str(source, '>incident["kernelci:29871398212"]#') == \
        dict(incident=[raw_incident(
            id="kernelci:29871398212",
            issue_id="kernelci:1209203344",
            issue_version=1,
            origin="kernelci",
            test_id="kernelci:kernelci.org:"
            "b9d8be63bc2abca63165de5fd74f0f6d2f0b0e2b",
        )])

    assert \
        query_str(source, '>incident["kernelci:29871398232"]#') == \
        dict(incident=[raw_incident(
            id="kernelci:29871398232",
            issue_id="kernelci:1209203344",
            issue_version=1,
            origin="kernelci",
            test_id="kernelci:kernelci.org:619c656de1fb4af479f2efae",
        )])

    assert \
        query_str(source, '>incident["redhat:987987da98798aa233-12"]#') == \
        dict(incident=[raw_incident(
            id="redhat:987987da98798aa233-12",
            issue_id="redhat:987987da98798aa233",
            issue_version=10,
            origin="redhat",
            test_id="redhat:redhat.org:"
            "b9d8be63bc2abca63165de5fd74f0f6d2f0b0d1c",
            build_id="redhat:redhat.org:619c65f9709de72e90f2efd0",
        )])

    assert \
        query_str(source,
                  '>incident["redhat:987987da98798aa233-12"]<test#') == \
        dict(test=[raw_test(
            build_id="redhat:redhat.org:619c65f9709de72e90f2efd0",
            id="redhat:redhat.org:"
            "b9d8be63bc2abca63165de5fd74f0f6d2f0b0d1c",
            origin="redhat",
            status="DONE",
            waived=True,
        )])

    assert \
        query_str(source, '>incident["kernelci:29871398232"]<test#') == \
        dict(test=[raw_test(
            build_id="kernelci:kernelci.org:619c64b1712847eccbf2efac",
            id="kernelci:kernelci.org:619c656de1fb4af479f2efae",
            origin="kernelci",
            path="baseline.dmesg.emerg",
            start_time="2021-11-23T03:52:13.671000+00:00",
            status="PASS",
            waived=False,
        )])

    assert \
        query_str(source, '>incident["kernelci:29871398212"]<test#') == \
        dict(test=[raw_test(
            build_id="kernelci:kernelci.org:619c65d3c1b0a764f3f2efa0",
            id="kernelci:kernelci.org:"
            "b9d8be63bc2abca63165de5fd74f0f6d2f0b0e2b",
            origin="kernelci",
            path="baseline.dmesg.crit",
            start_time="2021-11-23T03:52:13.666000+00:00",
            status="PASS",
            waived=False,
        )])

    assert \
        query_str(source, '>incident["_:2987d298712"]<test#') == \
        dict(test=[raw_test(
            build_id="redhat:redhat.org:619c65f9709de72e90f2efd0",
            id="redhat:redhat.org:"
            "b9d8be63bc2abca63165de5fd74f0f6d2f0b0d1c",
            origin="redhat",
            status="DONE",
            waived=True,
        )])

    assert \
        query_str(
            source,
            '>incident["redhat:987987da98798f987c-29874"]<test#'
        ) == \
        dict(test=[raw_test(
            build_id="redhat:redhat.org:619c65f9709de72e90f2efd0",
            id="redhat:redhat.org:"
            "b9d8be63bc2abca63165de5fd74f0f6d2f0b0d1c",
            origin="redhat",
            status="DONE",
            waived=True,
        )])

    assert \
        query_str(
            source,
            '>incident["redhat:987987da98798aa233-12"; '
            '"_:2987d298712"; "redhat:987987da98798f987c-29874"]<test#'
        ) == \
        dict(test=[raw_test(
            build_id="redhat:redhat.org:619c65f9709de72e90f2efd0",
            id="redhat:redhat.org:"
            "b9d8be63bc2abca63165de5fd74f0f6d2f0b0d1c",
            origin="redhat",
            status="DONE",
            waived=True,
        )])

    assert \
        query_str(source,
                  '>incident["redhat:987987da98798aa233-12"]<build#') == \
        dict(build=[raw_build(
            checkout_id="_:redhat:"
            "5acb9c2a7bc836e9619c65f9709de72e90f2efd0",
            id="redhat:redhat.org:619c65f9709de72e90f2efd0",
            origin="redhat",
        )])

    assert \
        query_str(source,
                  '>incident["redhat:987987da98798aa233-12"]<issue#') == \
        dict(issue=[raw_issue(
            id="redhat:987987da98798aa233",
            origin="redhat",
            report_url="https://bugzilla/1201011",
            report_subject="Compiler compiles wrong",
            version=10,
        )])

    assert \
        query_str(source, '>incident["kernelci:29871398232"]<issue#') == \
        dict(issue=[raw_issue(
            id="kernelci:1209203344",
            origin="kernelci",
            report_url="https://maillist/498232",
            report_subject="LED doesn't blink",
            version=1,
        )])

    assert \
        query_str(source, '>incident["kernelci:29871398212"]<issue#') == \
        dict(issue=[raw_issue(
            id="kernelci:1209203344",
            origin="kernelci",
            report_url="https://maillist/498232",
            report_subject="LED doesn't blink",
            version=1,
        )])

    assert \
        query_str(
            source,
            '>incident["kernelci:29871398212"; '
            '"kernelci:29871398232"]<issue#'
        ) == \
        dict(issue=[raw_issue(
            id="kernelci:1209203344",
            origin="kernelci",
            report_url="https://maillist/498232",
            report_subject="LED doesn't blink",
            version=1,
        )])

    assert \
        query_str(source, '>incident["_:2987d298712"]<issue#') == \
        dict(issue=[raw_issue(
            id="kernelci:1987934987",
            origin="kernelci",
            report_url="https://bugzilla/207065",
            report_subject="Printer doesn't print",
            version=1,
        )])

    assert \
        query_str(
            source,
            '>incident["redhat:987987da98798f987c-29874"]<issue#'
        ) == \
        dict(issue=[raw_issue(
            id="redhat:987987da98798f987c",
            origin="redhat",
            report_url="https://bugzilla/207065",
            report_subject="Printer doesn't print",
            version=100,
        )])

    assert \
        query_str(
            source,
            '>incident["redhat:987987da98798aa233-12"]<issue<bug#'
        ) == \
        dict(bug=[raw_bug(
            subject="Compiler compiles wrong",
            url="https://bugzilla/1201011",
        )])

    assert \
        query_str(source, '>incident["kernelci:29871398232"]<issue<bug#') == \
        dict(bug=[raw_bug(
            subject="LED doesn't blink",
            url="https://maillist/498232"
        )])

    assert \
        query_str(source, '>incident["kernelci:29871398212"]<issue<bug#') == \
        dict(bug=[raw_bug(
            subject="LED doesn't blink",
            url="https://maillist/498232"
        )])

    assert \
        query_str(
            source,
            '>incident["kernelci:29871398232"; '
            '"kernelci:29871398212"]<issue<bug#'
        ) == \
        dict(bug=[raw_bug(
            subject="LED doesn't blink",
            url="https://maillist/498232"
        )])

    assert \
        query_str(source, '>incident["_:2987d298712"]<issue<bug#') == \
        dict(bug=[raw_bug(
            subject="Printer doesn't print",
            url="https://bugzilla/207065"
        )])

    assert \
        query_str(
            source,
            '>incident["redhat:987987da98798f987c-29874"]<issue<bug#'
        ) == \
        dict(bug=[raw_bug(
            subject="Printer doesn't print",
            url="https://bugzilla/207065"
        )])

    assert \
        query_str(
            source,
            '>incident["_:2987d298712"; '
            '"redhat:987987da98798f987c-29874"]<issue<bug#'
        ) == \
        dict(bug=[raw_bug(
            subject="Printer doesn't print",
            url="https://bugzilla/207065"
        )])
