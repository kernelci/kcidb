"""kcdib.orm module tests"""

from jinja2 import Template
import kcidb_io
import kcidb


EMPTY_TEMPLATE = Template("")
SCHEMA = kcidb.orm.Schema(dict(
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
        summary_template=EMPTY_TEMPLATE,
        description_template=EMPTY_TEMPLATE,
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
        summary_template=EMPTY_TEMPLATE,
        description_template=EMPTY_TEMPLATE,
    ),
    build=dict(
        field_json_schemas=dict(
            id=dict(type="string"),
        ),
        required_fields=set(),
        id_fields=("id",),
        children=dict(
            test=("build_id",),
            build_test_environment=("build_id",)
        ),
        summary_template=EMPTY_TEMPLATE,
        description_template=EMPTY_TEMPLATE,
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
        summary_template=EMPTY_TEMPLATE,
        description_template=EMPTY_TEMPLATE,
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
        summary_template=EMPTY_TEMPLATE,
        description_template=EMPTY_TEMPLATE,
    ),
    test=dict(
        field_json_schemas=dict(
            id=dict(type="string"),
        ),
        required_fields=set(),
        id_fields=("id",),
        summary_template=EMPTY_TEMPLATE,
        description_template=EMPTY_TEMPLATE,
    ),
))


def parse(string, obj_id_list_list=None):
    """Parse a pattern string using test schema"""
    return kcidb.orm.Pattern.parse(string, obj_id_list_list,
                                   schema=SCHEMA)


def pattern(base, child, obj_type_name, obj_id_list, match):
    """Create a pattern with test schema"""
    return kcidb.orm.Pattern(base, child,
                             SCHEMA.types[obj_type_name],
                             obj_id_list, match)


def from_io(io_data):
    """Create a pattern from I/O with test schema"""
    return kcidb.orm.Pattern.from_io(io_data, schema=SCHEMA)


class KCIDBORMPatternTestCase(kcidb.unittest.TestCase):
    """Test case for the Pattern class"""

    def test_parse(self):
        """Check pattern parsing works"""
        self.assertEqual(parse(""), [])
        self.assertEqual(parse("<*"), [])
        self.assertEqual(parse(">revision"),
                         [pattern(None, True, "revision", None, False)])
        self.assertEqual(parse(">checkout"),
                         [pattern(None, True, "checkout", None, False)])
        self.assertEqual(parse(">build"),
                         [pattern(None, True, "build", None, False)])
        self.assertEqual(parse(">build_test_environment"),
                         [pattern(None, True, "build_test_environment",
                                  None, False)])
        self.assertEqual(parse(">test_environment"),
                         [pattern(None, True, "test_environment",
                                  None, False)])
        self.assertEqual(parse(">test"),
                         [pattern(None, True, "test", None, False)])
        self.assertEqual(parse(">revision#"),
                         [pattern(None, True, "revision", None, True)])
        self.assertEqual(parse(">revision$"),
                         [pattern(None, True, "revision", None, True)])
        self.assertEqual(parse(">revision%", [[("abc", "def")]]),
                         [pattern(None, True, "revision",
                                  [("abc", "def")], False)])
        self.assertEqual(
            parse(">revision%>checkout>build#", [[("abc", "def")]]),
            [pattern(
                pattern(
                    pattern(None, True, "revision", [("abc", "def")], False),
                    True, "checkout", None, False
                ),
                True, "build", None, True
            )])
        self.assertEqual(
            parse(">revision%>checkout%>build#",
                  [[("abc", "def")], [("123",)]]),
            [pattern(
                pattern(
                    pattern(None, True, "revision", [("abc", "def")], False),
                    True, "checkout", [("123",)], False
                ),
                True, "build", None, True
            )])
        self.assertEqual(
            parse(">build>*#"),
            [
                pattern(
                    pattern(None, True, "build", None, False),
                    True, "test", None, True
                ),
                pattern(
                    pattern(
                        pattern(None, True, "build", None, False),
                        True, "build_test_environment", None, True
                    ),
                    True, "test", None, True
                )
            ]
        )
        self.assertEqual(
            parse(">build%<*$", [[("abc",)]]),
            [
                pattern(
                    pattern(
                        pattern(None, True, "build", [("abc",)], False),
                        False, "checkout", None, False
                    ),
                    False, "revision", None, True
                )
            ]
        )

        build_to_build_pattern = \
            pattern(
                pattern(
                    pattern(
                        pattern(
                            pattern(None, True, "build", [("abc",)], False),
                            False, "checkout", None, False
                        ),
                        False, "revision", None, True
                    ),
                    True, "checkout", None, True
                ),
                True, "build", None, True
            )
        self.assertEqual(
            parse(">build%<*$>*#", [[("abc",)]]),
            [
                pattern(
                    build_to_build_pattern,
                    True, "test", None, True
                ),
                pattern(
                    pattern(
                        build_to_build_pattern,
                        True, "build_test_environment", None, True
                    ),
                    True, "test", None, True
                ),
            ]
        )

        self.assertEqual(parse(">revision[abc,def]"),
                         [pattern(None, True, "revision",
                                  [("abc", "def")], False)])
        self.assertEqual(parse(">checkout[123]"),
                         [pattern(None, True, "checkout",
                                  [("123",)], False)])
        self.assertEqual(
            parse(">revision[abc, def]>checkout[123]>build#"),
            [pattern(
                pattern(
                    pattern(None, True, "revision", [("abc", "def")], False),
                    True, "checkout", [("123",)], False
                ),
                True, "build", None, True
            )])
        self.assertEqual(parse(">revision[abc,def; ghi, jkl]"),
                         [pattern(None, True, "revision",
                                  [("abc", "def"), ("ghi", "jkl")], False)])
        self.assertEqual(parse('>checkout["123"]'),
                         [pattern(None, True, "checkout",
                                  [("123",)], False)])
        self.assertEqual(parse('>checkout["1 2 3"]'),
                         [pattern(None, True, "checkout",
                                  [("1 2 3",)], False)])
        self.assertEqual(parse('>checkout["1,2;3"]'),
                         [pattern(None, True, "checkout",
                                  [("1,2;3",)], False)])
        self.assertEqual(parse('>checkout["1\\"2\\"3"]'),
                         [pattern(None, True, "checkout",
                                  [("1\"2\"3",)], False)])
        self.assertEqual(parse('>checkout["1\\\\2\\\\3"]'),
                         [pattern(None, True, "checkout",
                                  [("1\\2\\3",)], False)])
        self.assertEqual(parse('>revision["abc","def"; "ghi", "jkl"]'),
                         [pattern(None, True, "revision",
                                  [("abc", "def"), ("ghi", "jkl")], False)])
        self.assertEqual(
            parse(' > revision [ "abc" , "def" ; "ghi" , "jkl" ] '),
            [pattern(None, True, "revision",
                     [("abc", "def"), ("ghi", "jkl")], False)]
        )

    def test_from_io(self):
        """
        Check Pattern.from_io() works correctly.
        """
        io_data = kcidb_io.new()
        self.assertEqual(from_io(io_data), [])

        io_data = {
            "checkouts": [
            ],
            "builds": [
            ],
            "tests": [
            ],
            **kcidb_io.new()
        }
        self.assertEqual(from_io(io_data), [])

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
            **kcidb_io.new()
        }
        self.assertEqual(from_io(io_data), [
            pattern(None, True, "checkout", [("origin:1",)], True)
        ])

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
            **kcidb_io.new()
        }
        self.assertEqual(from_io(io_data), [
            pattern(
                None, True, "checkout",
                [("origin:1",), ("origin:2",), ("origin:3",)], True
            )
        ])

        io_data = {
            "builds": [
                {
                    "checkout_id": "origin:1",
                    "id": "origin:2",
                    "origin": "origin",
                },
            ],
            **kcidb_io.new()
        }
        self.assertEqual(from_io(io_data), [
            pattern(None, True, "build", [("origin:2",)], True)
        ])

        io_data = {
            "tests": [
                {
                    "build_id": "origin:2",
                    "id": "origin:3",
                    "origin": "origin",
                },
            ],
            **kcidb_io.new()
        }
        self.assertEqual(from_io(io_data), [
            pattern(None, True, "test", [("origin:3",)], True)
        ])

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
            **kcidb_io.new()
        }
        self.assertEqual(from_io(io_data), [
            pattern(None, True, "checkout", [("origin:1",)], True),
            pattern(None, True, "build", [("origin:2",)], True),
            pattern(None, True, "test", [("origin:3",)], True),
        ])
