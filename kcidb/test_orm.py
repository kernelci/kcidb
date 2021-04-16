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


def parse(string, obj_id_set_list=None):
    """Parse a pattern string using test schema"""
    return kcidb.orm.Pattern.parse(string, obj_id_set_list,
                                   schema=SCHEMA)


def pattern(base, child, obj_type_name, obj_id_set=None):
    """Create a pattern with test schema"""
    return kcidb.orm.Pattern(base, child,
                             SCHEMA.types[obj_type_name], obj_id_set)


def from_io(io_data):
    """Create a pattern from I/O with test schema"""
    return kcidb.orm.Pattern.from_io(io_data, schema=SCHEMA)


class KCIDBORMPatternTestCase(kcidb.unittest.TestCase):
    """Test case for the Pattern class"""

    def test_parse_misc(self):
        """Check miscellaneous pattern parsing works"""
        self.assertEqual(parse(""), set())
        self.assertEqual(parse("<*"), set())
        self.assertEqual(parse("<*$"), set())
        self.assertEqual(parse("<*#"), set())
        self.assertEqual(parse(">revision"), set())
        self.assertEqual(parse(">revision$"),
                         {pattern(None, True, "revision")})
        self.assertEqual(parse(">revision#"),
                         {pattern(None, True, "revision")})
        self.assertEqual(parse(">checkout"), set())
        self.assertEqual(parse(">checkout$"),
                         {pattern(None, True, "checkout")})
        self.assertEqual(parse(">checkout#"),
                         {pattern(None, True, "checkout")})
        self.assertEqual(parse(">build"), set())
        self.assertEqual(parse(">build$"),
                         {pattern(None, True, "build")})
        self.assertEqual(parse(">build#"),
                         {pattern(None, True, "build")})
        self.assertEqual(parse(">build_test_environment"), set())
        self.assertEqual(parse(">build_test_environment$"),
                         {pattern(None, True, "build_test_environment")})
        self.assertEqual(parse(">build_test_environment#"),
                         {pattern(None, True, "build_test_environment")})
        self.assertEqual(parse(">test_environment"),
                         set())
        self.assertEqual(parse(">test_environment$"),
                         {pattern(None, True, "test_environment")})
        self.assertEqual(parse(">test_environment#"),
                         {pattern(None, True, "test_environment")})
        self.assertEqual(parse(">test"), set())
        self.assertEqual(parse(">test$"),
                         {pattern(None, True, "test")})
        self.assertEqual(parse(">test#"),
                         {pattern(None, True, "test")})

        self.assertEqual(parse(">revision%", [{("abc", "def")}]), set())
        self.assertEqual(parse(">revision%$", [{("abc", "def")}]),
                         {pattern(None, True, "revision", {("abc", "def")})})
        self.assertEqual(parse(">revision%#", [{("abc", "def")}]),
                         {pattern(None, True, "revision", {("abc", "def")})})
        self.assertEqual(
            parse(">revision%>checkout>build#", [{("abc", "def")}]),
            {pattern(
                pattern(
                    pattern(None, True, "revision", {("abc", "def")}),
                    True, "checkout"
                ),
                True, "build"
            )})
        self.assertEqual(
            parse(">revision%>checkout%>build#",
                  [{("abc", "def")}, {("123",)}]),
            {pattern(
                pattern(
                    pattern(None, True, "revision", {("abc", "def")}),
                    True, "checkout", {("123",)}
                ),
                True, "build"
            )})
        self.assertEqual(
            parse(">build>*#"),
            {
                pattern(
                    pattern(None, True, "build"),
                    True, "test"
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
                )
            }
        )
        self.assertEqual(
            parse(">build#>*#"),
            {
                pattern(None, True, "build"),
                pattern(
                    pattern(None, True, "build"),
                    True, "test"
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
                )
            }
        )
        self.assertEqual(
            parse(">build%<*$", [{("abc",)}]),
            {
                pattern(
                    pattern(
                        pattern(None, True, "build", {("abc",)}),
                        False, "checkout"
                    ),
                    False, "revision"
                )
            }
        )

        revision_pattern = pattern(
            pattern(
                pattern(None, True, "build", {("abc",)}),
                False, "checkout"
            ),
            False, "revision"
        )
        checkout_pattern = pattern(revision_pattern, True, "checkout")
        build_pattern = pattern(checkout_pattern, True, "build")
        build_test_environment_pattern = pattern(
            build_pattern, True, "build_test_environment"
        )
        self.assertEqual(
            parse(">build%<*$>*#", [{("abc",)}]),
            {
                revision_pattern,
                checkout_pattern,
                build_pattern,
                pattern(build_pattern, True, "test"),
                build_test_environment_pattern,
                pattern(
                    build_test_environment_pattern, True, "test"
                ),
            }
        )

    def test_parse_id_list(self):
        """Check pattern inline ID list parsing works"""
        self.assertEqual(parse(">revision[abc,def]#"),
                         {pattern(None, True, "revision", {("abc", "def")})})
        self.assertEqual(parse(">checkout[123]#"),
                         {pattern(None, True, "checkout", {("123",)})})
        self.assertEqual(
            parse(">revision[abc, def]>checkout[123]>build#"),
            {pattern(
                pattern(
                    pattern(None, True, "revision", {("abc", "def")}),
                    True, "checkout", {("123",)}
                ),
                True, "build"
            )})
        self.assertEqual(parse(">revision[abc,def; ghi, jkl]#"),
                         {pattern(None, True, "revision",
                                  {("abc", "def"), ("ghi", "jkl")})})
        self.assertEqual(parse('>checkout["123"]#'),
                         {pattern(None, True, "checkout", {("123",)})})
        self.assertEqual(parse('>checkout["1 2 3"]#'),
                         {pattern(None, True, "checkout", {("1 2 3",)})})
        self.assertEqual(parse('>checkout["1,2;3"]#'),
                         {pattern(None, True, "checkout", {("1,2;3",)})})
        self.assertEqual(parse('>checkout["1\\"2\\"3"]#'),
                         {pattern(None, True, "checkout", {("1\"2\"3",)})})
        self.assertEqual(parse('>checkout["1\\\\2\\\\3"]#'),
                         {pattern(None, True, "checkout", {("1\\2\\3",)})})
        self.assertEqual(parse('>revision["abc","def"; "ghi", "jkl"]#'),
                         {pattern(None, True, "revision",
                                  {("abc", "def"), ("ghi", "jkl")})})
        self.assertEqual(
            parse(' > revision [ "abc" , "def" ; "ghi" , "jkl" ] #'),
            {pattern(None, True, "revision",
                     {("abc", "def"), ("ghi", "jkl")})}
        )

    def test_parse_trail_discard(self):
        """Check pattern parsing discards unmatched trail specifications"""
        self.assertEqual(
            parse(">checkout[123]>build#>test>*"),
            {
                pattern(
                    pattern(None, True, "checkout", {("123",)}),
                    True, "build"
                )
            }
        )

        self.assertEqual(
            parse(">checkout[123]>build$>test>*"),
            {
                pattern(
                    pattern(None, True, "checkout", {("123",)}),
                    True, "build"
                )
            }
        )

    def test_parse_failures(self):
        """Check pattern parsing failures are handled appropriately"""
        with self.assertRaisesRegex(Exception, "Failed expanding") as exc_cm:
            parse(">foobar")
        exc = exc_cm.exception.__context__
        self.assertIsNotNone(exc)
        self.assertIsInstance(exc, Exception)
        self.assertRegex(str(exc), "Cannot find type 'foobar'")

        with self.assertRaisesRegex(Exception, "Failed expanding") as exc_cm:
            parse(">revision[abc]")
        exc = exc_cm.exception.__context__
        self.assertIsNotNone(exc)
        self.assertIsInstance(exc, Exception)
        self.assertRegex(str(exc), "Invalid number of ID fields")

    def test_from_io(self):
        """
        Check Pattern.from_io() works correctly.
        """
        io_data = kcidb_io.new()
        self.assertEqual(from_io(io_data), set())

        io_data = {
            "checkouts": [
            ],
            "builds": [
            ],
            "tests": [
            ],
            **kcidb_io.new()
        }
        self.assertEqual(from_io(io_data), set())

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
        self.assertEqual(from_io(io_data), {
            pattern(None, True, "checkout", {("origin:1",)})
        })

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
        self.assertEqual(from_io(io_data), {
            pattern(
                None, True, "checkout",
                {("origin:1",), ("origin:2",), ("origin:3",)}
            )
        })

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
        self.assertEqual(from_io(io_data), {
            pattern(None, True, "build", {("origin:2",)})
        })

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
        self.assertEqual(from_io(io_data), {
            pattern(None, True, "test", {("origin:3",)})
        })

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
        self.assertEqual(from_io(io_data), {
            pattern(None, True, "checkout", {("origin:1",)}),
            pattern(None, True, "build", {("origin:2",)}),
            pattern(None, True, "test", {("origin:3",)}),
        })
