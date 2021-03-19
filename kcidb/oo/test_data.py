"""kcdib.oo.data module tests"""

import kcidb


class KCIDBOODataPattern(kcidb.unittest.TestCase):
    """Test case for the Pattern class"""

    def test_parse(self):
        """Check pattern parsing works"""
        schema = kcidb.oo.data.Schema(dict(
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
                    build_test_environment=("build_id",)
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
            ),
        ))

        def parse(string, obj_id_list_list=None):
            return kcidb.oo.data.Pattern.parse(string, obj_id_list_list,
                                               schema=schema)

        def pattern(base, child, obj_type_name, obj_id_list, match):
            return kcidb.oo.data.Pattern(base, child,
                                         schema.types[obj_type_name],
                                         obj_id_list, match)

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
