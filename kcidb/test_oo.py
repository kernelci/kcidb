"""kcidb.oo test module"""

import kcidb
from kcidb.unittest import local_only
from kcidb.oo import Checkout, Build, Test, Node, Bug, Issue, Incident


@local_only
class KCIDBOOClientTestCase(kcidb.unittest.TestCase):
    """Test case for oo.Client"""
    # Conform to unittest conventions, pylint: disable=invalid-name

    def assertResponseContains(self, pattern_str, types, num_instances):
        """
        Check that the query for the specified pattern string returns
        objects of types with specified names, in specified quantity,
        and nothing else.
        Args:
            pattern_str:    A string which depicts the object to be retrieved.
            types:          A set of specified schema type name(s).
            num_instances:  The number of instance of object returned for
                            each schema type.
        """
        assert isinstance(pattern_str, str)
        assert isinstance(types, set)
        assert types <= set(kcidb.oo.CLASSES)
        assert isinstance(num_instances, int)

        data = self.client.query(kcidb.orm.Pattern.parse(pattern_str))

        self.assertEqual(set(data), types)
        for type_name, type_objs in data.items():
            self.assertEqual(len(type_objs), num_instances,
                             msg=f"Invalid number of {type_name} instances")
            for i, obj in enumerate(type_objs):
                self.assertIsInstance(obj, kcidb.oo.CLASSES[type_name],
                                      msg=f"Invalid type of {type_name} #{i}")

    def setUp(self):
        """Database setup and OO client creation"""
        source = kcidb.db.Client('sqlite::memory:')
        source.init()
        source.load(
            {
                "version": {
                    "major": 4,
                    "minor": 1
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
                        "waived": False,
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
                        "waived": True,
                        "status": "DONE"
                    },
                ]
            }
        )
        self.client = kcidb.oo.Client(source)

    def test_client_variable(self):
        """Check instance for client variable"""
        self.assertIsInstance(self.client, kcidb.oo.Client)

    def test_zero_object(self):
        """Check that empty object is returned."""
        self.assertResponseContains("", set(), 0)

    def test_one_object(self):
        """Check that one object is returned for every type"""

        self.assertResponseContains(
            ">test[kernelci:kernelci.org:619c656de1fb4af479f2efaa]#",
            {"test"}, 1
        )

        self.assertResponseContains(
            ">build[kernelci:kernelci.org:619c6a30636e8fe042f2efa0]#",
            {"build"}, 1
        )

        self.assertResponseContains(
            ">checkout[_:redhat:5acb9c2a7bc836e9619c65f9709de72e90f2efd0]#",
            {"checkout"}, 1
        )

        self.assertResponseContains(
            '>revision["5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1", ""]#',
            {"revision"}, 1
        )

        self.assertResponseContains(
            '>revision["bd355732283c23a365f7c55206c0385100d1c389", ""]#'
            '>checkout#>build#>test#',
            {"revision", "checkout", "build", "test"}, 1
        )

    def test_two_object(self):
        """Check that two objects are returned for every type"""

        self.assertResponseContains(
            ">test[kernelci:kernelci.org:619c656de1fb4af479f2efaa;"
            "redhat:redhat.org:b9d8be63bc2abca63165de5fd74f0f6d2f0b0d1c]#",
            {"test"}, 2
        )

        self.assertResponseContains(
            ">build[redhat:redhat.org:619c65f9709de72e90f2efd0;"
            "kernelci:kernelci.org:619c6c8d764403e3d4f2efff]#",
            {"build"}, 2
        )

        self.assertResponseContains(
            ">checkout[_:kernelci:5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1;"
            "_:redhat:5acb9c2a7bc836e9619c65f9709de72e90f2efd0]#",
            {"checkout"}, 2
        )

        self.assertResponseContains(
            '>revision["5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1", "";'
            '"bd355732283c23a365f7c55206c0385100d1c389", ""]#',
            {"revision"}, 2
        )

        self.assertResponseContains(
            '>revision["bd355732283c23a365f7c55206c0385100d1c389", "";'
            '"5acb9c2a7bc836e9619c65f9709de72e90f2efd0", ""]#'
            '>checkout#>build#>test#',
            {"test", "build", "checkout", "revision"}, 2
        )


@local_only
class KCIDBTraversingTestCase(kcidb.unittest.TestCase):
    """Test case for traversing objects."""

    # Conform to unittest conventions, pylint: disable=invalid-name

    def query_str(self, pattern_string):
        """Run OO client query with a parsed string."""
        return self.client.query(
            kcidb.orm.Pattern.parse(pattern_string)
        )

    @staticmethod
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

    def assertContains(self, container, contents):
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
            self.assertEqual(
                len(contents), 2,
                msg="Invalid number of element in contents"
            )
            self.assertEqual(
                len(container), contents[1],
                msg=f"Invalid number of {contents[0]} instances"
            )
            for i, obj in enumerate(container):
                self.assertIsInstance(
                    obj, contents[0],
                    msg=f"Invalid type of {contents[0]} #{i}")

        elif isinstance(contents, dict):
            for key in {**container, **contents}:
                if key not in contents:
                    self.assertNotIn(key, container,
                                     msg=f"Key({key}) missing in content.")
                if key not in container:
                    self.assertNotIn(key, contents,
                                     msg=f"Extra key({key}) found in content.")

                self.assertContains(container[key], contents[key])

    def setUp(self):
        """Setup database and create oo.Client object"""
        source = kcidb.db.Client("sqlite::memory:")
        source.init()
        source.load(
            {
                "version": {"major": 4, "minor": 1},
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
                        "id": "kernelci:valid1",
                        "origin": "kernelci",
                        "checkout_id": "_:valid1",
                        "architecture": "valid1",
                        "valid": True,
                    },
                    {
                        "id": "kernelci:valid2",
                        "origin": "kernelci",
                        "checkout_id": "_:valid1",
                        "architecture": "valid2",
                        "valid": True,
                    },
                    {
                        "id": "kernelci:invalid",
                        "origin": "kernelci",
                        "checkout_id": "_:valid1",
                        "architecture": "invalid",
                        "valid": False,
                    },
                    {
                        "id": "redhat:valid1",
                        "checkout_id": "_:valid2",
                        "origin": "redhat",
                        "valid": True,
                        "architecture": "valid1",
                    },
                    {
                        "id": "redhat:valid2",
                        "checkout_id": "_:valid2",
                        "origin": "redhat",
                        "valid": True,
                        "architecture": "valid2",
                    },
                    {
                        "id": "redhat:invalid",
                        "checkout_id": "_:valid2",
                        "origin": "redhat",
                        "valid": False,
                        "architecture": "invalid",
                    },
                ],
                "tests": [
                    {
                        "id": "kernelci:valid1_1",
                        "build_id": "kernelci:valid1",
                        "origin": "kernelci", "status": "PASS", "path": "pass1"
                    },
                    {
                        "id": "kernelci:valid1_2",
                        "build_id": "kernelci:valid1",
                        "origin": "kernelci", "status": "PASS", "path": "pass2"
                    },
                    {
                        "id": "kernelci:valid1_3",
                        "build_id": "kernelci:valid1",
                        "origin": "kernelci", "status": "FAIL", "path": "fail",
                    },
                    {
                        "id": "kernelci:valid2_1",
                        "build_id": "kernelci:valid2",
                        "origin": "kernelci", "status": "PASS", "path": "pass1"
                    },
                    {
                        "id": "kernelci:valid2_2",
                        "build_id": "kernelci:valid2",
                        "origin": "kernelci", "status": "PASS", "path": "pass2"
                    },
                    {
                        "id": "kernelci:valid2_3",
                        "build_id": "kernelci:valid2",
                        "origin": "kernelci", "status": "FAIL", "path": "fail"
                    },
                    {
                        "id": "redhat:valid1_1",
                        "build_id": "redhat:valid1",
                        "origin": "redhat", "status": "PASS", "path": "pass1"
                    },
                    {
                        "id": "redhat:valid1_2",
                        "build_id": "redhat:valid1",
                        "origin": "redhat", "status": "PASS", "path": "pass2",
                    },
                    {
                        "id": "redhat:valid1_3",
                        "build_id": "redhat:valid1",
                        "origin": "redhat", "status": "FAIL", "path": "fail",
                    },
                    {
                        "id": "redhat:valid2_1",
                        "build_id": "redhat:valid2",
                        "origin": "redhat", "status": "PASS", "path": "pass1"
                    },
                    {
                        "id": "redhat:valid2_2",
                        "build_id": "redhat:valid2",
                        "origin": "redhat", "status": "PASS", "path": "pass2",
                    },
                    {
                        "id": "redhat:valid2_3",
                        "build_id": "redhat:valid2",
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
                        "id": "kernelci:valid1_1_1",
                        "origin": "kernelci",
                        "issue_id": "kernelci:1",
                        "issue_version": 1,
                        "present": True,
                        "test_id": "kernelci:valid1_1",
                    },
                    {
                        "id": "kernelci:valid1_3_1",
                        "origin": "kernelci",
                        "issue_id": "kernelci:1",
                        "issue_version": 1,
                        "present": True,
                        "test_id": "kernelci:valid1_3",
                    },
                    {
                        "id": "redhat:valid1",
                        "origin": "redhat",
                        "issue_id": "redhat:1",
                        "issue_version": 1,
                        "present": True,
                        "build_id": "redhat:valid1",
                    },
                    {
                        "id": "redhat:valid2",
                        "origin": "redhat",
                        "issue_id": "redhat:1",
                        "issue_version": 1,
                        "present": True,
                        "build_id": "redhat:valid2",
                    },
                ],
            }
        )
        self.client = kcidb.oo.Client(source)

    def test_traversing_revision_links(self):
        """Check that revision's links are successfully traversed."""

        revision = self.query_str(
            '>revision["5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1", ""]#'
        )['revision'][0]

        self.assertContains(revision.checkouts, (Checkout, 3))
        self.assertContains(revision.builds, (Build, 6))
        self.assertContains(revision.tests, (Test, 12))

        self.assertContains(revision.repo_branch_checkouts, {
            "https://repo_valid": {
                "valid1": (Checkout, 1),
                "valid2": (Checkout, 1),
            },
            "https://repo_invalid": {
                "invalid": (Checkout, 1),
            },
        })
        self.assertContains(revision.architecture_valid_builds, {
            "invalid": {
                False: (Build, 2),
                True: (Build, 0),
                None: (Build, 0),
            },
            "valid1": {
                False: (Build, 0),
                True: (Build, 2),
                None: (Build, 0)
            },
            "valid2": {
                False: (Build, 0),
                True: (Build, 2),
                None: (Build, 0)
            }
        })

        self.assertFalse(revision.checkouts_valid)
        self.assertFalse(revision.builds_valid)

        self.assertIsInstance(revision.tests_root, Node)

        self.assertContains(revision.bugs, (Bug, 2))
        self.assertEqual({revision.bugs[0].url,
                          revision.bugs[1].url},
                         {"https://bugzilla.redhat.com/1",
                          "https://kernelci.org/issue/1"})
        self.assertEqual({revision.bugs[0].subject,
                          revision.bugs[1].subject},
                         {"Bad issue", "Worse issue"})

        self.assertContains(revision.issues, (Issue, 2))
        self.assertEqual({revision.issues[0].id, revision.issues[1].id},
                         {"redhat:1", "kernelci:1"})

        self.assertContains(revision.incidents, (Incident, 4))
        self.assertEqual({i.id for i in revision.incidents},
                         {"kernelci:valid1_1_1", "kernelci:valid1_3_1",
                          "redhat:valid1", "redhat:valid2"})

    def test_traversing_valid_checkout_links(self):
        """Check that valid checkout links are successfully traversed."""
        checkouts = KCIDBTraversingTestCase.filter_valid(
            self.query_str(
                '>revision["5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1", ""]'
                '>checkout#'
            )["checkout"]
        )
        self.assertContains(checkouts, (Checkout, 2))
        self.assertEqual(
            checkouts[0].get_parent_id("revision"),
            checkouts[0].revision.get_id()
        )
        self.assertContains(checkouts[0].builds, (Build, 3))
        self.assertContains(checkouts[0].tests, (Test, 6))
        self.assertIsInstance(checkouts[0].tests_root, Node)

        self.assertContains(checkouts[0].bugs, (Bug, 1))
        self.assertContains(checkouts[1].bugs, (Bug, 1))
        self.assertEqual({checkouts[0].bugs[0].url,
                          checkouts[1].bugs[0].url},
                         {"https://kernelci.org/issue/1",
                          "https://bugzilla.redhat.com/1"})

        self.assertContains(checkouts[0].issues, (Issue, 1))
        self.assertContains(checkouts[1].issues, (Issue, 1))
        self.assertEqual({checkouts[0].issues[0].id,
                          checkouts[1].issues[0].id},
                         {"redhat:1", "kernelci:1"})

        self.assertContains(checkouts[0].incidents, (Incident, 2))
        self.assertContains(checkouts[1].incidents, (Incident, 2))
        self.assertEqual({i.id for c in checkouts for i in c.incidents},
                         {"kernelci:valid1_1_1", "kernelci:valid1_3_1",
                          "redhat:valid1", "redhat:valid2"})

    def test_traversing_valid_build_links(self):
        """Check that valid build links are successfully traversed."""
        builds = KCIDBTraversingTestCase.filter_valid(
            self.query_str(
                '>revision["5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1", ""]'
                '>checkout>build#'
            )["build"]
        )
        self.assertContains(builds, (Build, 4))
        self.assertEqual(
            builds[3].get_parent_id("checkout"),
            builds[3].checkout.get_id()
        )
        self.assertContains(builds[0].tests, (Test, 3))
        self.assertIsInstance(builds[0].tests_root, Node)

    def test_traversing_test_links(self):
        """Check that test links are successfully traversed."""
        tests = self.query_str(
            '>revision["5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1", ""]'
            '>checkout>build>test#'
        )["test"]
        self.assertContains(tests, (Test, 12))
        self.assertEqual(
            tests[2].get_parent_id("build"),
            tests[2].build.get_id()
        )

    def test_traversing_revision_root_test_node(self):
        """
        Check that valid revision's root test node links are
        successfully traversed.
        """
        revision = self.query_str(
            '>revision["5acb9c2a7bc836e9e5172bbcd2311499c5b4e5f1", ""]#'
        )['revision'][0]

        self.assertEqual(
            revision,
            revision.tests_root.parent
        )
        self.assertContains(
            revision.tests_root.tests,
            (Test, 12)
        )

        path = []
        for key in revision.tests_root.nodes:
            node = revision.tests_root.nodes[key]
            path.append(node.path)

            self.assertEqual(node.parent, revision.tests_root)

            self.assertContains(node.tests, (Test, 4))
            # Check that each tests have the same path.
            self.assertEqual(len({obj.path for obj in node.tests}), 1)

            self.assertEqual(node.nodes, {})

        self.assertEqual(path, ["pass1", "pass2", "fail"])
