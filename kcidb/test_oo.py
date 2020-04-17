"""kcdib.oo module tests"""
import unittest
from kcidb.io import schema
from kcidb.oo import Node, Revision, Build, Test, TestEnvironment, \
    from_io, apply_mask, remove_orphans

# Disable long line checking for JSON data
# flake8: noqa
# pylint: disable=line-too-long
# Needed for (dynamically-added) Node attributes
# pylint: disable=no-member

class FromIOTestCase(unittest.TestCase):
    """kcidb.oo.from_io() test case"""

    def setUp(self):
        """Setup tests"""
        # pylint: disable=invalid-name
        self.maxDiff = None
        self.version = dict(
            major=schema.LATEST.major,
            minor=schema.LATEST.minor
        )

    def test_empty(self):
        """
        Check empty I/O data is converted correctly.
        """
        io_data = dict(version=self.version)

        expected_oo_data = dict(version=self.version)

        oo_data = from_io(io_data)
        self.assertEqual(oo_data, expected_oo_data)

    def test_revision(self):
        """
        Check single-build I/O data is converted correctly.
        """
        io_data = {
            "version": self.version,
            "revisions": [
                {
                    "contacts": [
                        "rdma-dev-team@redhat.com"
                    ],
                    "discovery_time": "2020-03-02T15:16:15.790000+00:00",
                    "git_repository_branch": "wip/jgg-for-next",
                    "git_repository_commit_hash": "5e29d1443c46b6ca70a4c940a67e8c09f05dcb7e",
                    "git_repository_url": "git://git.kernel.org/pub/scm/linux/kernel/git/rdma/rdma.git",
                    "misc": {
                        "pipeline_id": 467715
                    },
                    "id": "redhat:git://git.kernel.org/pub/scm/linux/kernel/git/rdma/rdma.git@5e29d1443c46b6ca70a4c940a67e8c09f05dcb7e",
                    "patch_mboxes": [],
                    "valid": True,
                },
            ],
        }

        expected_oo_data = {
            "version": self.version,
            "revisions": {
                "redhat:git://git.kernel.org/pub/scm/linux/kernel/git/rdma/rdma.git@5e29d1443c46b6ca70a4c940a67e8c09f05dcb7e":
                Revision({}, {
                    "contacts": [
                        "rdma-dev-team@redhat.com"
                    ],
                    "discovery_time": "2020-03-02T15:16:15.790000+00:00",
                    "git_repository_branch": "wip/jgg-for-next",
                    "git_repository_commit_hash": "5e29d1443c46b6ca70a4c940a67e8c09f05dcb7e",
                    "git_repository_url": "git://git.kernel.org/pub/scm/linux/kernel/git/rdma/rdma.git",
                    "misc": {
                        "pipeline_id": 467715
                    },
                    "id": "redhat:git://git.kernel.org/pub/scm/linux/kernel/git/rdma/rdma.git@5e29d1443c46b6ca70a4c940a67e8c09f05dcb7e",
                    "patch_mboxes": [],
                    "valid": True,
                    "builds_": {},
                }),
            },
        }

        oo_data = from_io(io_data)
        self.assertEqual(oo_data, expected_oo_data)

    def test_build(self):
        """
        Check single-build I/O data is converted correctly.
        """
        io_data = {
            "version": self.version,
            "builds": [
                {
                    "architecture": "aarch64",
                    "command": "make -j30 INSTALL_MOD_STRIP=1 targz-pkg",
                    "compiler": "aarch64-linux-gnu-gcc (GCC) 9.2.1 20190827 (Red Hat Cross 9.2.1-1)",
                    "config_name": "fedora",
                    "duration": 237.0,
                    "id": "redhat:678223",
                    "input_files": [],
                    "log_url": "https://cki-artifacts.s3.amazonaws.com/datawarehouse/2020/03/03/469720/build_aarch64.log",
                    "misc": {
                        "job_id": 678223,
                        "pipeline_id": 469720
                    },
                    "output_files": [],
                    "revision_id": "redhat:https://git.kernel.org/pub/scm/linux/kernel/git/stable/linux-stable-rc.git@1254e88b4fc1470d152f494c3590bb6a33ab33eb",
                    "start_time": "2020-03-03T17:52:02.370000+00:00",
                    "valid": True
                },
            ],
        }

        expected_oo_data = {
            "version": self.version,
            "builds": {
                "redhat:678223":
                Build({}, {
                    "architecture": "aarch64",
                    "command": "make -j30 INSTALL_MOD_STRIP=1 targz-pkg",
                    "compiler": "aarch64-linux-gnu-gcc (GCC) 9.2.1 20190827 (Red Hat Cross 9.2.1-1)",
                    "config_name": "fedora",
                    "duration": 237.0,
                    "id": "redhat:678223",
                    "input_files": [],
                    "log_url": "https://cki-artifacts.s3.amazonaws.com/datawarehouse/2020/03/03/469720/build_aarch64.log",
                    "misc": {
                        "job_id": 678223,
                        "pipeline_id": 469720
                    },
                    "output_files": [],
                    "revision_id": "redhat:https://git.kernel.org/pub/scm/linux/kernel/git/stable/linux-stable-rc.git@1254e88b4fc1470d152f494c3590bb6a33ab33eb",
                    "start_time": "2020-03-03T17:52:02.370000+00:00",
                    "valid": True,
                    "revision_": None,
                    "tests_": {},
                }),
            },
        }

        oo_data = from_io(io_data)
        self.assertEqual(oo_data, expected_oo_data)

    def test_test(self):
        """
        Check single-test I/O data is converted correctly.
        """
        io_data = {
            "version": self.version,
            "tests": [
                {
                    "build_id": "redhat:679936",
                    "description": "IOMMU boot test",
                    "duration": 1847.0,
                    "id": "redhat:107205807",
                    "output_files": [
                        {
                            "name": "x86_64_4_console.log",
                            "url": "https://cki-artifacts.s3.amazonaws.com/datawarehouse/2020/03/04/471145/x86_64_4_console.log"
                        },
                        {
                            "name": "x86_64_4_IOMMU_boot_test_dmesg.log",
                            "url": "https://cki-artifacts.s3.amazonaws.com/datawarehouse/2020/03/04/471145/x86_64_4_IOMMU_boot_test_dmesg.log"
                        },
                        {
                            "name": "x86_64_4_IOMMU_boot_test_resultoutputfile.log",
                            "url": "https://cki-artifacts.s3.amazonaws.com/datawarehouse/2020/03/04/471145/x86_64_4_IOMMU_boot_test_resultoutputfile.log"
                        },
                        {
                            "name": "x86_64_4_IOMMU_boot_test_taskout.log",
                            "url": "https://cki-artifacts.s3.amazonaws.com/datawarehouse/2020/03/04/471145/x86_64_4_IOMMU_boot_test_taskout.log"
                        }
                    ],
                    "environment": {
                        "description": "meson-gxl-s905d-p230 in lab-baylibre",
                        "misc": {
                            "device": "meson-gxl-s905d-p230",
                            "instance": "meson-gxl-s905d-p230-sea",
                            "lab": "lab-baylibre",
                            "mach": "amlogic",
                            "rootfs_url": "https://storage.kernelci.org/images/rootfs/buildroot/kci-2019.02-9-g25091c539382/arm64/baseline/rootfs.cpio.gz"
                        }
                    },
                    "path": "redhat_iommu_boot",
                    "start_time": "2020-03-04T21:30:57+00:00",
                    "status": "ERROR",
                    "waived": True
                },
            ],
        }

        expected_oo_data = {
            "version": self.version,
            "tests": {
                "redhat:107205807":
                Test({}, {
                    "build_id": "redhat:679936",
                    "description": "IOMMU boot test",
                    "duration": 1847.0,
                    "id": "redhat:107205807",
                    "output_files": [
                        Node({}, {
                            "name": "x86_64_4_console.log",
                            "url": "https://cki-artifacts.s3.amazonaws.com/datawarehouse/2020/03/04/471145/x86_64_4_console.log"
                        }),
                        Node({}, {
                            "name": "x86_64_4_IOMMU_boot_test_dmesg.log",
                            "url": "https://cki-artifacts.s3.amazonaws.com/datawarehouse/2020/03/04/471145/x86_64_4_IOMMU_boot_test_dmesg.log"
                        }),
                        Node({}, {
                            "name": "x86_64_4_IOMMU_boot_test_resultoutputfile.log",
                            "url": "https://cki-artifacts.s3.amazonaws.com/datawarehouse/2020/03/04/471145/x86_64_4_IOMMU_boot_test_resultoutputfile.log"
                        }),
                        Node({}, {
                            "name": "x86_64_4_IOMMU_boot_test_taskout.log",
                            "url": "https://cki-artifacts.s3.amazonaws.com/datawarehouse/2020/03/04/471145/x86_64_4_IOMMU_boot_test_taskout.log"
                        })
                    ],
                    "environment": TestEnvironment({}, {
                        "description": "meson-gxl-s905d-p230 in lab-baylibre",
                        "misc": {
                            "device": "meson-gxl-s905d-p230",
                            "instance": "meson-gxl-s905d-p230-sea",
                            "lab": "lab-baylibre",
                            "mach": "amlogic",
                            "rootfs_url": "https://storage.kernelci.org/images/rootfs/buildroot/kci-2019.02-9-g25091c539382/arm64/baseline/rootfs.cpio.gz"
                        }
                    }),
                    "path": "redhat_iommu_boot",
                    "start_time": "2020-03-04T21:30:57+00:00",
                    "status": "ERROR",
                    "waived": True,
                    "build_": None,
                }),
            },
        }

        oo_data = from_io(io_data)
        self.assertEqual(oo_data, expected_oo_data)

    def test_linking(self):
        """
        Check objects are linked correctly.
        """
        io_data = {
            "version": self.version,
            "revisions": [
                {
                    "id": "origin:1",
                },
                {
                    "id": "origin:2",
                },
            ],
            "builds": [
                {
                    "revision_id": "origin:1",
                    "id": "origin:1",
                },
                {
                    "revision_id": "origin:non-existent",
                    "id": "origin:2",
                },
            ],
            "tests": [
                {
                    "build_id": "origin:1",
                    "id": "origin:1",
                },
                {
                    "build_id": "origin:non-existent",
                    "id": "origin:2",
                },
            ],
        }

        revision1 = Revision({}, {"id": "origin:1",
                                  "builds_": {}})
        revision2 = Revision({}, {"id": "origin:2",
                                  "builds_": {}})
        build1 = Build({}, {"revision_id": "origin:1",
                            "id": "origin:1",
                            "revision_": revision1,
                            "tests_": {}})
        build2 = Build({}, {"revision_id": "origin:non-existent",
                            "id": "origin:2",
                            "revision_": None,
                            "tests_": {}})
        revision1.builds_[build1.id] = build1
        test1 = Test({}, {"build_id": "origin:1",
                          "build_": build1,
                          "id": "origin:1",})
        test2 = Test({}, {"build_id": "origin:non-existent",
                          "build_": None,
                          "id": "origin:2",})
        build1.tests_[test1.id] = test1
        expected_oo_data = {
            "version": self.version,
            "revisions": {
                "origin:1": revision1,
                "origin:2": revision2,
            },
            "builds": {
                "origin:1": build1,
                "origin:2": build2,
            },
            "tests": {
                "origin:1": test1,
                "origin:2": test2,
            },
        }

        oo_data = from_io(io_data)
        self.assertEqual(oo_data, expected_oo_data)

class ApplyMaskTestCase(unittest.TestCase):
    """kcidb.oo.apply_mask() test case"""

    def setUp(self):
        """Setup tests"""
        # pylint: disable=invalid-name
        self.maxDiff = None
        self.version = dict(
            major=schema.LATEST.major,
            minor=schema.LATEST.minor
        )

    def test_empty_non_empty(self):
        """
        Check empty/non-empty base/mask are handled correctly.
        """

        empty = from_io({
            "version": self.version,
        })

        non_empty = from_io({
            "version": self.version,
            "revisions": [{"id": "origin:1",},],
        })

        self.assertEqual(apply_mask(empty, empty), empty)
        self.assertEqual(apply_mask(non_empty, empty), empty)
        self.assertEqual(apply_mask(empty, non_empty), empty)
        self.assertEqual(apply_mask(non_empty, non_empty), non_empty)

    def test_mismatch(self):
        """
        Check mismatching base and mask data cancel out.
        """

        empty = from_io({
            "version": self.version,
        })

        first = from_io({
            "version": self.version,
            "revisions": [{"id": "origin:1",},],
            "builds": [{"id": "origin:1", "revision_id": "origin:1"},],
            "tests": [{"id": "origin:1", "build_id": "origin:1"},],
        })

        second = from_io({
            "version": self.version,
            "revisions": [{"id": "origin:2",},],
            "builds": [{"id": "origin:2", "revision_id": "origin:2"},],
            "tests": [{"id": "origin:2", "build_id": "origin:2"},],
        })

        self.assertEqual(apply_mask(first, second), empty)

    def test_match(self):
        """
        Check fully-matching base and mask data stay unchanged.
        """

        first = from_io({
            "version": self.version,
            "revisions": [{"id": "origin:1",},],
            "builds": [{"id": "origin:1", "revision_id": "origin:1"},],
            "tests": [{"id": "origin:1", "build_id": "origin:1"},],
        })

        second = from_io({
            "version": self.version,
            "revisions": [{"id": "origin:1",},],
            "builds": [{"id": "origin:1", "revision_id": "origin:1"},],
            "tests": [{"id": "origin:1", "build_id": "origin:1"},],
        })

        self.assertEqual(apply_mask(first, second), first)

    def test_partial_match(self):
        """
        Check partially-matching base and mask data work correctly.
        """

        first = from_io({
            "version": self.version,
            "revisions": [
                {"id": "origin:1",},
                {"id": "origin:2",},
            ],
            "builds": [
                {"id": "origin:1-1", "revision_id": "origin:1"},
                {"id": "origin:1-2", "revision_id": "origin:1"},
                {"id": "origin:2-1", "revision_id": "origin:2"},
                {"id": "origin:2-2", "revision_id": "origin:2"},
            ],
            "tests": [
                {"id": "origin:1-1-1", "build_id": "origin:1-1"},
                {"id": "origin:1-1-2", "build_id": "origin:1-1"},
                {"id": "origin:1-2-1", "build_id": "origin:1-2"},
                {"id": "origin:1-2-2", "build_id": "origin:1-2"},
                {"id": "origin:2-1-1", "build_id": "origin:2-1"},
                {"id": "origin:2-1-2", "build_id": "origin:2-1"},
                {"id": "origin:2-2-1", "build_id": "origin:2-2"},
                {"id": "origin:2-2-2", "build_id": "origin:2-2"},
            ],
        })

        second = from_io({
            "version": self.version,
            "revisions": [
                {"id": "origin:1",},
                {"id": "origin:X",},
            ],
            "builds": [
                {"id": "origin:1-1", "revision_id": "origin:1"},
                {"id": "origin:1-X", "revision_id": "origin:1"},
                {"id": "origin:2-1", "revision_id": "origin:X"},
                {"id": "origin:2-X", "revision_id": "origin:X"},
            ],
            "tests": [
                {"id": "origin:1-1-1", "build_id": "origin:1-1"},
                {"id": "origin:1-1-X", "build_id": "origin:1-1"},
                {"id": "origin:1-2-1", "build_id": "origin:1-2"},
                {"id": "origin:1-2-X", "build_id": "origin:1-2"},
                {"id": "origin:2-1-1", "build_id": "origin:2-1"},
                {"id": "origin:2-1-X", "build_id": "origin:2-1"},
                {"id": "origin:2-2-1", "build_id": "origin:2-2"},
                {"id": "origin:2-2-X", "build_id": "origin:2-2"},
            ],
        })

        expected_output = {
            "version": first["version"],
            "revisions": {
                k: first["revisions"][k]
                for k in ("origin:1", "origin:2",)
            },
            "builds": {
                k: first["builds"][k]
                for k in (
                    "origin:1-1", "origin:1-2", "origin:2-1", "origin:2-2",
                )
            },
            "tests": {
                k: first["tests"][k]
                for k in (
                    "origin:1-1-1", "origin:1-2-1",
                    "origin:2-1-1", "origin:2-2-1",
                )
            },
        }

        output = apply_mask(first, second)
        self.assertEqual(output, expected_output)

        del first["tests"]
        del second["tests"]
        expected_output = {
            "version": first["version"],
            "revisions": {
                k: first["revisions"][k]
                for k in ("origin:1", "origin:2",)
            },
            "builds": {
                k: first["builds"][k]
                for k in ("origin:1-1", "origin:2-1",)
            },
        }

        output = apply_mask(first, second)
        self.assertEqual(output, expected_output)

        del first["builds"]
        del second["builds"]
        expected_output = {
            "version": first["version"],
            "revisions": {
                k: first["revisions"][k]
                for k in ("origin:1",)
            },
        }

        output = apply_mask(first, second)
        self.assertEqual(output, expected_output)

class RemoveOrphansTestCase(unittest.TestCase):
    """kcidb.oo.remove_orphans() test case"""

    def setUp(self):
        """Setup tests"""
        # pylint: disable=invalid-name
        self.maxDiff = None
        self.version = dict(
            major=schema.LATEST.major,
            minor=schema.LATEST.minor
        )

    def test_empty(self):
        """
        Check removing orphans from empty data is a noop.
        """
        empty = {
            "version": self.version,
        }
        print(empty)
        self.assertEqual(remove_orphans(from_io(empty)), from_io(empty))

    def test_root_object(self):
        """
        Check root objects are not removed.
        """
        input_data = from_io({
            "version": self.version,
            "revisions": [{"id": "origin:1",},],
        })
        expected_output_data = {
            "version": self.version,
            "revisions": input_data["revisions"].copy()
        }
        self.assertEqual(remove_orphans(input_data), expected_output_data)

        input_data = from_io({
            "version": self.version,
            "revisions": [{"id": "origin:1",}, {"id": "origin:2",},],
        })
        expected_output_data = {
            "version": self.version,
            "revisions": input_data["revisions"].copy()
        }
        self.assertEqual(remove_orphans(input_data), expected_output_data)

    def test_non_root_orphans(self):
        """
        Check non-root orphans are removed.
        """
        # Builds
        expected_output_data = {
            "version": self.version,
            "builds": {}
        }

        # Single build
        input_data = from_io({
            "version": self.version,
            "builds": [
                {"id": "origin:1", "revision_id": "origin:1"},
            ],
        })
        self.assertEqual(remove_orphans(input_data), expected_output_data)

        # Two builds
        input_data = from_io({
            "version": self.version,
            "builds": [
                {"id": "origin:1", "revision_id": "origin:1"},
                {"id": "origin:2", "revision_id": "origin:2"},
            ],
        })
        self.assertEqual(remove_orphans(input_data), expected_output_data)

        # Tests
        expected_output_data = {
            "version": self.version,
            "tests": {}
        }

        # Single test
        input_data = from_io({
            "version": self.version,
            "tests": [
                {"id": "origin:1", "build_id": "origin:1"},
            ],
        })
        self.assertEqual(remove_orphans(input_data), expected_output_data)

        # Two tests
        input_data = from_io({
            "version": self.version,
            "tests": [
                {"id": "origin:1", "build_id": "origin:1"},
                {"id": "origin:2", "build_id": "origin:2"},
            ],
        })
        self.assertEqual(remove_orphans(input_data), expected_output_data)

        # Builds and tests
        expected_output_data = {
            "version": self.version,
            "builds": {},
            "tests": {},
        }

        # Disconnected builds and tests
        input_data = from_io({
            "version": self.version,
            "builds": [
                {"id": "origin:1", "revision_id": "origin:1"},
                {"id": "origin:2", "revision_id": "origin:2"},
            ],
            "tests": [
                {"id": "origin:1", "build_id": "origin:3"},
                {"id": "origin:2", "build_id": "origin:4"},
            ],
        })
        self.assertEqual(remove_orphans(input_data), expected_output_data)

        # Connected builds and tests
        input_data = from_io({
            "version": self.version,
            "builds": [
                {"id": "origin:1", "revision_id": "origin:1"},
                {"id": "origin:2", "revision_id": "origin:2"},
            ],
            "tests": [
                {"id": "origin:1", "build_id": "origin:1"},
                {"id": "origin:2", "build_id": "origin:2"},
            ],
        })
        self.assertEqual(remove_orphans(input_data), expected_output_data)

    def test_non_root_linked(self):
        """
        Check non-root linked objects are preserved.
        """

        # Builds
        input_data = from_io({
            "version": self.version,
            "revisions": [
                {"id": "origin:1",},
                {"id": "origin:2",},
            ],
            "builds": [
                {"id": "origin:1-1", "revision_id": "origin:1"},
                {"id": "origin:1-2", "revision_id": "origin:1"},
                {"id": "origin:2-1", "revision_id": "origin:2"},
                {"id": "origin:2-2", "revision_id": "origin:2"},
            ],
        })

        expected_output_data = {
            "version": self.version,
            "revisions": input_data["revisions"].copy(),
            "builds": input_data["builds"].copy(),
        }

        self.assertEqual(remove_orphans(input_data), expected_output_data)

        # Tests
        input_data = from_io({
            "version": self.version,
            "revisions": [
                {"id": "origin:1",},
                {"id": "origin:2",},
            ],
            "builds": [
                {"id": "origin:1-1", "revision_id": "origin:1"},
                {"id": "origin:1-2", "revision_id": "origin:1"},
                {"id": "origin:2-1", "revision_id": "origin:2"},
                {"id": "origin:2-2", "revision_id": "origin:2"},
            ],
            "tests": [
                {"id": "origin:1-1-1", "build_id": "origin:1-1"},
                {"id": "origin:1-1-2", "build_id": "origin:1-1"},
                {"id": "origin:1-2-1", "build_id": "origin:1-2"},
                {"id": "origin:1-2-2", "build_id": "origin:1-2"},
                {"id": "origin:2-1-1", "build_id": "origin:2-1"},
                {"id": "origin:2-1-2", "build_id": "origin:2-1"},
                {"id": "origin:2-2-1", "build_id": "origin:2-2"},
                {"id": "origin:2-2-2", "build_id": "origin:2-2"},
            ],
        })

        expected_output_data = {
            "version": self.version,
            "revisions": input_data["revisions"].copy(),
            "builds": input_data["builds"].copy(),
            "tests": input_data["tests"].copy(),
        }

        self.assertEqual(remove_orphans(input_data), expected_output_data)

    def test_mixed(self):
        """
        Check mixed orphaned and linked objects are handled correctly.
        """

        input_data = from_io({
            "version": self.version,
            "revisions": [
                {"id": "origin:1",},
            ],
            "builds": [
                {"id": "origin:1-1", "revision_id": "origin:1"},
                {"id": "origin:1-2", "revision_id": "origin:1"},
                {"id": "origin:X-1", "revision_id": "origin:X"},
                {"id": "origin:X-2", "revision_id": "origin:X"},
            ],
            "tests": [
                {"id": "origin:1-1-1", "build_id": "origin:1-1"},
                {"id": "origin:1-1-2", "build_id": "origin:1-1"},
                {"id": "origin:1-2-1", "build_id": "origin:1-2"},
                {"id": "origin:1-2-2", "build_id": "origin:1-2"},

                {"id": "origin:X-1-1", "build_id": "origin:X-1"},
                {"id": "origin:X-1-2", "build_id": "origin:X-1"},

                {"id": "origin:X-X-1", "build_id": "origin:X-X"},
                {"id": "origin:X-X-2", "build_id": "origin:X-X"},
            ],
        })

        expected_output_data = {
            "version": self.version,
            "revisions": {
                k: v for k, v in input_data["revisions"].items() if "X" not in k
            },
            "builds": {
                k: v for k, v in input_data["builds"].items() if "X" not in k
            },
            "tests": {
                k: v for k, v in input_data["tests"].items() if "X" not in k
            },
        }

        self.assertEqual(remove_orphans(input_data), expected_output_data)
