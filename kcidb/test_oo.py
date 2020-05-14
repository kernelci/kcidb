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

IO_REVISION1 = {
    "id":
        "https://git.kernel.org/pub/scm/linux/kernel/git/stable/"
        "linux-stable.git@aa73bcc376865c23e61dcebd467697b527901be8",
    "origin":
        "origin",
}

IO_REVISION2 = {
    "id":
        "https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/"
        "linux.git@c0d73a868d9b411bd2d0c8e5ff9d98bfa8563cb1"
        "+903638c087335b10293663c682b9aa0076f9f7be478a8e782"
        "8bc22e12d301b42",
    "origin":
        "origin",
}

IO_REVISION3 = {
    "id":
        "https://git.kernel.org/pub/scm/linux/kernel/git/stable/"
        "linux-stable.git@7f74c309d3e46088e6606183d15aba89539b650d",
    "origin":
        "origin",
}


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
                    "id": "git://git.kernel.org/pub/scm/linux/kernel/git/rdma/rdma.git@5e29d1443c46b6ca70a4c940a67e8c09f05dcb7e",
                    "origin": "redhat",
                    "patch_mboxes": [],
                    "valid": True,
                },
            ],
        }

        expected_oo_data = {
            "version": self.version,
            "revisions": {
                "git://git.kernel.org/pub/scm/linux/kernel/git/rdma/rdma.git@5e29d1443c46b6ca70a4c940a67e8c09f05dcb7e":
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
                    "id": "git://git.kernel.org/pub/scm/linux/kernel/git/rdma/rdma.git@5e29d1443c46b6ca70a4c940a67e8c09f05dcb7e",
                    "origin": "redhat",
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
                    "origin": "redhat",
                    "input_files": [],
                    "log_url": "https://cki-artifacts.s3.amazonaws.com/datawarehouse/2020/03/03/469720/build_aarch64.log",
                    "misc": {
                        "job_id": 678223,
                        "pipeline_id": 469720
                    },
                    "output_files": [],
                    "revision_id": "https://git.kernel.org/pub/scm/linux/kernel/git/stable/linux-stable-rc.git@1254e88b4fc1470d152f494c3590bb6a33ab33eb",
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
                    "origin": "redhat",
                    "input_files": [],
                    "log_url": "https://cki-artifacts.s3.amazonaws.com/datawarehouse/2020/03/03/469720/build_aarch64.log",
                    "misc": {
                        "job_id": 678223,
                        "pipeline_id": 469720
                    },
                    "output_files": [],
                    "revision_id": "https://git.kernel.org/pub/scm/linux/kernel/git/stable/linux-stable-rc.git@1254e88b4fc1470d152f494c3590bb6a33ab33eb",
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
                    "origin": "redhat",
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
                    "origin": "redhat",
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
            "revisions": [IO_REVISION1, IO_REVISION2,],
            "builds": [
                {
                    "revision_id": IO_REVISION1["id"],
                    "id": "origin:1",
                    "origin": "origin",
                },
                {
                    "revision_id": IO_REVISION3["id"],
                    "id": "origin:2",
                    "origin": "origin",
                },
            ],
            "tests": [
                {
                    "build_id": "origin:1",
                    "id": "origin:1",
                    "origin": "origin",
                },
                {
                    "build_id": "origin:non-existent",
                    "id": "origin:2",
                    "origin": "origin",
                },
            ],
        }

        revision1_attrs = {"builds_": {}}
        revision1_attrs.update(IO_REVISION1)
        revision1 = Revision({}, revision1_attrs)
        revision2_attrs = {"builds_": {}}
        revision2_attrs.update(IO_REVISION2)
        revision2 = Revision({}, revision2_attrs)
        build1 = Build({}, {"revision_id": IO_REVISION1["id"],
                            "id": "origin:1",
                            "origin": "origin",
                            "revision_": revision1,
                            "tests_": {}})
        build2 = Build({}, {"revision_id": IO_REVISION3["id"],
                            "id": "origin:2",
                            "origin": "origin",
                            "revision_": None,
                            "tests_": {}})
        revision1.builds_[build1.id] = build1
        test1 = Test({}, {"build_id": "origin:1",
                          "build_": build1,
                          "id": "origin:1",
                          "origin": "origin",})
        test2 = Test({}, {"build_id": "origin:non-existent",
                          "build_": None,
                          "id": "origin:2",
                          "origin": "origin",})
        build1.tests_[test1.id] = test1
        expected_oo_data = {
            "version": self.version,
            "revisions": {
                IO_REVISION1["id"]: revision1,
                IO_REVISION2["id"]: revision2,
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
            "revisions": [IO_REVISION1,],
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
            "revisions": [IO_REVISION1,],
            "builds": [{"id": "origin:1", "origin": "origin",
                        "revision_id": IO_REVISION1["id"]},],
            "tests": [{"id": "origin:1", "origin": "origin",
                       "build_id": "origin:1"},],
        })

        second = from_io({
            "version": self.version,
            "revisions": [IO_REVISION2,],
            "builds": [{"id": "origin:2", "origin": "origin",
                        "revision_id": IO_REVISION2["id"]},],
            "tests": [{"id": "origin:2", "origin": "origin",
                       "build_id": "origin:2"},],
        })

        self.assertEqual(apply_mask(first, second), empty)

    def test_match(self):
        """
        Check fully-matching base and mask data stay unchanged.
        """

        first = from_io({
            "version": self.version,
            "revisions": [IO_REVISION1,],
            "builds": [{"id": "origin:1", "origin": "origin",
                        "revision_id": IO_REVISION1["id"]},],
            "tests": [{"id": "origin:1", "origin": "origin",
                       "build_id": "origin:1"},],
        })

        second = from_io({
            "version": self.version,
            "revisions": [IO_REVISION1,],
            "builds": [{"id": "origin:1", "origin": "origin",
                        "revision_id": IO_REVISION1["id"]},],
            "tests": [{"id": "origin:1", "origin": "origin",
                       "build_id": "origin:1"},],
        })

        self.assertEqual(apply_mask(first, second), first)

    def test_partial_match(self):
        """
        Check partially-matching base and mask data work correctly.
        """

        first = from_io({
            "version": self.version,
            "revisions": [IO_REVISION1, IO_REVISION2,],
            "builds": [
                {"id": "origin:1-1", "origin": "origin",
                 "revision_id": IO_REVISION1["id"]},
                {"id": "origin:1-2", "origin": "origin",
                 "revision_id": IO_REVISION1["id"]},
                {"id": "origin:2-1", "origin": "origin",
                 "revision_id": IO_REVISION2["id"]},
                {"id": "origin:2-2", "origin": "origin",
                 "revision_id": IO_REVISION2["id"]},
            ],
            "tests": [
                {"id": "origin:1-1-1", "origin": "origin",
                 "build_id": "origin:1-1"},
                {"id": "origin:1-1-2", "origin": "origin",
                 "build_id": "origin:1-1"},
                {"id": "origin:1-2-1", "origin": "origin",
                 "build_id": "origin:1-2"},
                {"id": "origin:1-2-2", "origin": "origin",
                 "build_id": "origin:1-2"},
                {"id": "origin:2-1-1", "origin": "origin",
                 "build_id": "origin:2-1"},
                {"id": "origin:2-1-2", "origin": "origin",
                 "build_id": "origin:2-1"},
                {"id": "origin:2-2-1", "origin": "origin",
                 "build_id": "origin:2-2"},
                {"id": "origin:2-2-2", "origin": "origin",
                 "build_id": "origin:2-2"},
            ],
        })

        second = from_io({
            "version": self.version,
            "revisions": [IO_REVISION1, IO_REVISION3,],
            "builds": [
                {"id": "origin:1-1", "origin": "origin",
                 "revision_id": IO_REVISION1["id"]},
                {"id": "origin:1-X", "origin": "origin",
                 "revision_id": IO_REVISION1["id"]},
                {"id": "origin:2-1", "origin": "origin",
                 "revision_id": IO_REVISION3["id"]},
                {"id": "origin:2-X", "origin": "origin",
                 "revision_id": IO_REVISION3["id"]},
            ],
            "tests": [
                {"id": "origin:1-1-1", "origin": "origin",
                 "build_id": "origin:1-1"},
                {"id": "origin:1-1-X", "origin": "origin",
                 "build_id": "origin:1-1"},
                {"id": "origin:1-2-1", "origin": "origin",
                 "build_id": "origin:1-2"},
                {"id": "origin:1-2-X", "origin": "origin",
                 "build_id": "origin:1-2"},
                {"id": "origin:2-1-1", "origin": "origin",
                 "build_id": "origin:2-1"},
                {"id": "origin:2-1-X", "origin": "origin",
                 "build_id": "origin:2-1"},
                {"id": "origin:2-2-1", "origin": "origin",
                 "build_id": "origin:2-2"},
                {"id": "origin:2-2-X", "origin": "origin",
                 "build_id": "origin:2-2"},
            ],
        })

        expected_output = {
            "version": first["version"],
            "revisions": {
                k: first["revisions"][k]
                for k in (IO_REVISION1["id"], IO_REVISION2["id"],)
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
                for k in (IO_REVISION1["id"], IO_REVISION2["id"],)
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
                for k in (IO_REVISION1["id"],)
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
            "revisions": [IO_REVISION1,],
        })
        expected_output_data = {
            "version": self.version,
            "revisions": input_data["revisions"].copy()
        }
        self.assertEqual(remove_orphans(input_data), expected_output_data)

        input_data = from_io({
            "version": self.version,
            "revisions": [IO_REVISION1, IO_REVISION2,],
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
                {"id": "origin:1", "origin": "origin",
                 "revision_id": IO_REVISION1["id"]},
            ],
        })
        self.assertEqual(remove_orphans(input_data), expected_output_data)

        # Two builds
        input_data = from_io({
            "version": self.version,
            "builds": [
                {"id": "origin:1", "origin": "origin",
                 "revision_id": IO_REVISION1["id"]},
                {"id": "origin:2", "origin": "origin",
                 "revision_id": IO_REVISION2["id"]},
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
                {"id": "origin:1", "origin": "origin",
                 "build_id": "origin:1"},
            ],
        })
        self.assertEqual(remove_orphans(input_data), expected_output_data)

        # Two tests
        input_data = from_io({
            "version": self.version,
            "tests": [
                {"id": "origin:1", "origin": "origin",
                 "build_id": "origin:1"},
                {"id": "origin:2", "origin": "origin",
                 "build_id": "origin:2"},
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
                {"id": "origin:1", "origin": "origin",
                 "revision_id": IO_REVISION1["id"]},
                {"id": "origin:2", "origin": "origin",
                 "revision_id": IO_REVISION2["id"]},
            ],
            "tests": [
                {"id": "origin:1", "origin": "origin",
                 "build_id": "origin:3"},
                {"id": "origin:2", "origin": "origin",
                 "build_id": "origin:4"},
            ],
        })
        self.assertEqual(remove_orphans(input_data), expected_output_data)

        # Connected builds and tests
        input_data = from_io({
            "version": self.version,
            "builds": [
                {"id": "origin:1", "origin": "origin",
                 "revision_id": IO_REVISION1["id"]},
                {"id": "origin:2", "origin": "origin",
                 "revision_id": IO_REVISION2["id"]},
            ],
            "tests": [
                {"id": "origin:1", "origin": "origin",
                 "build_id": "origin:1"},
                {"id": "origin:2", "origin": "origin",
                 "build_id": "origin:2"},
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
            "revisions": [IO_REVISION1, IO_REVISION2,],
            "builds": [
                {"id": "origin:1-1", "origin": "origin",
                 "revision_id": IO_REVISION1["id"]},
                {"id": "origin:1-2", "origin": "origin",
                 "revision_id": IO_REVISION1["id"]},
                {"id": "origin:2-1", "origin": "origin",
                 "revision_id": IO_REVISION2["id"]},
                {"id": "origin:2-2", "origin": "origin",
                 "revision_id": IO_REVISION2["id"]},
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
            "revisions": [IO_REVISION1, IO_REVISION2,],
            "builds": [
                {"id": "origin:1-1", "origin": "origin",
                 "revision_id": IO_REVISION1["id"]},
                {"id": "origin:1-2", "origin": "origin",
                 "revision_id": IO_REVISION1["id"]},
                {"id": "origin:2-1", "origin": "origin",
                 "revision_id": IO_REVISION2["id"]},
                {"id": "origin:2-2", "origin": "origin",
                 "revision_id": IO_REVISION2["id"]},
            ],
            "tests": [
                {"id": "origin:1-1-1", "origin": "origin",
                 "build_id": "origin:1-1"},
                {"id": "origin:1-1-2", "origin": "origin",
                 "build_id": "origin:1-1"},
                {"id": "origin:1-2-1", "origin": "origin",
                 "build_id": "origin:1-2"},
                {"id": "origin:1-2-2", "origin": "origin",
                 "build_id": "origin:1-2"},
                {"id": "origin:2-1-1", "origin": "origin",
                 "build_id": "origin:2-1"},
                {"id": "origin:2-1-2", "origin": "origin",
                 "build_id": "origin:2-1"},
                {"id": "origin:2-2-1", "origin": "origin",
                 "build_id": "origin:2-2"},
                {"id": "origin:2-2-2", "origin": "origin",
                 "build_id": "origin:2-2"},
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
            "revisions": [IO_REVISION1,],
            "builds": [
                {"id": "origin:1-1", "origin": "origin",
                 "revision_id": IO_REVISION1["id"]},
                {"id": "origin:1-2", "origin": "origin",
                 "revision_id": IO_REVISION1["id"]},
                {"id": "origin:X-1", "origin": "origin",
                 "revision_id": IO_REVISION3["id"]},
                {"id": "origin:X-2", "origin": "origin",
                 "revision_id": IO_REVISION3["id"]},
            ],
            "tests": [
                {"id": "origin:1-1-1", "origin": "origin",
                 "build_id": "origin:1-1"},
                {"id": "origin:1-1-2", "origin": "origin",
                 "build_id": "origin:1-1"},
                {"id": "origin:1-2-1", "origin": "origin",
                 "build_id": "origin:1-2"},
                {"id": "origin:1-2-2", "origin": "origin",
                 "build_id": "origin:1-2"},

                {"id": "origin:X-1-1", "origin": "origin",
                 "build_id": "origin:X-1"},
                {"id": "origin:X-1-2", "origin": "origin",
                 "build_id": "origin:X-1"},

                {"id": "origin:X-X-1", "origin": "origin",
                 "build_id": "origin:X-X"},
                {"id": "origin:X-X-2", "origin": "origin",
                 "build_id": "origin:X-X"},
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
