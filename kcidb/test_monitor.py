"""kcdib.monitor module tests"""

import os
import unittest
import kcidb
from kcidb.io import SCHEMA
from kcidb import orm, db, oo, monitor
from kcidb.unittest import local_only, deployment_only

# Disable long line checking for JSON data
# flake8: noqa
# pylint: disable=line-too-long


@local_only
class MatchTestCase(unittest.TestCase):
    """kcidb.monitor.match test case"""

    def setUp(self):
        """Setup tests"""
        # pylint: disable=invalid-name
        self.maxDiff = None
        self.version = dict(
            major=SCHEMA.major,
            minor=SCHEMA.minor
        )

    @staticmethod
    def match(io_data, pattern=">*#"):
        """
        Match subscriptions to I/O data and generate notifications.

        Args:
            io_data:    The I/O data to match subscriptions to.
            pattern:    The ORM pattern string matching I/O data's objects to
                        match. Default is all objects.

        Returns:
            A tuple of generated notifications.
        """
        assert SCHEMA.is_valid(io_data)
        db_client = db.Client("sqlite::memory:")
        db_client.init()
        oo_client = oo.Client(db_client)
        db_client.load(io_data)
        return monitor.match(oo_client.query(orm.Pattern.parse(pattern)))

    def test_min(self):
        """Check minimal matching"""

        notifications = self.match({
            "version": self.version,
            "checkouts": [
                {
                    "contacts": [
                        "rdma-dev-team@redhat.com"
                    ],
                    "start_time": "2020-03-02T15:16:15.790000+00:00",
                    "git_repository_branch": "wip/jgg-for-next",
                    "git_commit_hash": "5e29d1443c46b6ca70a4c940a67e8c09f05dcb7e",
                    "patchset_hash": "",
                    "git_repository_url": "git://git.kernel.org/pub/scm/linux/kernel/git/rdma/rdma.git",
                    "misc": {
                        "pipeline_id": 467715
                    },
                    "id": "non_test:1",
                    "origin": "non_test",
                    "patchset_files": [],
                    "valid": True,
                },
                {
                    "contacts": [
                        "rdma-dev-team@redhat.com"
                    ],
                    "start_time": "2020-03-02T15:16:15.790000+00:00",
                    "git_repository_branch": "wip/jgg-for-next",
                    "git_commit_hash": "1254e88b4fc1470d152f494c3590bb6a33ab33eb",
                    "patchset_hash": "",
                    "git_repository_url": "git://git.kernel.org/pub/scm/linux/kernel/git/rdma/rdma.git",
                    "misc": {
                        "pipeline_id": 467715
                    },
                    "id": "test:1",
                    "origin": "test",
                    "patchset_files": [],
                    "valid": True,
                },
            ],
            "builds": [
                {
                    "architecture": "aarch64",
                    "command": "make -j30 INSTALL_MOD_STRIP=1 targz-pkg",
                    "compiler": "aarch64-linux-gnu-gcc (GCC) 9.2.1 20190827 (Red Hat Cross 9.2.1-1)",
                    "config_name": "fedora",
                    "duration": 237.0,
                    "id": "non_test:1",
                    "origin": "non_test",
                    "input_files": [],
                    "log_url": "https://cki-artifacts.s3.amazonaws.com/datawarehouse/2020/03/03/469720/build_aarch64.log",
                    "misc": {
                        "job_id": 678223,
                        "pipeline_id": 469720
                    },
                    "output_files": [],
                    "checkout_id": "test:1",
                    "start_time": "2020-03-03T17:52:02.370000+00:00",
                    "valid": True
                },
                {
                    "architecture": "aarch64",
                    "command": "make -j30 INSTALL_MOD_STRIP=1 targz-pkg",
                    "compiler": "aarch64-linux-gnu-gcc (GCC) 9.2.1 20190827 (Red Hat Cross 9.2.1-1)",
                    "config_name": "fedora",
                    "duration": 237.0,
                    "id": "test:1",
                    "origin": "test",
                    "input_files": [],
                    "log_url": "https://cki-artifacts.s3.amazonaws.com/datawarehouse/2020/03/03/469720/build_aarch64.log",
                    "misc": {
                        "job_id": 678223,
                        "pipeline_id": 469720
                    },
                    "output_files": [],
                    "checkout_id": "test:1",
                    "start_time": "2020-03-03T17:52:02.370000+00:00",
                    "valid": True
                },
            ],
            "tests": [
                {
                    "build_id": "redhat:679936",
                    "comment": "IOMMU boot test",
                    "duration": 1847.0,
                    "id": "non_test:1",
                    "origin": "non_test",
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
                        "comment": "meson-gxl-s905d-p230 in lab-baylibre",
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
                {
                    "build_id": "redhat:679936",
                    "comment": "IOMMU boot test",
                    "duration": 1847.0,
                    "id": "test:1",
                    "origin": "test",
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
                        "comment": "meson-gxl-s905d-p230 in lab-baylibre",
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
        })
        self.assertEqual(len(notifications), 4)
        for notification in notifications:
            obj_type_name = notification.obj.get_type().name
            self.assertIsInstance(notification, monitor.output.Notification)
            message = notification.render()
            self.assertIsNone(message['From'])
            self.assertEqual(message['To'], "test@kernelci.org")
            self.assertEqual(message['X-KCIDB-Notification-Message-ID'],
                             obj_type_name)
            self.assertIn(f"Test {obj_type_name}: ", message['Subject'])
            text, html = message.get_payload()
            self.assertEqual('text/plain', text.get_content_type())
            self.assertEqual('utf-8', text.get_content_charset())
            content = text.get_payload()
            self.assertIn(f"Test {obj_type_name} detected!\n\n",
                          content)
            self.assertEqual('text/html', html.get_content_type())
            self.assertEqual('utf-8', html.get_content_charset())
            content = html.get_payload()
            self.assertIn(f"Test {obj_type_name} detected!\n\n",
                          content)

    def test_mark_brown(self):
        """Check Mark Brown's subscription works"""
        notifications = self.match({
            "version": {
                "major": 4,
                "minor": 0
            },
            "checkouts": [
                {
                    "id": "_:redhat:d58071a8a76d779eedab38033ae4c821c30295a5",
                    "origin": "redhat",
                    "tree_name": "arm-next",
                    "git_repository_url": "https://git.kernel.org/pub/scm/linux/kernel/git/arm64/linux.git",
                    "git_commit_hash": "d58071a8a76d779eedab38033ae4c821c30295a5",
                    "git_repository_branch": "for-kernelci",
                    "patchset_files": [],
                    "patchset_hash": "",
                    "start_time": "2021-11-29T14:36:59.751000+00:00",
                    "contacts": [],
                    "valid": True
                },
                {
                    "id": "_:kernelci:383a44aec91c327ef4a9d03cfa65d1eaf3746c06",
                    "origin": "kernelci",
                    "git_repository_url": "https://git.kernel.org/pub/scm/linux/kernel/git/soc/soc.git",
                    "git_commit_hash": "383a44aec91c327ef4a9d03cfa65d1eaf3746c06",
                    "git_commit_name": "v5.16-rc2-19-g383a44aec91c",
                    "git_repository_branch": "arm/fixes",
                    "patchset_files": [],
                    "patchset_hash": "",
                    "start_time": "2021-11-26T01:27:55.210000+00:00",
                    "contacts": [],
                    "valid": True
                }
            ],
            "builds": [
                {
                    "id": "kernelci:kernelci.org:619fecfc6d932a49b5f2efb0",
                    "checkout_id": "_:kernelci:383a44aec91c327ef4a9d03cfa65d1eaf3746c06",
                    "origin": "kernelci",
                    "comment": "v5.16-rc2-19-g383a44aec91c",
                    "start_time": "2021-11-25T20:07:24.499000+00:00",
                    "duration": 486.695294857,
                    "architecture": "x86_64",
                    "config_name": "x86_64_defconfig+x86-chromebook+kselftest",
                    "valid": True
                },
                {
                    "id": "redhat:1827126781",
                    "checkout_id": "_:redhat:d58071a8a76d779eedab38033ae4c821c30295a5",
                    "origin": "redhat",
                    "start_time": "2021-11-28T22:49:02.797230+00:00",
                    "architecture": "ppc64le",
                    "command": "make -j24 INSTALL_MOD_STRIP=1 targz-pkg",
                    "config_name": "fedora",
                    "valid": False
                }
            ],
            "tests": [
                {
                    "id": "kernelci:kernelci.org:619fee1d79537d9bc8f2efb4",
                    "build_id": "kernelci:kernelci.org:619fecfc6d932a49b5f2efb0",
                    "origin": "kernelci",
                    "path": "ltp-timers.clock_getcpuclockid",
                    "comment": "ltp-timers on asus-C433TA-AJ0005-rammus in lab-collabora",
                    "status": "FAIL",
                    "waived": False,
                    "start_time": "2021-11-25T20:12:13.929000+00:00"
                }
            ]
        })
        subjects = sorted([
                n.message.subject
                for n in notifications
                if n.subscription == "mark_brown" and
                    "Mark Brown <broonie@kernel.org>" in n.message.to
        ])
        self.assertEqual(len(subjects), 2)
        self.assertRegex(subjects[0], r"^Testing done for ")
        self.assertRegex(subjects[1], r"^Testing done for ")


@deployment_only
class DeploymentEmailTestCase(unittest.TestCase):
    """Deployment email generation test case"""

    @deployment_only
    def test_email_generated(self):
        """Check appropriate email is generated for "test" subscription"""

        client = kcidb.Client(
            project_id=os.environ["GCP_PROJECT"],
            topic_name=os.environ["KCIDB_LOAD_QUEUE_TOPIC"]
        )
        email_subscriber = kcidb.mq.EmailSubscriber(
            os.environ["GCP_PROJECT"],
            os.environ["KCIDB_SMTP_TOPIC"],
            os.environ["KCIDB_SMTP_SUBSCRIPTION"]
        )
        io_data = {
            "version": {
                "major": 4,
                "minor": 0
            },
            "checkouts": [
                {
                    "contacts": [
                        "rdma-dev-team@redhat.com"
                    ],
                    "start_time": "2020-03-02T15:16:15.790000+00:00",
                    "git_repository_branch": "wip/jgg-for-next",
                    "git_commit_hash": "5e29d1443c46b6ca70a4c940a67e8c09f05dcb7e",
                    "patchset_hash": "",
                    "git_repository_url": "git://git.kernel.org/pub/scm/linux/kernel/git/rdma/rdma.git",
                    "misc": {
                        "pipeline_id": 467715
                    },
                    "id": "non_test:1",
                    "origin": "non_test",
                    "patchset_files": [],
                    "valid": True,
                },
                {
                    "contacts": [
                        "rdma-dev-team@redhat.com"
                    ],
                    "start_time": "2020-03-02T15:16:15.790000+00:00",
                    "git_repository_branch": "wip/jgg-for-next",
                    "git_commit_hash": "1254e88b4fc1470d152f494c3590bb6a33ab33eb",
                    "patchset_hash": "",
                    "git_repository_url": "git://git.kernel.org/pub/scm/linux/kernel/git/rdma/rdma.git",
                    "misc": {
                        "pipeline_id": 467715
                    },
                    "id": "test:1",
                    "origin": "test",
                    "patchset_files": [],
                    "valid": True,
                },
            ],
            "builds": [
                {
                    "architecture": "aarch64",
                    "command": "make -j30 INSTALL_MOD_STRIP=1 targz-pkg",
                    "compiler": "aarch64-linux-gnu-gcc (GCC) 9.2.1 20190827 (Red Hat Cross 9.2.1-1)",
                    "config_name": "fedora",
                    "duration": 237.0,
                    "id": "non_test:1",
                    "origin": "non_test",
                    "input_files": [],
                    "log_url": "https://cki-artifacts.s3.amazonaws.com/datawarehouse/2020/03/03/469720/build_aarch64.log",
                    "misc": {
                        "job_id": 678223,
                        "pipeline_id": 469720
                    },
                    "output_files": [],
                    "checkout_id": "test:1",
                    "start_time": "2020-03-03T17:52:02.370000+00:00",
                    "valid": True
                },
                {
                    "architecture": "aarch64",
                    "command": "make -j30 INSTALL_MOD_STRIP=1 targz-pkg",
                    "compiler": "aarch64-linux-gnu-gcc (GCC) 9.2.1 20190827 (Red Hat Cross 9.2.1-1)",
                    "config_name": "fedora",
                    "duration": 237.0,
                    "id": "test:1",
                    "origin": "test",
                    "input_files": [],
                    "log_url": "https://cki-artifacts.s3.amazonaws.com/datawarehouse/2020/03/03/469720/build_aarch64.log",
                    "misc": {
                        "job_id": 678223,
                        "pipeline_id": 469720
                    },
                    "output_files": [],
                    "checkout_id": "test:1",
                    "start_time": "2020-03-03T17:52:02.370000+00:00",
                    "valid": True
                },
            ],
            "tests": [
                {
                    "build_id": "redhat:679936",
                    "comment": "IOMMU boot test",
                    "duration": 1847.0,
                    "id": "non_test:1",
                    "origin": "non_test",
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
                        "comment": "meson-gxl-s905d-p230 in lab-baylibre",
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
                {
                    "build_id": "redhat:679936",
                    "comment": "IOMMU boot test",
                    "duration": 1847.0,
                    "id": "test:1",
                    "origin": "test",
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
                        "comment": "meson-gxl-s905d-p230 in lab-baylibre",
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

        # Submit data to submission queue
        client.submit(io_data)
        # Try to pull the four notification messages we're expecting and
        # check we get one correct message per object type
        obj_types = {"revision", "checkout", "build", "test"}
        for ack_id, email in email_subscriber.pull_iter(4, 1800):
            email_subscriber.ack(ack_id)
            self.assertEqual(email['From'], "bot@kernelci.org")
            self.assertEqual(email['To'], "test@kernelci.org")
            obj_type = email['X-KCIDB-Notification-Message-ID']
            self.assertIn(obj_type, obj_types)
            obj_types.remove(obj_type)
            self.assertIn(f"Test {obj_type}: ", email['Subject'])
            text, html = email.get_payload()
            self.assertEqual('text/plain', text.get_content_type())
            self.assertEqual('utf-8', text.get_content_charset())
            content = text.get_content()
            self.assertIn(f"Test {obj_type} detected!\r\n\r\n", content)
            self.assertEqual('text/html', html.get_content_type())
            self.assertEqual('utf-8', html.get_content_charset())
            content = html.get_content()
            self.assertIn(f"Test {obj_type} detected!\r\n\r\n", content)
        # Check we got all four
        self.assertEqual(len(obj_types), 0)
        # Check we get no more notification messages
        self.assertEqual(len(email_subscriber.pull(1, 5)), 0)
