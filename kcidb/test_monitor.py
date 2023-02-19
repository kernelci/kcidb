"""kcdib.monitor module tests"""

import re
import os
import unittest
import kcidb
from kcidb.io import SCHEMA
from kcidb import io, orm, db, oo, monitor

# Disable long line checking for JSON data
# flake8: noqa
# pylint: disable=line-too-long


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


def test_min():
    """Check minimal matching"""

    notifications = match({
        "version": dict(major=SCHEMA.major, minor=SCHEMA.minor),
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
        "issues": [
            {
                "id": "test:1",
                "version": 1,
                "origin": "test",
                "report_url": "https://test.com/bug/1",
                "report_subject": "Bug in kernel",
                "culprit": {"code": True},
            },
            {
                "id": "non_test:1",
                "version": 1,
                "origin": "non_test",
                "report_url": "https://non-test.com/bug/1",
                "report_subject": "Another bug in kernel",
                "culprit": {"tool": True},
            },
        ],
        "incidents": [
            {
                "id": "test:1",
                "issue_id": "test:1",
                "issue_version": 1,
                "origin": "test",
                "test_id": "test:1",
                "present": True,
            },
            {
                "id": "non_test:1",
                "issue_id": "non_test:1",
                "issue_version": 1,
                "origin": "non_test",
                "test_id": "non_test:1",
                "present": True,
            },
        ]
    })
    assert len(notifications) == 7
    for notification in notifications:
        obj_type_name = notification.obj.get_type().name
        assert isinstance(notification, monitor.output.Notification)
        message = notification.render()
        assert message['From'] is None
        assert message['To'] == "test@kernelci.org"
        assert message['X-KCIDB-Notification-Message-ID'] == obj_type_name
        assert f"Test {obj_type_name}: " in message['Subject']
        text, html = message.get_payload()
        assert 'text/plain' == text.get_content_type()
        assert 'utf-8' == text.get_content_charset()
        content = text.get_payload()
        assert f"Test {obj_type_name} detected!\n\n" in content
        assert 'text/html' == html.get_content_type()
        assert 'utf-8' == html.get_content_charset()
        content = html.get_payload()
        assert f"Test {obj_type_name} detected!\n\n" in content


def test_mark_brown():
    """Check Mark Brown's subscription works"""
    notifications = match({
        "version": {
            "major": 4,
            "minor": 1
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
    assert len(subjects) == 2
    assert re.match(r"^Testing done for ", subjects[0])
    assert re.match(r"^Testing done for ", subjects[1])


@unittest.skipUnless(
    os.environ.get("KCIDB_UPDATED_PUBLISH", ""),
    "Updates about loaded data are disabled"
)
@unittest.skipUnless(
    os.environ.get("KCIDB_SMTP_TOPIC", ""),
    "SMTP is not mocked"
)
def test_email_generated(empty_deployment):
    """Check appropriate email is generated for "test" subscription"""
    # It's a test, pylint: disable=too-many-locals

    # Calm down please, pylint
    assert empty_deployment is None


    client = kcidb.Client(
        project_id=os.environ["GCP_PROJECT"],
        topic_name=os.environ["KCIDB_LOAD_QUEUE_TOPIC"]
    )
    db_client = db.Client(os.environ["KCIDB_DATABASE"])
    email_subscriber = kcidb.mq.EmailSubscriber(
        os.environ["GCP_PROJECT"],
        os.environ["KCIDB_SMTP_TOPIC"],
        os.environ["KCIDB_SMTP_SUBSCRIPTION"]
    )

    io_data_v3_0 = {
        "version": {
            "major": 3,
            "minor": 0
        },
        "revisions": [{
            "id": "f00af9d68ed146b47fdbfe91134fcf04c36e6d78",
            "origin": "test",
            "git_repository_url":
                "https://android.googlesource.com/kernel/common.git",
            "git_commit_hash":
                "f00af9d68ed146b47fdbfe91134fcf04c36e6d78",
            "git_commit_name":
                "ASB-2023-01-05_mainline-9680-gf00af9d68ed1",
            "discovery_time": "2023-01-27T08:27:50.000000+00:00",
            "valid": True,
            "git_repository_branch": "android-mainline"
        }],
        "builds": [{
            "id": "test:2KtyFbORDouvFKy49kQtfgCmcac",
            "revision_id": "f00af9d68ed146b47fdbfe91134fcf04c36e6d78",
            "origin": "test",
            "architecture": "x86_64",
            "compiler": "Debian clang version 15.0.7",
            "config_name": "gki_defconfig",
            "config_url":
                "https://storage.tuxsuite.com/public/"
                "clangbuiltlinux/continuous-integration2/builds/"
                "2KtyFbORDouvFKy49kQtfgCmcac/config",
            "log_url":
                "https://storage.tuxsuite.com/public/"
                "clangbuiltlinux/continuous-integration2/builds/"
                "2KtyFbORDouvFKy49kQtfgCmcac/build.log",
            "start_time": "2023-01-27T08:27:50.000000+00:00",
            "valid": True
        }],
        "tests": [{
            "build_id":
                "test:e716fd2a536671b69625b2536ebe9ede623b93b4",
            "description": "INFO: task hung in ipv6_route_ioctl (2)",
            "id": "test:bf7c6406637722a401e0",
            "misc": {
                "origin_url":
                    "https://syzkaller.appspot.com/bug?"
                    "extid=bf7c6406637722a401e0",
                "reported_by":
                    "syzbot+bf7c6406637722a401e0@"
                    "syzkaller.appspotmail.com"
            },
            "origin": "test",
            "output_files": [
                {
                    "name": "report.txt",
                    "url":
                        "https://syzkaller.appspot.com/x/report.txt?"
                        "x=1262b549480000"
                },
                {
                    "name": "log.txt",
                    "url":
                        "https://syzkaller.appspot.com/x/log.txt?"
                        "x=10ab93cd480000"
                },
                {
                    "name": "machine_info.txt",
                    "url": "https://syzkaller.appspot.com/x/"
                        "minfo.txt?x=75affc83eb386f34"
                }
            ],
            "path": "syzkaller",
            "start_time": "2023-01-28T02:21:00.000000+00:00",
            "status": "FAIL",
            "waived": False
        }],
    }
    obj_types_v3_0 = {"revision", "checkout", "build", "test"}

    io_data_v4_0 = {
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
                "git_commit_hash": "1254e88b4fc1470d152f494c3590bb6a33ba0bab",
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
                "build_id": "non_test:1",
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
                "build_id": "test:1",
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
    obj_types_v4_0 = obj_types_v3_0

    io_data_v4_1 = {
        "version": {
            "major": 4,
            "minor": 1
        },
        "checkouts": [
            {
                "contacts": [
                    "rdma-dev-team@redhat.com"
                ],
                "start_time": "2020-03-02T15:16:15.790000+00:00",
                "git_repository_branch": "wip/jgg-for-next",
                "git_commit_hash": "5e29d1443c46b6ca70a4c940a67e8c09f05ad334",
                "patchset_hash": "",
                "git_repository_url": "git://git.kernel.org/pub/scm/linux/kernel/git/rdma/rdma.git",
                "misc": {
                    "pipeline_id": 467715
                },
                "id": "non_test:2",
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
                "id": "test:2",
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
                "id": "non_test:2",
                "origin": "non_test",
                "input_files": [],
                "log_url": "https://cki-artifacts.s3.amazonaws.com/datawarehouse/2020/03/03/469720/build_aarch64.log",
                "misc": {
                    "job_id": 678223,
                    "pipeline_id": 469720
                },
                "output_files": [],
                "checkout_id": "test:2",
                "start_time": "2020-03-03T17:52:02.370000+00:00",
                "valid": True
            },
            {
                "architecture": "aarch64",
                "command": "make -j30 INSTALL_MOD_STRIP=1 targz-pkg",
                "compiler": "aarch64-linux-gnu-gcc (GCC) 9.2.1 20190827 (Red Hat Cross 9.2.1-1)",
                "config_name": "fedora",
                "duration": 237.0,
                "id": "test:2",
                "origin": "test",
                "input_files": [],
                "log_url": "https://cki-artifacts.s3.amazonaws.com/datawarehouse/2020/03/03/469720/build_aarch64.log",
                "misc": {
                    "job_id": 678223,
                    "pipeline_id": 469720
                },
                "output_files": [],
                "checkout_id": "test:2",
                "start_time": "2020-03-03T17:52:02.370000+00:00",
                "valid": True
            },
        ],
        "tests": [
            {
                "build_id": "non_test:2",
                "comment": "IOMMU boot test",
                "duration": 1847.0,
                "id": "non_test:2",
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
                "build_id": "test:2",
                "comment": "IOMMU boot test",
                "duration": 1847.0,
                "id": "test:2",
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
        "issues": [
            {
                "id": "non_test:2",
                "origin": "non_test",
                "version": 1,
                "report_url": "https://kernelci.org/issue/1",
                "report_subject": "Bad issue",
            },
            {
                "id": "test:2",
                "origin": "test",
                "version": 1,
                "report_url": "https://bugzilla.redhat.com/1",
                "report_subject": "Worse issue",
            },
        ],
        "incidents": [
            {
                "id": "non_test:2:1",
                "origin": "non_test",
                "issue_id": "non_test:2",
                "issue_version": 1,
                "present": True,
                "test_id": "non_test:2",
            },
            {
                "id": "test:2",
                "origin": "test",
                "issue_id": "test:2",
                "issue_version": 1,
                "present": True,
                "test_id": "test:2",
            },
        ],
    }
    obj_types_v4_1 = obj_types_v4_0 | {"bug", "issue", "incident"}

    io_version_data_and_obj_types = {
        io.schema.V3_0: (io_data_v3_0, obj_types_v3_0),
        io.schema.V4_0: (io_data_v4_0, obj_types_v4_0),
        io.schema.V4_1: (io_data_v4_1, obj_types_v4_1),
    }

    # For each I/O version and its sample data
    for io_version, (io_data, obj_types) in \
            io_version_data_and_obj_types.items():
        # Skip I/O versions newer than database schema
        if db_client.get_schema()[1] < io_version:
            continue
        # Submit data to submission queue
        client.submit(io_data)
        # Try to pull the notification messages we're expecting and
        # check we get one correct message per object type
        remaining_obj_types = obj_types.copy()
        for ack_id, email in \
                email_subscriber.pull_iter(len(remaining_obj_types), 600):
            email_subscriber.ack(ack_id)
            assert email['From'] == "bot@kernelci.org", \
                f"Email From incorrect for {io_version!r}"
            assert email['To'] == "test@kernelci.org", \
                f"Email To incorrect for {io_version!r}"
            obj_type = email['X-KCIDB-Notification-Message-ID']
            assert obj_type in remaining_obj_types, \
                f"Duplicate {obj_type!r} email for {io_version!r}"
            remaining_obj_types.remove(obj_type)
            assert f"Test {obj_type}: " in email['Subject'], \
                f"Email Subject incorrect for {io_version!r}"
            text, html = email.get_payload()
            assert 'text/plain' == text.get_content_type()
            assert 'utf-8' == text.get_content_charset()
            content = text.get_content()
            assert f"Test {obj_type} detected!\r\n\r\n" in content
            assert 'text/html' == html.get_content_type()
            assert 'utf-8' == html.get_content_charset()
            content = html.get_content()
            assert f"Test {obj_type} detected!\r\n\r\n" in content
        # Check we got all types
        assert len(remaining_obj_types) == 0, \
            f"Missing {remaining_obj_types!r} emails for {io_version!r}"
        # Check we get no more notification messages
        assert len(email_subscriber.pull(1, 5)) == 0, \
            f"Extra emails for {io_version!r}"
