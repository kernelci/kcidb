"""kcdib.misc module tests"""

import email
import unittest
from kcidb.io import schema
from kcidb import oo
from kcidb.misc import NotificationMessage, Notification

# Disable long line checking for JSON data
# flake8: noqa
# pylint: disable=line-too-long


class NotificationTestCase(unittest.TestCase):
    """kcidb.misc.Notification test case"""

    def setUp(self):
        """Setup tests"""
        # pylint: disable=invalid-name
        self.maxDiff = None
        self.version = dict(
            major=schema.LATEST.major,
            minor=schema.LATEST.minor
        )

    def test_min(self):
        """Check minimal Notification functionality"""

        oo_data = oo.from_io({
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
                    "id": "origin:1",
                    "origin": "origin",
                    "patch_mboxes": [],
                    "valid": True,
                },
            ],
        })

        notification_message = NotificationMessage(
            ["foo@kernelci.org", "bar@kernelci.org"],
            "Revision detected: ",
            "We detected a new revision!\n\n",
            "id"
        )
        notification = Notification("revisions",
                                    next(iter(oo_data["revisions"].values())),
                                    "subscription",
                                    notification_message)
        self.assertEqual(notification.id,
                         "subscription:revisions:b3JpZ2luOjE=:aWQ=")
        message = notification.render()
        self.assertIsInstance(message, email.message.EmailMessage)
        self.assertIsNone(message['From'])
        self.assertEqual(message['To'], "foo@kernelci.org, bar@kernelci.org")
        self.assertEqual(message['X-KCIDB-Notification-ID'], notification.id)
        self.assertEqual(message['X-KCIDB-Notification-Message-ID'], "id")
        self.assertIn("Revision detected: ", message['Subject'])
        self.assertIn("We detected a new revision!", message.get_payload())
