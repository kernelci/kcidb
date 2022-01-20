"""kcdib.monitor.output module tests"""

import email
import unittest
from kcidb import orm, db, oo
from kcidb.monitor.output import NotificationMessage, Notification,\
    NOTIFICATION_MESSAGE_SUBJECT_MAX_LEN, NOTIFICATION_MESSAGE_BODY_MAX_LEN
from kcidb.io import SCHEMA
from kcidb.unittest import local_only

# Disable long line checking for JSON data
# flake8: noqa
# pylint: disable=line-too-long


@local_only
class NotificationTestCase(unittest.TestCase):
    """kcidb.monitor.output.Notification test case"""

    def setUp(self):
        """Setup tests"""
        # pylint: disable=invalid-name
        self.maxDiff = None
        self.version = dict(
            major=SCHEMA.major,
            minor=SCHEMA.minor
        )

    def test_min(self):
        """Check minimal Notification functionality"""

        db_client = db.Client("sqlite::memory:")
        db_client.init()
        oo_client = oo.Client(db_client)
        db_client.load({
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
                    "id": "origin:1",
                    "origin": "origin",
                    "patchset_files": [],
                    "valid": True,
                },
            ],
        })
        oo_data = oo_client.query(orm.Pattern.parse(">*#"))

        notification_message = NotificationMessage(
            to=["foo@kernelci.org", "bar@kernelci.org"],
            subject='Revision detected: '
            '{% include "revision_summary.txt.j2" %}',
            body='We detected a new revision!\n\n'
            '{% include "revision_description.txt.j2" %}',
            id="id"
        )
        notification = Notification(oo_data["revision"][0],
                                    "subscription",
                                    notification_message)
        self.assertEqual(notification.id,
                         "subscription:revision:"
                         "KCc1ZTI5ZDE0NDNjNDZiNmNhNzBhNGM5NDBhNjdl"
                         "OGMwOWYwNWRjYjdlJywgJycp:aWQ=")
        message = notification.render()
        self.assertIsInstance(message, email.message.EmailMessage)
        self.assertIsNone(message['From'])
        self.assertEqual(message['To'], "foo@kernelci.org, bar@kernelci.org")
        self.assertEqual(message['X-KCIDB-Notification-ID'], notification.id)
        self.assertEqual(message['X-KCIDB-Notification-Message-ID'], "id")
        self.assertIn("Revision detected: ", message['Subject'])
        text, html = message.get_payload()
        self.assertEqual('text/plain', text.get_content_type())
        self.assertEqual('utf-8', text.get_content_charset())
        content = text.get_payload()
        self.assertIn("We detected a new revision!", content)

        self.assertEqual('text/html', html.get_content_type())
        self.assertEqual('utf-8', html.get_content_charset())
        content = html.get_payload()
        self.assertIn("We detected a new revision!", content)

    def test_subject_and_body_length(self):
        """Check subject and body length of Notification """

        db_client = db.Client("sqlite::memory:")
        db_client.init()
        oo_client = oo.Client(db_client)
        db_client.load({
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
                    "git_repository_url": "https://git.kernel.org/pub/scm/linux/kernel/git/arm64/linux.git",
                    "misc": {
                        "pipeline_id": 467715
                    },
                    "id": "origin:1",
                    "origin": "origin",
                    "patchset_files": [],
                    "valid": True,
                },
            ],
            "builds": [
                                {
                    "id": "kernelci:kernelci.org:619fecfc6d932a49b5f2efb0",
                    "checkout_id": "origin:1",
                    "origin": "origin",
                    "comment": "v5.16-rc2-19-g383a44aec91c",
                    "start_time": "2021-11-25T20:07:24.499000+00:00",
                    "duration": 486.695294857,
                    "architecture": "x86_64",
                    "config_name": "x86_64_defconfig+x86-chromebook+kselftest",
                    "valid": True
                },
            ]
        })
        revision = oo_client.query(orm.Pattern.parse(">revision#"))["revision"][0]

        # test under-limit length subject/body are left intact in the email
        notification_message = NotificationMessage(
            ["kernelci-results-staging@groups.io"],
            subject="Test completed for linux.git",
            body="Below is the summary of results Kernel CI database has "
                 "recorded for this revision so far")
        notification = Notification(revision, "test_subscription", notification_message)
        message = notification.render()
        self.assertIsInstance(message, email.message.EmailMessage)
        self.assertEqual(len(message.get("Subject")), 28)
        self.assertEqual(len(message.get_body('plain').get_content()[:-1]), 88)

        # test zero length subject/body produces zero length subject and body in the email
        notification_message = NotificationMessage(
            ["kernelci-results-staging@groups.io"], subject="", body="")
        notification = Notification(revision, "test_subscription_2", notification_message)
        message = notification.render()
        self.assertEqual(len(message.get("Subject")), 0)
        self.assertEqual(len(message.get_body('plain').get_content()[:-1]), 0)

        # test exact-limit length subject/body are left intact in the email,
        notification_message = NotificationMessage(
            ["kernelci-results-staging@groups.io"],
            subject="S" * NOTIFICATION_MESSAGE_SUBJECT_MAX_LEN,
            body="B" * NOTIFICATION_MESSAGE_BODY_MAX_LEN
        )
        notification = Notification(revision, "test_subscription_3", notification_message)
        message = notification.render()
        self.assertEqual(len(message.get("Subject")), NOTIFICATION_MESSAGE_SUBJECT_MAX_LEN)
        self.assertEqual(len(message.get_body('plain').get_content()[:-1]),
                         NOTIFICATION_MESSAGE_BODY_MAX_LEN)

        # test exact-limit length with a multibyte character in UTF-8
        # subject/body are left intact in the email,
        notification_message = NotificationMessage(
            ["kernelci-results-staging@groups.io"],
            subject="©" * NOTIFICATION_MESSAGE_SUBJECT_MAX_LEN,
            body="•" * NOTIFICATION_MESSAGE_BODY_MAX_LEN
        )
        notification = Notification(revision, "test_subscription_4", notification_message)
        message = notification.render()
        self.assertEqual(len(message.get("Subject")), NOTIFICATION_MESSAGE_SUBJECT_MAX_LEN)
        self.assertEqual(len(message.get_body('plain').get_content()[:-1]),
                         NOTIFICATION_MESSAGE_BODY_MAX_LEN)

        # test exact-limit length with a single-byte character in UTF-8
        # subject/body are left intact in the email,
        notification_message = NotificationMessage(
            ["kernelci-results-staging@groups.io"],
            subject="*" * NOTIFICATION_MESSAGE_SUBJECT_MAX_LEN,
            body="<" * NOTIFICATION_MESSAGE_BODY_MAX_LEN
        )
        notification = Notification(revision, "test_subscription_5", notification_message)
        message = notification.render()
        self.assertEqual(len(message.get("Subject")), NOTIFICATION_MESSAGE_SUBJECT_MAX_LEN)
        self.assertEqual(len(message.get_body('plain').get_content()[:-1]),
                         NOTIFICATION_MESSAGE_BODY_MAX_LEN)

        # test over limit length subject/body are trimmed to the exact limit
        # with the last character replaced with the scissors' emoji in the email
        notification_message = NotificationMessage(
            ["kernelci-results-staging@groups.io"],
            subject="Test completed for linux.git:123" * 10,
            body="Below is the summary of results Kernel CI database has recorded " * 70
        )
        notification = Notification(revision, "test_subscription_6", notification_message)
        message = notification.render()
        self.assertEqual(len(message.get("Subject")), NOTIFICATION_MESSAGE_SUBJECT_MAX_LEN)
        self.assertEqual(message.get("Subject")[-2:], "✂️")
        self.assertEqual(len(message.get_body('plain').get_content()[:-1]), NOTIFICATION_MESSAGE_BODY_MAX_LEN)
        self.assertEqual((message.get_body('plain').get_content())[-3:-1], "✂️")

    def test_subject_invalid_character(self):
        """Check subject invalid character of Notification """

        db_client = db.Client("sqlite::memory:")
        db_client.init()
        oo_client = oo.Client(db_client)
        db_client.load({
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
                    "git_repository_url": "https://git.kernel.org/pub/scm/linux/kernel/git/arm64/linux.git",
                    "misc": {
                        "pipeline_id": 467715
                    },
                    "id": "origin:1",
                    "origin": "origin",
                    "patchset_files": [],
                    "valid": True,
                },
            ],
            "builds": [
                                {
                    "id": "kernelci:kernelci.org:619fecfc6d932a49b5f2efb0",
                    "checkout_id": "origin:1",
                    "origin": "origin",
                    "comment": "v5.16-rc2-19-g383a44aec91c",
                    "start_time": "2021-11-25T20:07:24.499000+00:00",
                    "duration": 486.695294857,
                    "architecture": "x86_64",
                    "config_name": "x86_64_defconfig+x86-chromebook+kselftest",
                    "valid": True
                },
            ]
        })
        revision = oo_client.query(orm.Pattern.parse(">revision#"))["revision"][0]

        # test invalid character in subject are replaced
        # by the uncertainty sign character in the email
        notification_message = NotificationMessage(
            ["kernelci-results-staging@groups.io"],
            subject="Test completed for linux.git:\n@commit20-\x7f\t",
            body="Below is the summary of results Kernel CI database has recorded for this revision so far."
        )
        notification = Notification(revision, "test_subscription", notification_message)
        message = notification.render()
        self.assertIsInstance(message, email.message.EmailMessage)
        self.assertEqual(len(message.get("Subject")), 42)
        self.assertEqual(message.get("Subject")[29], "⯑")
        self.assertEqual(message.get("Subject")[-2:], "⯑⯑")

        # test empty subject passes unchanged in the email
        notification_message = NotificationMessage(
            ["kernelci-results-staging@groups.io"],
            subject="",
            body="Below is the summary of results."
        )
        notification = Notification(revision, "test_subscription2", notification_message)
        message = notification.render()
        self.assertIsInstance(message, email.message.EmailMessage)
        self.assertEqual(len(message.get("Subject")), 0)

        # test subject with all valid characters passes unchanged in the email
        notification_message = NotificationMessage(
            ["kernelci-results-staging@groups.io"],
            subject="Testing for this Subject.😀😀",
            body="Below is the summary of results."
        )
        notification = Notification(revision, "test_subscription2", notification_message)
        message = notification.render()
        self.assertIsInstance(message, email.message.EmailMessage)
        self.assertEqual(len(message.get("Subject")), 27)
        self.assertNotIn(message.get("Subject"), "⯑")
