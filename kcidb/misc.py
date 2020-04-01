"""Kernel CI reporting - misc definitions"""

import re
from email.message import EmailMessage
from kcidb.io import schema
from kcidb import oo

# We like the "id" name
# pylint: disable=invalid-name,redefined-builtin

# Address to put into From of notification e-mails
NOTIFICATION_FROM = "kernelci.org bot <bot@kernelci.org>"

# A regex matching permitted notification summary strings
NOTIFICATION_SUMMARY_RE = re.compile(r"[^\x00-\x1f\x7f]*")


# pylint: disable=too-few-public-methods
class NotificationMessage:
    """
    Message for a notification about a report object state.
    """
    def __init__(self, recipients, summary="", description="", id=""):
        """
        Initialize a notification message.

        Args:
            recipients:     List of e-mail addresses of notification message
                            recipients.
            summary:        Summary of the object state being notified about.
                            Must be a single-line string without control
                            characters.
            description:    Detailed description of the object state being
                            notified about.
            id:             String identifier of the notification message.
                            Must be 256 bytes at most when encoded into UTF-8.
                            The system will only send one notification with
                            the same ID for the same subscription for each
                            database object.
        """
        assert isinstance(recipients, list)
        assert all(isinstance(r, str) for r in recipients)
        assert isinstance(summary, str)
        assert NOTIFICATION_SUMMARY_RE.fullmatch(summary)
        assert isinstance(description, str)
        assert isinstance(id, str)
        assert len(id.encode("utf-8")) <= 256

        self.recipients = recipients
        self.summary = summary
        self.description = description
        self.id = id


# pylint: disable=too-few-public-methods
class Notification:
    """
    Notification about a report object state.
    """
    def __init__(self, obj_list_name, obj, subscription, message):
        """
        Initialize a notification.

        Args:
            obj_list_name:  Name of the object list (e.g. "revisions") to
                            which the object notified about belongs (and
                            therefore its type).
            obj:            Object-oriented representation of the object
                            being notified about.
            subscription:   The name of the subscription which generated the
                            notification (e.g. the name of subscription
                            module).
            message:        Notification message. An instance of
                            NotificationMessage.
        """
        assert isinstance(obj_list_name, str)
        assert obj_list_name
        assert obj_list_name in schema.LATEST.tree
        assert isinstance(obj, oo.Node)
        assert isinstance(subscription, str)
        assert isinstance(message, NotificationMessage)

        self.obj_list_name = obj_list_name
        self.obj = obj
        self.subscription = subscription
        self.message = message

    def render(self):
        """
        Render the notification as an e-mail message.

        Returns:
            An instance of email.message.EmailMessage with the notification
            ready to send.
        """
        email = EmailMessage()
        email["Subject"] = self.message.summary + self.obj.summarize()
        email["From"] = NOTIFICATION_FROM
        email["To"] = ", ".join(self.message.recipients)
        email["X-KCIDB-Notification-Message-ID"] = self.message.id
        email.set_content(self.message.description + self.obj.describe())
        return email
