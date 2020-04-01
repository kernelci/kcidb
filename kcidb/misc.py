"""Kernel CI reporting - misc definitions"""

import re
from kcidb.io import schema
from kcidb import oo

# A regex matching permitted notification summary strings
NOTIFICATION_SUMMARY_RE = re.compile(r"[^\x00-\x1f\x7f]*")


# pylint: disable=too-few-public-methods
class NotificationMessage:
    """
    Message for a notification about a report object state.
    """
    def __init__(self, recipients, summary="", description=""):
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
        """
        assert isinstance(recipients, list)
        assert all(isinstance(r, str) for r in recipients)
        assert isinstance(summary, str)
        assert NOTIFICATION_SUMMARY_RE.fullmatch(summary)
        assert isinstance(description, str)

        self.recipients = recipients
        self.summary = summary
        self.description = description


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
        """
