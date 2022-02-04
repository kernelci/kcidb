"""Kernel CI reporting - monitor - output"""

import datetime
import re
import base64
import textwrap
import html
import logging
from email.message import EmailMessage
import kcidb.oo
from kcidb.monitor.misc import is_valid_firestore_id
from kcidb.templates import ENV as TEMPLATE_ENV

# We like the "id" name
# pylint: disable=invalid-name

# Characters not allowed in notifications message subjects
NOTIFICATION_MESSAGE_SUBJECT_INVALID_CHARS = r"\x00-\x1f\x7f"
# A regex matching permitted notification message subject strings
NOTIFICATION_MESSAGE_SUBJECT_RE = \
    re.compile(f"[^{NOTIFICATION_MESSAGE_SUBJECT_INVALID_CHARS}]*")
# Maximum character length of a notification message subject
NOTIFICATION_MESSAGE_SUBJECT_MAX_LEN = 256
# Maximum character length of a notification message body
NOTIFICATION_MESSAGE_BODY_MAX_LEN = 4096

# A regex matching permitted subscription name strings
SUBSCRIPTION_RE = re.compile(r"([A-Za-z0-9][A-Za-z0-9_]*)?")

# Module's logger
LOGGER = logging.getLogger(__name__)


class NotificationMessage:
    """
    Message for a notification about a report object state.
    """
    # It's OK pylint: disable=too-many-arguments
    def __init__(self, to, subject, body, cc=None, bcc=None, id="", due=None):
        """
        Initialize a notification message.

        Args:
            to:             List of e-mail addresses of notification message
                            recipients to put into the 'To' header.
            subject:        The text of a Jinja2 template for the message
                            subject. Must render and encode into 256 bytes of
                            UTF-8 at most, and be a single-line string without
                            control characters.
                            When rendering, the template environment will
                            contain the object being notified about in a
                            variable named after the object's type. I.e. a
                            revision will be in the "revision" variable, a
                            build in "build", and so on.
            body:           The text of a Jinja2 template for the message
                            body. Must render and encode into 4096 bytes of
                            UTF-8 at most.
                            When rendering, the template environment will
                            contain the object being notified about in a
                            variable named after the object's type. I.e. a
                            revision will be in the "revision" variable, a
                            build in "build", and so on.
            cc:             List of e-mail addresses to receive a copy of the
                            notification message, or None, meaning an empty
                            list. To be put into the 'Cc' header.
            bcc:            List of e-mail addresses to receive a "blind" copy
                            of the notification message, or None, meaning an
                            empty list. To be put into the 'Bcc' header.
            id:             String identifier of the notification message.
                            Must encode into 256 bytes of UTF-8 at most.
                            The system will only send one notification with
                            the same ID for the same subscription for each
                            database object.
            due:            An "aware" datetime.datetime object specifying the
                            time notification should be sent out,
                            or None to use
                            datetime.datetime.now(datetime.timezone.utc).
        """
        assert isinstance(to, list)
        assert all(isinstance(address, str) for address in to)
        assert isinstance(subject, str)
        assert isinstance(body, str)
        if cc is None:
            cc = []
        assert isinstance(cc, list)
        assert all(isinstance(address, str) for address in cc)
        if bcc is None:
            bcc = []
        assert isinstance(bcc, list)
        assert all(isinstance(address, str) for address in bcc)
        assert isinstance(id, str)
        assert len(id.encode()) <= 256
        assert due is None or \
               isinstance(due, datetime.datetime) and due.tzinfo

        self.to = to
        self.subject = subject
        self.body = body
        self.cc = cc
        self.bcc = bcc
        self.id = id
        self.due = due


class Notification:
    """
    Notification about a report object state.
    """

    @staticmethod
    def _to_id_part(string):
        """
        Encode a string for safe use as part of a notification ID.

        Args:
            string: The string to encode.

        Returns:
            The encoded string, usable as part of an ID.
        """
        return base64.b64encode(string.encode(), altchars=b'+-').decode()

    @staticmethod
    def _from_id_part(id_part):
        """
        Decode an ID part into the original string passed to _to_id_part()
        previously.

        Args:
            id: The ID part to decode.

        Returns:
            The decoded string.
        """
        return base64.b64decode(id_part, altchars=b'+-',
                                validate=True).decode()

    def __init__(self, obj, subscription, message):
        """
        Initialize a notification.

        Args:
            obj:            Object-oriented representation of the object
                            being notified about (an instance of
                            kcidb.oo.Object).
            subscription:   The name of the subscription which generated the
                            notification (e.g. the name of subscription
                            module).
                            Must consist only of latin letters, digits, and
                            underscores. Must not start with an underscore.
                            Must encode into 64 bytes of UTF-8 at most.
            message:        Notification message. An instance of
                            NotificationMessage.
        """
        assert isinstance(obj, kcidb.oo.Object)
        assert isinstance(subscription, str)
        assert SUBSCRIPTION_RE.fullmatch(subscription)
        assert len(subscription.encode()) <= 64
        assert isinstance(message, NotificationMessage)

        self.obj = obj
        self.subscription = subscription
        self.message = message
        id = self.subscription + ":" + \
            self.obj.get_type().name + ":" + \
            Notification._to_id_part(repr(obj.get_id())) + ":" + \
            Notification._to_id_part(message.id)
        assert is_valid_firestore_id(id)
        self.id = id

    def render(self):
        """
        Render the notification as an e-mail message.

        Returns:
            An instance of email.message.EmailMessage with the notification
            ready to send, but missing the From header.
        """
        # Render the message subject and body
        ctx = {
            self.obj.get_type().name: self.obj,
        }
        subject = TEMPLATE_ENV.from_string(self.message.subject).render(ctx)
        subject_extra_characters = \
            len(subject) - NOTIFICATION_MESSAGE_SUBJECT_MAX_LEN
        if subject_extra_characters > 0:
            subject = subject[:NOTIFICATION_MESSAGE_SUBJECT_MAX_LEN - 2] + "✂️"
            LOGGER.warning("Subject is %s characters too long, truncated",
                           subject_extra_characters)
        if not NOTIFICATION_MESSAGE_SUBJECT_RE.fullmatch(subject):
            subject = re.sub(f"[{NOTIFICATION_MESSAGE_SUBJECT_INVALID_CHARS}]",
                             "⯑", subject)
            LOGGER.warning("Subject is invalid, must match %s"
                           "regex, invalid characters removed",
                           NOTIFICATION_MESSAGE_SUBJECT_RE.pattern)
        body = TEMPLATE_ENV.from_string(self.message.body).render(ctx)
        body_extra_characters = \
            len(body) - NOTIFICATION_MESSAGE_BODY_MAX_LEN
        if body_extra_characters > 0:
            body = body[:NOTIFICATION_MESSAGE_BODY_MAX_LEN - 2] + "✂️"
            LOGGER.warning("Body is %s characters too long, truncated",
                           body_extra_characters)

        # Generate the plain-text message
        email = EmailMessage()
        email["Subject"] = subject
        email["To"] = ", ".join(self.message.to)
        if self.message.cc:
            email["Cc"] = ", ".join(self.message.cc)
        if self.message.bcc:
            email["Bcc"] = ", ".join(self.message.bcc)
        email["X-KCIDB-Notification-ID"] = self.id
        email["X-KCIDB-Notification-Message-ID"] = self.message.id
        email.set_content(body)

        # Add the HTML version generated from plain text
        escaped_subject = html.escape(subject, quote=True)
        escaped_body = html.escape(body, quote=True)
        linked_body = re.sub(
            r'((http|https|git|ftp)://[^\s]+)',
            '<a href="\\1">\\1</a>',
            escaped_body
        )
        html_body = textwrap.dedent("""\
            <html>
                <head>
                    <title>{}</title>
                </head>
                <body>
                    <pre>{}</pre>
                </body>
            </html>
        """).format(escaped_subject, linked_body)
        email.add_alternative(html_body, subtype='html')

        return email
