"""Kernel CI reporting - misc definitions"""

import re
import os
import sys
import base64
import traceback
import argparse
import logging
import json
from textwrap import indent
from email.message import EmailMessage
from google.cloud import secretmanager
import jq
from kcidb.io import schema
from kcidb import oo

# We like the "id" name
# pylint: disable=invalid-name,redefined-builtin

# Module's logger
LOGGER = logging.getLogger(__name__)

# A regex matching permitted notification message summary strings
NOTIFICATION_MESSAGE_SUMMARY_RE = re.compile(r"[^\x00-\x1f\x7f]*")

# A regex matching permitted subscription name strings
SUBSCRIPTION_RE = re.compile(r"([A-Za-z0-9][A-Za-z0-9_]*)?")

# A dictionary of names of logging levels and their values
LOGGING_LEVEL_MAP = {
    name: value
    for name, value in logging.__dict__.items()
    if name.isalpha() and name.isupper() and isinstance(value, int) and value
}
# Logging level disabling all logging
LOGGING_LEVEL_MAP["NONE"] = max(LOGGING_LEVEL_MAP.values()) + 1
# Sort levels highest->lowest
# I don't see it, pylint: disable=unnecessary-comprehension
LOGGING_LEVEL_MAP = {
    k: v
    for k, v in sorted(LOGGING_LEVEL_MAP.items(),
                       key=lambda i: i[1], reverse=True)
}


def logging_setup(level):
    """
    Setup logging: set root logger log level and disable irrelevant logging.

    Args:
        level:  Logging level for the root logger.
    """
    assert isinstance(level, int)
    logging.getLogger().setLevel(level)
    # We'll do it later, pylint: disable=fixme
    # TODO Consider separate arguments for controlling the below
    logging.getLogger("urllib3").setLevel(LOGGING_LEVEL_MAP["NONE"])
    logging.getLogger("google").setLevel(LOGGING_LEVEL_MAP["NONE"])


def format_exception_stack(exc):
    """
    Format an exception's context stack as a series of indented messages.

    Args:
        exc:    The exception to format the stack of.

    Returns:
        The formatted exception stack.
    """
    assert isinstance(exc, Exception)
    string = ""
    prefix = ""
    while True:
        summary = ": ".join(s for s in (type(exc).__name__, str(exc)) if s)
        string += indent(summary, prefix)
        if exc.__context__:
            string += ":\n"
            prefix += "  "
            exc = exc.__context__
        else:
            break
    return string


def log_and_print_excepthook(type, value, tb):
    """
    Log an exception with DEBUG level and print its summary to stderr.
    Adheres to sys.excepthook interface.

    Args:
        type:   Exception class.
        value:  Exception instance.
        tb:     Exception traceback object.
    """
    lines = traceback.format_exception(type, value, tb)
    LOGGER.debug("%s", "".join(lines).rstrip())
    print(format_exception_stack(value), file=sys.stderr)


class ArgumentParser(argparse.ArgumentParser):
    """
    KCIDB command-line argument parser handling common arguments.
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize the parser, adding common arguments.

        Args:
            args:   Positional arguments to initialize ArgumentParser with.
            kwargs: Keyword arguments to initialize ArgumentParser with.
        """
        super().__init__(*args, **kwargs)
        self.add_argument(
            '-l', '--log-level',
            metavar="LEVEL",
            default="NONE",
            choices=LOGGING_LEVEL_MAP.keys(),
            help='Limit logging to LEVEL (%(choices)s). Default is NONE.'
        )

    def parse_args(self, args=None, namespace=None):
        """
        Parse arguments, including common ones, apply ones affecting global
        state.

        Args:
            args:       List of strings to parse. The default is taken from
                        sys.argv.
            namespace:  An object to take the attributes. The default is a new
                        empty argparse.Namespace object.

        Returns:
            Namespace populated with arguments.
        """
        args = super().parse_args(args=args, namespace=namespace)
        logging.basicConfig()
        logging_setup(LOGGING_LEVEL_MAP[args.log_level])
        return args


def non_negative_int(string):
    """
    Parse a non-negative integer out of a string.
    Matches the argparse type function interface.

    Args:
        string: The string to parse.

    Returns:
        The non-negative integer parsed out of the string.

    Raises:
        argparse.ArgumentTypeError: the string wasn't representing a
        non-negative integer.
    """
    if not re.match("^[0-9]+$", string):
        raise argparse.ArgumentTypeError(
            f'{repr(string)} is not a positive integer, nor zero'
        )
    return int(string)


class OutputArgumentParser(ArgumentParser):
    """
    Command-line argument parser for tools outputting JSON.
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize the parser, adding report output arguments.

        Args:
            args:   Positional arguments to initialize ArgumentParser with.
            kwargs: Keyword arguments to initialize ArgumentParser with.
        """
        super().__init__(*args, **kwargs)
        self.add_argument(
            '--indent',
            metavar="NUMBER",
            type=non_negative_int,
            help='Pretty-print JSON using NUMBER of spaces for indenting. '
                 'Print single-line if zero. Default is 4.',
            default=4,
            required=False
        )
        self.add_argument(
            '--seq',
            help='Prefix JSON output with the RS character, to match '
                 'RFC 7464 and "application/json-seq" media type.',
            action='store_true'
        )


def json_load_stream_fd(stream_fd, chunk_size=4*1024*1024):
    """
    Load a series of JSON values from a stream file descriptor.

    Args:
        stream_fd:  The file descriptor for the stream to read.
        chunk_size: Maximum size of chunks to read from the file, bytes.

    Returns:
        An iterator returning loaded JSON values.
    """
    def read_chunk():
        while True:
            chunk = os.read(stream_fd, chunk_size)
            if chunk:
                yield chunk
            else:
                break

    return jq.parse_json(text_iter=read_chunk())


# It's OK, pylint: disable=redefined-outer-name
def json_dump(value, fp, indent=0, seq=False):
    """
    Dump a JSON value to a file, followed by a newline.

    Args:
        value:  The JSON value to dump.
        fp:     The file-like object to output to.
        indent: Number of indent spaces for pretty-printing, or zero to
                disable pretty-printing and dump the value single-line.
        seq:    Prefix the value with an RS character, to make output comply
                with RFC 7464 and the "application/json-seq" media type.
    """
    if seq:
        fp.write("\x1e")
    json.dump(value, fp, indent=indent or None)
    fp.write("\n")


def get_secret(project_id, secret_id):
    """
    Get the latest version of a secret from Google Secret Manager.

    Args:
        project_id: The ID of the Google Cloud project to fetch secrets from.
        secret_id:  The ID of the secret to fetch latest version of.

    Returns:
        The latest version of the secret.
    """
    assert isinstance(project_id, str) and project_id
    assert isinstance(secret_id, str) and secret_id
    client = secretmanager.SecretManagerServiceClient()
    path = client.secret_version_path(project_id, secret_id, "latest")
    return client.access_secret_version(path).payload.data.decode()


def is_valid_firestore_id(value):
    """
    Check if a value is valid for use as a Google Firestore
    collection or document ID, according to
    https://firebase.google.com/docs/firestore/quotas

    Args:
        value: The value to check.

    Returns:
        True if the value is a valid Firestore document/collection ID,
        False if not.
    """
    try:
        return isinstance(value, str) and \
               len(value.encode()) <= 1500 and \
               value != "." and \
               value != ".." and \
               "/" not in value and \
               not (value.startswith("__") and value.endswith("__"))
    except UnicodeError:
        return False


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
                            Must encode into 256 bytes of UTF-8 at most.
                            Must be a single-line string without control
                            characters.
            description:    Detailed description of the object state being
                            notified about.
                            Must encode into 4096 bytes of UTF-8 at most.
            id:             String identifier of the notification message.
                            Must encode into 256 bytes of UTF-8 at most.
                            The system will only send one notification with
                            the same ID for the same subscription for each
                            database object.
        """
        assert isinstance(recipients, list)
        assert all(isinstance(r, str) for r in recipients)
        assert isinstance(summary, str)
        assert NOTIFICATION_MESSAGE_SUMMARY_RE.fullmatch(summary)
        assert len(summary.encode()) <= 256
        assert isinstance(description, str)
        assert len(id.encode()) <= 4096
        assert isinstance(id, str)
        assert len(id.encode()) <= 256

        self.recipients = recipients
        self.summary = summary
        self.description = description
        self.id = id


# pylint: disable=too-few-public-methods
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
                            Must consist only of latin letters, digits, and
                            underscores. Must not start with an underscore.
                            Must encode into 64 bytes of UTF-8 at most.
            message:        Notification message. An instance of
                            NotificationMessage.
        """
        assert isinstance(obj_list_name, str)
        assert obj_list_name
        assert obj_list_name in schema.LATEST.tree
        assert isinstance(obj, oo.Node)
        assert isinstance(subscription, str)
        assert SUBSCRIPTION_RE.fullmatch(subscription)
        assert len(subscription.encode()) <= 64
        assert isinstance(message, NotificationMessage)

        self.obj_list_name = obj_list_name
        self.obj = obj
        self.subscription = subscription
        self.message = message
        id = self.subscription + ":" + \
            self.obj_list_name + ":" + \
            Notification._to_id_part(obj.id) + ":" + \
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
        email = EmailMessage()
        email["Subject"] = self.message.summary + self.obj.summarize()
        email["To"] = ", ".join(self.message.recipients)
        email["X-KCIDB-Notification-ID"] = self.id
        email["X-KCIDB-Notification-Message-ID"] = self.message.id
        email.set_content(self.message.description + self.obj.describe())
        return email
