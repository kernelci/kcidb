"""
Kernel CI notification spool.

The spool registers notifications, and doesn't allow registering the same
notification twice. It stores generated notification emails, so they can be
picked up and sent asynchronously and provides an interface for making sure
every notification email is sent, and sent only once (as well as possible).
"""

import sys
import datetime
import email
import email.policy
from google.cloud import firestore
from kcidb.misc import ArgumentParser, iso_timestamp, log_and_print_excepthook
from kcidb.monitor.misc import is_valid_firestore_id
from kcidb.monitor.output import Notification

# Because we like the "id" name
# pylint: disable=invalid-name


class Client:
    """Notification spool client"""

    @staticmethod
    def is_valid_id(value):
        """
        Check if a value is a valid notification ID.

        Args:
            value: The value to check.

        Returns:
            True if the value is a valid notification ID,
            False if not.
        """
        return is_valid_firestore_id(value)

    def __init__(self, collection_path, project=None, pick_timeout=None):
        """
        Initialize a spool client.

        Args:
            collection_path:    The Google Firestore path to the collection
                                of notification documents.
            project:            The Google Cloud project the Firestore
                                database housing the spool belongs to.
                                Inferred from the environment, if None is
                                specified. The default is None.
            pick_timeout:       A datetime.timedelta object specifying how
                                long a notification should be considered
                                picked, by default, or None, meaning 2
                                minutes.
        """
        assert isinstance(collection_path, str) and collection_path
        assert project is None or isinstance(project, str)
        assert pick_timeout is None or \
            isinstance(pick_timeout, datetime.timedelta)
        self.db = firestore.Client(project=project)
        self.collection_path = collection_path
        self.parser = email.parser.Parser(policy=email.policy.SMTPUTF8)
        self.pick_timeout = pick_timeout or datetime.timedelta(minutes=2)

    def _get_coll(self):
        """
        Get a reference for the notification document collection.

        Returns:
            The notification document collection reference.
        """
        return self.db.collection(self.collection_path)

    def _get_doc(self, id):
        """
        Get the notification document reference for specified ID.

        Args:
            id:         The ID of the notification to get the document
                        reference for. Must be a valid Firestore ID.

        Returns:
            The notification document reference.
        """
        assert Client.is_valid_id(id)
        return self._get_coll().document(id)

    def post(self, notification, timestamp=None):
        """
        Post a notification onto the spool, if it wasn't there already.

        Args:
            notification:   An instance of kcidb.monitor.output.Notification
                            to post onto the spool.
            timestamp:      An "aware" datetime.datetime object specifying the
                            notification creation time, or None to use
                            datetime.datetime.now(datetime.timezone.utc).

        Returns:
            True, if the new notification was posted onto the spool,
            or the existing, "unpicked" notification was updated,
            False, if not (it existed and was "picked" at the moment).
        """
        assert isinstance(notification, Notification)
        assert timestamp is None or \
            isinstance(timestamp, datetime.datetime) and timestamp.tzinfo
        if timestamp is None:
            timestamp = datetime.datetime.now(datetime.timezone.utc)

        @firestore.transactional
        def create_or_update(transaction):
            due = notification.message.due \
                if notification.message.due else timestamp
            doc = self._get_doc(notification.id)
            snapshot = doc.get(field_paths=["picked_until"],
                               transaction=transaction)
            if snapshot.exists:
                # if the notification has been picked
                if snapshot.get("picked_until") > timestamp:
                    return False
                transaction.update(doc, dict(
                    message=notification.render().
                    as_string(policy=email.policy.SMTPUTF8), due=due))
            else:
                transaction.create(doc, dict(
                    created_at=timestamp,
                    # Set to a definitely timed-out time, free for picking
                    picked_until=datetime.datetime.min,
                    message=notification.render().
                    as_string(policy=email.policy.SMTPUTF8),
                    due=due
                ))
            return True

        return create_or_update(self.db.transaction())

    def pick(self, id, timestamp=None, timeout=None):
        """
        Pick a notification from the spool, for sending.
        A notification can only be picked by one client.

        Args:
            id:         The ID of the notification to pick.
                        Must be a valid Firestore ID.
            timestamp:  An "aware" datetime.datetime object specifying the
                        picking time, or None to use
                        datetime.datetime.now(datetime.timezone.utc).
            timeout:    A datetime.timedelta object specifying how long should
                        the notification stay picked, from the specified
                        timestamp. After that time, the notification becomes
                        free for picking again. None for the default
                        "pick_timeout" specified at initialization time.

        Returns:
            An email.message.EmailMessage object containing parsed
            notification message, ready for sending, except missing the From
            header, or None if the notification was already picked, was
            invalid, or wasn't in the spool.
        """
        assert Client.is_valid_id(id)
        assert timestamp is None or \
            isinstance(timestamp, datetime.datetime) and timestamp.tzinfo
        assert timeout is None or isinstance(timeout, datetime.timedelta)
        if timestamp is None:
            timestamp = datetime.datetime.now(datetime.timezone.utc)
        if timeout is None:
            timeout = self.pick_timeout

        @firestore.transactional
        def pick_if_not_picked(transaction, doc, timestamp):
            """Pick notification, if not picked yet"""
            # Get the document snapshot
            snapshot = doc.get(field_paths=["picked_until", "message", "due"],
                               transaction=transaction)
            picked_until = snapshot.get("picked_until")
            message_text = snapshot.get("message")
            due = snapshot.get('due')
            # If the document doesn't exist, has no "picked_until" field,
            # no "message" field, or the picking has not timed out yet
            # or the due date has not passed
            if not picked_until or not message_text or \
               picked_until > timestamp or due > timestamp:
                return None
            # Parse the message
            message = self.parser.parsestr(message_text)
            # Mark notification picked until timeout
            transaction.update(doc, dict(
                picked_at=timestamp,
                picked_until=timestamp + timeout,
            ))
            return message

        return pick_if_not_picked(self.db.transaction(),
                                  self._get_doc(id),
                                  timestamp)

    def ack(self, id, timestamp=None):
        """
        Acknowledge delivery of a notification picked from the spool.

        Args:
            id:         The ID of the notification to acknowledge.
                        Must have been picked previously by the same client.
            timestamp:  An "aware" datetime.datetime object specifying the
                        acknowledgment time, or None to use
                        datetime.datetime.now(datetime.timezone.utc).
        """
        assert Client.is_valid_id(id)
        assert timestamp is None or \
            isinstance(timestamp, datetime.datetime) and timestamp.tzinfo
        if timestamp is None:
            timestamp = datetime.datetime.now(datetime.timezone.utc)
        self._get_doc(id).update(dict(
            acked_at=timestamp,
            picked_until=datetime.datetime.max,
        ))

    def delete(self, id):
        """
        Remove a notification from the spool, regardless of its state.

        Args:
            id: The ID of the notification to remove.
        """
        assert Client.is_valid_id(id)
        self._get_doc(id).delete()

    def wipe(self, until=None):
        """
        Wipe notifications from the spool.

        Args:
            until:  An "aware" datetime.datetime object specifying the latest
                    creation time for removed notifications, or None to use
                    datetime.datetime.now(datetime.timezone.utc).
        """
        assert until is None or \
            isinstance(until, datetime.datetime) and until.tzinfo
        if until is None:
            until = datetime.datetime.now(datetime.timezone.utc)
        for snapshot in \
            self._get_coll().where("created_at", "<=", until). \
                select([]).stream():
            snapshot.reference.delete()

    def unpicked(self, timestamp=None):
        """
        Retrieve IDs of notifications, which weren't picked for delivery yet.

        Args:
            timestamp:  An "aware" datetime.datetime object specifying the
                        intended pickup time, or None to use
                            datetime.datetime.now(datetime.timezone.utc).

        Yields:
            The ID of the next notification free for picking.
        """
        assert timestamp is None or \
            isinstance(timestamp, datetime.datetime) and timestamp.tzinfo
        if timestamp is None:
            timestamp = datetime.datetime.now(datetime.timezone.utc)
        for snapshot in \
            self._get_coll().where("picked_until", "<", timestamp). \
                select([]).stream():
            id = snapshot.id
            assert Client.is_valid_id(id)
            yield id


def wipe_main():
    """Execute the kcidb-monitor-spool-wipe command-line tool"""
    sys.excepthook = log_and_print_excepthook
    description = \
        'kcidb-monitor-spool-wipe - Remove (old) notifications from the spool'
    parser = ArgumentParser(description=description)
    parser.add_argument(
        '-p', '--project',
        help='ID of the Google Cloud project containing the spool. '
             'Taken from credentials by default.',
        default=None,
        required=False
    )
    parser.add_argument(
        '-c', '--collection',
        help='The Google Firestore path to the spool collection.',
        required=True
    )
    parser.add_argument(
        'until',
        metavar='UNTIL',
        type=iso_timestamp,
        nargs='?',
        help='An ISO-8601 timestamp specifying the newest notification to be '
             'removed. The default is current time.'
    )
    args = parser.parse_args()
    Client(args.collection, project=args.project).wipe(until=args.until)
