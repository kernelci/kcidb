"""Google Cloud Functions for Kernel CI reporting"""

import os
import json
import base64
import datetime
import logging
import smtplib
import kcidb


# Name of the Google Cloud project we're deployed in
PROJECT_ID = os.environ["GCP_PROJECT"]

kcidb.misc.logging_setup(
    kcidb.misc.LOGGING_LEVEL_MAP[os.environ.get("KCIDB_LOG_LEVEL", "NONE")]
)
LOGGER = logging.getLogger()

# The subscriber object for the submission queue
_LOAD_QUEUE_SUBSCRIBER = None
# Maximum number of messages loaded from the submission queue in one go
LOAD_QUEUE_MSG_MAX = int(os.environ["KCIDB_LOAD_QUEUE_MSG_MAX"])
# Maximum number of objects loaded from the submission queue in one go
LOAD_QUEUE_OBJ_MAX = int(os.environ["KCIDB_LOAD_QUEUE_OBJ_MAX"])
# Maximum time for pulling maximum amount of submissions from the queue
LOAD_QUEUE_TIMEOUT_SEC = float(os.environ["KCIDB_LOAD_QUEUE_TIMEOUT_SEC"])

# Minimum time between loading submissions into the database
DATABASE_LOAD_PERIOD = datetime.timedelta(
    seconds=int(os.environ["KCIDB_DATABASE_LOAD_PERIOD_SEC"])
)

# A whitespace-separated list of subscription names to limit notifying to
SELECTED_SUBSCRIPTIONS = \
    os.environ.get("KCIDB_SELECTED_SUBSCRIPTIONS", "").split()

# The address of the SMTP host to send notifications through
SMTP_HOST = os.environ["KCIDB_SMTP_HOST"]
# The port of the SMTP server to send notifications through
SMTP_PORT = int(os.environ["KCIDB_SMTP_PORT"])
# The username to authenticate to SMTP server with
SMTP_USER = os.environ["KCIDB_SMTP_USER"]
# The SMTP user's password
_SMTP_PASSWORD = None
# The address to tell the SMTP server to send the message to,
# overriding any recipients in the message itself.
SMTP_TO_ADDRS = os.environ.get("KCIDB_SMTP_TO_ADDRS", None)
# The name of the PubSub topic to post email messages to, instead of sending
# them to the SMTP server with SMTP_* parameters specified above. If
# specified, those parameters are ignored.
SMTP_TOPIC = os.environ.get("KCIDB_SMTP_TOPIC", None)
# The publisher object for email messages, if we're requested to send them to
# a PubSub topic
_SMTP_PUBLISHER = None
# The name of the subscription for email messages posted to the SMTP topic
SMTP_SUBSCRIPTION = os.environ.get("KCIDB_SMTP_SUBSCRIPTION", None)
# The address to use as the "From" address in sent notifications
SMTP_FROM_ADDR = os.environ.get("KCIDB_SMTP_FROM_ADDR", None)
# A string to be added to the CC header of notifications being sent out
EXTRA_CC = os.environ.get("KCIDB_EXTRA_CC", None)
# The database client instance
_DB_CLIENT = None
# The object-oriented database client instance
_OO_CLIENT = None
# The notification spool client
_SPOOL_CLIENT = None
# True if the database updates should be published to the updated queue
UPDATED_PUBLISH = bool(os.environ.get("KCIDB_UPDATED_PUBLISH", ""))
# Maximum number of objects published onto the updated queue in one message
UPDATED_QUEUE_OBJ_MAX = int(os.environ["KCIDB_UPDATED_QUEUE_OBJ_MAX"])
# The publisher object for the queue with patterns matching objects updated by
# loading submissions.
_UPDATED_QUEUE_PUBLISHER = None


def get_smtp_publisher():
    """
    Get the created/cached publisher object for email messages, if we're
    requested to send them to a PubSub topic, or None, if not.
    """
    # It's alright, pylint: disable=global-statement
    global _SMTP_PUBLISHER
    if SMTP_TOPIC is not None and _SMTP_PUBLISHER is None:
        _SMTP_PUBLISHER = kcidb.mq.EmailPublisher(PROJECT_ID, SMTP_TOPIC)
    return _SMTP_PUBLISHER


def get_smtp_password():
    """Get the (cached) password for the SMTP user"""
    # It's alright, pylint: disable=global-statement
    global _SMTP_PASSWORD
    if _SMTP_PASSWORD is None:
        secret = os.environ["KCIDB_SMTP_PASSWORD_SECRET"]
        _SMTP_PASSWORD = kcidb.misc.get_secret(PROJECT_ID, secret)
    return _SMTP_PASSWORD


def get_load_queue_subscriber():
    """
    Create or get the cached subscriber object for the submission queue.
    """
    # It's alright, pylint: disable=global-statement
    global _LOAD_QUEUE_SUBSCRIBER
    if _LOAD_QUEUE_SUBSCRIBER is None:
        _LOAD_QUEUE_SUBSCRIBER = kcidb.mq.IOSubscriber(
            PROJECT_ID,
            os.environ["KCIDB_LOAD_QUEUE_TOPIC"],
            os.environ["KCIDB_LOAD_QUEUE_SUBSCRIPTION"],
            schema=get_db_client().get_schema()[1]
        )
    return _LOAD_QUEUE_SUBSCRIBER


def get_updated_queue_publisher():
    """
    Create or get the cached publisher object for the queue with patterns
    matching objects updated by loaded submissions. Return None if update
    publishing is disabled.
    """
    # It's alright, pylint: disable=global-statement
    global _UPDATED_QUEUE_PUBLISHER
    if UPDATED_PUBLISH and _UPDATED_QUEUE_PUBLISHER is None:
        _UPDATED_QUEUE_PUBLISHER = kcidb.mq.ORMPatternPublisher(
            PROJECT_ID,
            os.environ["KCIDB_UPDATED_QUEUE_TOPIC"]
        )
    return _UPDATED_QUEUE_PUBLISHER


def get_db_client():
    """Create or retrieve the cached database client."""
    # It's alright, pylint: disable=global-statement
    global _DB_CLIENT
    if _DB_CLIENT is None:
        # Put PostgreSQL .pgpass (if any) into PGPASSFILE environment variable
        pgpass_secret = os.environ.get("KCIDB_PGPASS_SECRET")
        if pgpass_secret is not None:
            kcidb.misc.get_secret_pgpass(PROJECT_ID, pgpass_secret)
        _DB_CLIENT = kcidb.db.Client(os.environ["KCIDB_DATABASE"])
    return _DB_CLIENT


def get_oo_client():
    """Create or retrieve the cached OO database client."""
    # It's alright, pylint: disable=global-statement
    global _OO_CLIENT
    if _OO_CLIENT is None:
        _OO_CLIENT = kcidb.oo.Client(get_db_client())
    return _OO_CLIENT


def get_spool_client():
    """Create or retrieve the cached notification spool client."""
    # It's alright, pylint: disable=global-statement
    global _SPOOL_CLIENT
    if _SPOOL_CLIENT is None:
        collection_path = os.environ["KCIDB_SPOOL_COLLECTION_PATH"]
        _SPOOL_CLIENT = kcidb.monitor.spool.Client(collection_path)
    return _SPOOL_CLIENT


# pylint: disable=unused-argument

def kcidb_load_message(event, context):
    """
    Load a single message's KCIDB data from the triggering Pub Sub
    subscription into the database.
    """
    # Get new data
    data = get_load_queue_subscriber().decode_data(
        base64.b64decode(event["data"])
    )
    LOGGER.debug("DATA: %s", json.dumps(data))
    # Store it in the database
    get_db_client().load(data)
    publisher = get_updated_queue_publisher()
    if publisher:
        # Upgrade the data to the latest I/O version to enable ID extraction
        data = kcidb.io.SCHEMA.upgrade(data, copy=False)
        # Generate patterns matching all affected objects
        pattern_set = set()
        for pattern in kcidb.orm.Pattern.from_io(
                data, max_objs=UPDATED_QUEUE_OBJ_MAX):
            # TODO Avoid formatting and parsing
            pattern_set |= kcidb.orm.Pattern.parse(repr(pattern) + "<*#")
        # Publish patterns matching all affected objects, one per message
        publisher.publish_iter({pattern} for pattern in pattern_set)


def kcidb_load_queue(event, context):
    """
    Load multiple KCIDB data messages from the load queue into the database,
    if it stayed unmodified for at least DATABASE_LOAD_PERIOD.
    """
    subscriber = get_load_queue_subscriber()
    db_client = get_db_client()
    io_schema = db_client.get_schema()[1]
    publisher = get_updated_queue_publisher()
    # Do nothing, if updated recently
    now = datetime.datetime.now(datetime.timezone.utc)
    last_modified = db_client.get_last_modified()
    LOGGER.debug("Now: %s, Last modified: %s", now, last_modified)
    if last_modified and now - last_modified < DATABASE_LOAD_PERIOD:
        LOGGER.info("Database too fresh, exiting")
        return

    # Pull messages
    msgs = subscriber.pull(
        max_num=LOAD_QUEUE_MSG_MAX,
        timeout=LOAD_QUEUE_TIMEOUT_SEC,
        max_obj=LOAD_QUEUE_OBJ_MAX
    )
    if msgs:
        LOGGER.info("Pulled %u messages", len(msgs))
    else:
        LOGGER.info("Pulled nothing, exiting")
        return

    # Create merged data referencing the pulled pieces
    LOGGER.debug("Merging %u messages...", len(msgs))
    data = io_schema.merge(
        io_schema.new(),
        (msg[1] for msg in msgs),
        copy_target=False, copy_sources=False
    )
    LOGGER.info("Merged %u messages", len(msgs))
    # Load the merged data into the database
    obj_num = io_schema.count(data)
    LOGGER.debug("Loading %u objects...", obj_num)
    db_client.load(data)
    LOGGER.info("Loaded %u objects", obj_num)

    # Acknowledge all the loaded messages
    for msg in msgs:
        subscriber.ack(msg[0])
    LOGGER.debug("ACK'ed %u messages", len(msgs))

    if publisher:
        # Upgrade the data to the latest I/O version to enable ID extraction
        data = kcidb.io.SCHEMA.upgrade(data, copy=False)

        # Generate patterns matching all affected objects
        pattern_set = set()
        for pattern in kcidb.orm.Pattern.from_io(
                data, max_objs=UPDATED_QUEUE_OBJ_MAX):
            # TODO Avoid formatting and parsing
            pattern_set |= kcidb.orm.Pattern.parse(repr(pattern) + "<*#")

        # Publish patterns matching all affected objects, one per message
        publisher.publish_iter({pattern} for pattern in pattern_set)
        LOGGER.info("Published %u updates made by %u loaded objects",
                    len(pattern_set), obj_num)


def kcidb_spool_notifications(event, context):
    """
    Spool notifications about objects matching patterns arriving from a Pub
    Sub subscription
    """
    oo_client = get_oo_client()
    spool_client = get_spool_client()
    # Reset the ORM cache
    oo_client.reset_cache()
    # Get arriving data
    pattern_set = set()
    for line in base64.b64decode(event["data"]).decode().splitlines():
        pattern_set |= kcidb.orm.Pattern.parse(line)
    LOGGER.info("RECEIVED %u PATTERNS", len(pattern_set))
    LOGGER.debug(
        "PATTERNS:\n%s",
        "".join(repr(p) + "\n" for p in pattern_set)
    )
    # Spool notifications from subscriptions
    for notification in kcidb.monitor.match(oo_client.query(pattern_set)):
        if not SELECTED_SUBSCRIPTIONS or \
           notification.subscription in SELECTED_SUBSCRIPTIONS:
            LOGGER.info("POSTING %s", notification.id)
            spool_client.post(notification)
        else:
            LOGGER.info("DROPPING %s", notification.id)


def kcidb_send_notification(data, context):
    """
    Send notifications from the spool
    """
    spool_client = get_spool_client()
    # Get the notification ID
    notification_id = context.resource.split("/")[-1]
    # Pick the notification if we can
    message = spool_client.pick(notification_id)
    if not message:
        return
    LOGGER.info("SENDING %s", notification_id)
    # send message via email
    send_message(message)
    # Acknowledge notification as sent
    spool_client.ack(notification_id)


def kcidb_pick_notifications(data, context):
    """
    Pick abandoned notifications and send them.
    """
    spool_client = get_spool_client()
    for notification_id in spool_client.unpicked():
        # Pick abandoned notification and resend
        message = spool_client.pick(notification_id)
        if not message:
            continue
        LOGGER.info("SENDING %s", notification_id)
        # send message via email
        send_message(message)
        # Acknowledge notification as sent
        spool_client.ack(notification_id)


def send_message(message):
    """
    Send message via email.

    Args:
        message:    The message to send.
    """
    # Set From address, if specified
    if SMTP_FROM_ADDR:
        message['From'] = SMTP_FROM_ADDR
    # Add extra CC, if specified
    if EXTRA_CC:
        cc_addrs = message["CC"]
        if cc_addrs:
            message.replace_header("CC", cc_addrs + ", " + EXTRA_CC)
        else:
            message["CC"] = EXTRA_CC
    # If we're requested to divert messages to a PubSub topic
    publisher = get_smtp_publisher()
    if publisher is not None:
        publisher.publish(message)
    else:
        # Connect to the SMTP server
        smtp = smtplib.SMTP(host=SMTP_HOST, port=SMTP_PORT)
        smtp.ehlo()
        smtp.starttls()
        smtp.ehlo()
        smtp.login(SMTP_USER, get_smtp_password())
        try:
            # Send message
            smtp.send_message(message, to_addrs=SMTP_TO_ADDRS)
        finally:
            # Disconnect from the SMTP server
            smtp.quit()
