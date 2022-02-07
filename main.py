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
LOAD_QUEUE_SUBSCRIBER = kcidb.mq.IOSubscriber(
    PROJECT_ID,
    os.environ["KCIDB_LOAD_QUEUE_TOPIC"],
    os.environ["KCIDB_LOAD_QUEUE_SUBSCRIPTION"]
)
# Maximum number of messages loaded from the submission queue in one go
LOAD_QUEUE_MSG_MAX = int(os.environ["KCIDB_LOAD_QUEUE_MSG_MAX"])
# Maximum number of objects loaded from the submission queue in one go
LOAD_QUEUE_OBJ_MAX = int(os.environ["KCIDB_LOAD_QUEUE_OBJ_MAX"])
# Maximum time for pulling maximum amount of submissions from the queue
LOAD_QUEUE_TIMEOUT_SEC = float(os.environ["KCIDB_LOAD_QUEUE_TIMEOUT_SEC"])

# String specifying the database to access,
# i.e. the kcidb.db.Client.__init__() argument.
DATABASE = os.environ["KCIDB_DATABASE"]
# Minimum time between loading submissions into the database
DATABASE_LOAD_PERIOD = datetime.timedelta(
    seconds=int(os.environ["KCIDB_DATABASE_LOAD_PERIOD_SEC"])
)

# A whitespace-separated list of subscription names to limit notifying to
SELECTED_SUBSCRIPTIONS = \
    os.environ.get("KCIDB_SELECTED_SUBSCRIPTIONS", "").split()

# A Firestore path to the collection for spooled notifications
SPOOL_COLLECTION_PATH = os.environ["KCIDB_SPOOL_COLLECTION_PATH"]

# The address of the SMTP host to send notifications through
SMTP_HOST = os.environ["KCIDB_SMTP_HOST"]
# The port of the SMTP server to send notifications through
SMTP_PORT = int(os.environ["KCIDB_SMTP_PORT"])
# The username to authenticate to SMTP server with
SMTP_USER = os.environ["KCIDB_SMTP_USER"]
# The name of the Google Secret Manager's "secret" with SMTP user's password
SMTP_PASSWORD_SECRET = os.environ["KCIDB_SMTP_PASSWORD_SECRET"]
# The SMTP user's password
SMTP_PASSWORD = kcidb.misc.get_secret(PROJECT_ID, SMTP_PASSWORD_SECRET)
# The address to tell the SMTP server to send the message to,
# overriding any recipients in the message itself.
SMTP_TO_ADDRS = os.environ.get("KCIDB_SMTP_TO_ADDRS", None)
# The name of the PubSub topic to post email messages to, instead of sending
# them to the SMTP server with SMTP_* parameters specified above. If
# specified, those parameters are ignored.
SMTP_TOPIC = os.environ.get("KCIDB_SMTP_TOPIC", None)
# The publisher object for email messages, if we're requested to send them to
# a PubSub topic
SMTP_PUBLISHER = None if SMTP_TOPIC is None \
    else kcidb.mq.EmailPublisher(PROJECT_ID, SMTP_TOPIC)
# The name of the subscription for email messages posted to the SMTP topic
SMTP_SUBSCRIPTION = os.environ.get("KCIDB_SMTP_SUBSCRIPTION", None)
# The address to use as the "From" address in sent notifications
SMTP_FROM_ADDR = os.environ.get("KCIDB_SMTP_FROM_ADDR", None)
# A string to be added to the CC header of notifications being sent out
EXTRA_CC = os.environ.get("KCIDB_EXTRA_CC", None)

# The database client instance
DB_CLIENT = kcidb.db.Client(DATABASE)
# The object-oriented database client instance
OO_CLIENT = kcidb.oo.Client(DB_CLIENT)
# The notification spool client
SPOOL_CLIENT = kcidb.monitor.spool.Client(SPOOL_COLLECTION_PATH)
# The publisher object for the queue with patterns matching objects updated by
# loading submissions.
UPDATED_QUEUE_PUBLISHER = kcidb.mq.ORMPatternPublisher(
    PROJECT_ID,
    os.environ["KCIDB_UPDATED_QUEUE_TOPIC"]
)


# pylint: disable=unused-argument

def kcidb_load_message(event, context):
    """
    Load a single message's KCIDB data from the triggering Pub Sub
    subscription into the database.
    """
    # Get new data
    data = kcidb.mq.IOSubscriber.decode_data(base64.b64decode(event["data"]))
    LOGGER.debug("DATA: %s", json.dumps(data))
    # Store it in the database
    DB_CLIENT.load(data)
    # Generate patterns matching all affected objects
    pattern_set = set()
    for pattern in kcidb.orm.Pattern.from_io(data):
        # TODO Avoid formatting and parsing
        pattern_set |= kcidb.orm.Pattern.parse(repr(pattern) + "<*#")
    # Publish patterns matching all affected objects
    UPDATED_QUEUE_PUBLISHER.publish(pattern_set)


def kcidb_load_queue(event, context):
    """
    Load multiple KCIDB data messages from the LOAD_QUEUE_SUBSCRIBER queue
    into the database, if it stayed unmodified for at least
    DATABASE_LOAD_PERIOD.
    """
    # Do nothing, if updated recently
    now = datetime.datetime.now(datetime.timezone.utc)
    last_modified = DB_CLIENT.get_last_modified()
    LOGGER.debug("Now: %s, Last modified: %s", now, last_modified)
    if last_modified and now - last_modified < DATABASE_LOAD_PERIOD:
        LOGGER.info("Database too fresh, exiting")
        return

    # Pull messages
    msgs = LOAD_QUEUE_SUBSCRIBER.pull(
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
    data = kcidb.io.SCHEMA.merge(
        kcidb.io.SCHEMA.new(),
        (msg[1] for msg in msgs),
        copy_target=False, copy_sources=False
    )
    LOGGER.info("Merged %u messages", len(msgs))
    # Load the merged data into the database
    obj_num = kcidb.io.SCHEMA.count(data)
    LOGGER.debug("Loading %u objects...", obj_num)
    DB_CLIENT.load(data)
    LOGGER.info("Loaded %u objects", obj_num)

    # Acknowledge all the loaded messages
    for msg in msgs:
        LOAD_QUEUE_SUBSCRIBER.ack(msg[0])
    LOGGER.debug("ACK'ed %u messages", len(msgs))

    # Generate patterns matching all affected objects
    pattern_set = set()
    for pattern in kcidb.orm.Pattern.from_io(data):
        # TODO Avoid formatting and parsing
        pattern_set |= kcidb.orm.Pattern.parse(repr(pattern) + "<*#")

    # Publish patterns matching all affected objects
    UPDATED_QUEUE_PUBLISHER.publish(pattern_set)
    LOGGER.info("Published updates made by %u loaded objects", obj_num)


def kcidb_spool_notifications(event, context):
    """
    Spool notifications about objects matching patterns arriving from a Pub
    Sub subscription
    """
    # Reset the ORM cache
    OO_CLIENT.reset_cache()
    # Get arriving data
    pattern_set = kcidb.mq.ORMPatternSubscriber.decode_data(
        base64.b64decode(event["data"])
    )
    LOGGER.debug(
        "PATTERNS:\n%s",
        "".join(repr(p) + "\n" for p in pattern_set)
    )
    # Spool notifications from subscriptions
    for notification in kcidb.monitor.match(OO_CLIENT.query(pattern_set)):
        if not SELECTED_SUBSCRIPTIONS or \
           notification.subscription in SELECTED_SUBSCRIPTIONS:
            LOGGER.info("POSTING %s", notification.id)
            SPOOL_CLIENT.post(notification)
        else:
            LOGGER.info("DROPPING %s", notification.id)


def kcidb_send_notification(data, context):
    """
    Send notifications from the spool
    """
    # Get the notification ID
    notification_id = context.resource.split("/")[-1]
    # Pick the notification if we can
    message = SPOOL_CLIENT.pick(notification_id)
    if not message:
        return
    LOGGER.info("SENDING %s", notification_id)
    # send message via email
    send_message(message)
    # Acknowledge notification as sent
    SPOOL_CLIENT.ack(notification_id)


def kcidb_pick_notifications(data, context):
    """
    Pick abandoned notifications and send them.
    """
    for notification_id in SPOOL_CLIENT.unpicked():
        # Pick abandoned notification and resend
        message = SPOOL_CLIENT.pick(notification_id)
        if not message:
            continue
        LOGGER.info("SENDING %s", notification_id)
        # send message via email
        send_message(message)
        # Acknowledge notification as sent
        SPOOL_CLIENT.ack(notification_id)


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
    if SMTP_PUBLISHER is not None:
        SMTP_PUBLISHER.publish(message)
    else:
        # Connect to the SMTP server
        smtp = smtplib.SMTP(host=SMTP_HOST, port=SMTP_PORT)
        smtp.ehlo()
        smtp.starttls()
        smtp.ehlo()
        smtp.login(SMTP_USER, SMTP_PASSWORD)
        try:
            # Send message
            smtp.send_message(message, to_addrs=SMTP_TO_ADDRS)
        finally:
            # Disconnect from the SMTP server
            smtp.quit()
