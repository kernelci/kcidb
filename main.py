"""Google Cloud Functions for Kernel CI reporting"""

import os
import json
import base64
import datetime
import logging
import smtplib
import kcidb_io
import kcidb


PROJECT_ID = os.environ["GCP_PROJECT"]

kcidb.misc.logging_setup(
    kcidb.misc.LOGGING_LEVEL_MAP[os.environ.get("KCIDB_LOG_LEVEL", "NONE")]
)
LOGGER = logging.getLogger()

LOAD_QUEUE_SUBSCRIBER = kcidb.mq.IOSubscriber(
    PROJECT_ID,
    os.environ["KCIDB_LOAD_QUEUE_TOPIC"],
    os.environ["KCIDB_LOAD_QUEUE_SUBSCRIPTION"]
)
LOAD_QUEUE_MSG_MAX = int(os.environ["KCIDB_LOAD_QUEUE_MSG_MAX"])
LOAD_QUEUE_OBJ_MAX = int(os.environ["KCIDB_LOAD_QUEUE_OBJ_MAX"])
LOAD_QUEUE_TIMEOUT_SEC = float(os.environ["KCIDB_LOAD_QUEUE_TIMEOUT_SEC"])

DATABASE = os.environ["KCIDB_DATABASE"]
DATABASE_LOAD_PERIOD = datetime.timedelta(
    seconds=int(os.environ["KCIDB_DATABASE_LOAD_PERIOD_SEC"])
)

SELECTED_SUBSCRIPTIONS = \
    os.environ.get("KCIDB_SELECTED_SUBSCRIPTIONS", "").split()

SPOOL_COLLECTION_PATH = os.environ["KCIDB_SPOOL_COLLECTION_PATH"]

SMTP_HOST = os.environ["KCIDB_SMTP_HOST"]
SMTP_PORT = int(os.environ["KCIDB_SMTP_PORT"])
SMTP_USER = os.environ["KCIDB_SMTP_USER"]
SMTP_PASSWORD_SECRET = os.environ["KCIDB_SMTP_PASSWORD_SECRET"]
SMTP_PASSWORD = kcidb.misc.get_secret(PROJECT_ID, SMTP_PASSWORD_SECRET)
SMTP_FROM_ADDR = os.environ.get("KCIDB_SMTP_FROM_ADDR", None)
SMTP_TO_ADDRS = os.environ.get("KCIDB_SMTP_TO_ADDRS", None)

DB_CLIENT = kcidb.db.Client(DATABASE)
SPOOL_CLIENT = kcidb.monitor.spool.Client(SPOOL_COLLECTION_PATH)
LOADED_QUEUE_PUBLISHER = kcidb.mq.IOPublisher(
    PROJECT_ID,
    os.environ["KCIDB_LOADED_QUEUE_TOPIC"]
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
    # Forward the data to the "loaded" MQ topic
    LOADED_QUEUE_PUBLISHER.publish(data)


def kcidb_load_queue_msgs(subscriber, msg_max, obj_max, timeout_sec):
    """
    Pull I/O data messages from a subscriber with a limit on message number,
    total object number and time spent.

    Args:
        subscriber:     The subscriber (kcidb.mq.Subscriber) to pull from.
        msg_max:        Maximum number of messages to pull.
        obj_max:        Maximum number of objects to pull.
        timeout_sec:    Maximum number of seconds to spend.

    Returns:
        The list of pulled messages.
    """
    # Yeah it's crowded, but bear with us, pylint: disable=too-many-locals
    # Pull data from queue until we get enough, or time runs out
    start = datetime.datetime.now(datetime.timezone.utc)
    obj_num = 0
    pulls = 0
    msgs = []
    while True:
        # Calculate remaining messages
        pull_msg_max = msg_max - len(msgs)
        if pull_msg_max <= 0:
            LOGGER.debug("Received enough messages")
            break

        # Calculate remaining time
        pull_timeout_sec = \
            timeout_sec - \
            (datetime.datetime.now(datetime.timezone.utc) - start). \
            total_seconds()
        if pull_timeout_sec <= 0:
            LOGGER.debug("Ran out of time")
            break

        # Pull
        LOGGER.debug("Pulling <= %u messages from the queue, "
                     "with timeout %us...", pull_msg_max, pull_timeout_sec)
        pull_msgs = subscriber.pull(pull_msg_max, timeout=pull_timeout_sec)
        pulls += 1
        LOGGER.debug("Pulled %u messages", len(pull_msgs))

        # Add messages up to obj_max, except the first one
        for index, msg in enumerate(pull_msgs):
            msg_obj_num = kcidb_io.count(msg[1])
            obj_num += msg_obj_num
            if msgs and obj_num > obj_max:
                LOGGER.debug("Message #%u crossed %u-object boundary "
                             "at %u total objects",
                             len(msgs) + 1, obj_max, obj_num)
                obj_num -= msg_obj_num
                for nack_msg in pull_msgs[index:]:
                    subscriber.nack(nack_msg[0])
                LOGGER.debug("NACK'ed %s messages", len(pull_msgs) - index)
                break
            msgs.append(msg)
        else:
            continue
        break

    duration_seconds = \
        (datetime.datetime.now(datetime.timezone.utc) - start).total_seconds()
    LOGGER.debug("Pulled %u messages, %u objects total "
                 "in %u pulls and %u seconds",
                 len(msgs), obj_num, pulls, duration_seconds)
    return msgs


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
    msgs = kcidb_load_queue_msgs(LOAD_QUEUE_SUBSCRIBER,
                                 LOAD_QUEUE_MSG_MAX,
                                 LOAD_QUEUE_OBJ_MAX,
                                 LOAD_QUEUE_TIMEOUT_SEC)
    if msgs:
        LOGGER.info("Pulled %u messages", len(msgs))
    else:
        LOGGER.info("Pulled nothing, exiting")
        return

    # Create merged data referencing the pulled pieces
    LOGGER.debug("Merging %u messages...", len(msgs))
    data = kcidb_io.merge(kcidb_io.new(), (msg[1] for msg in msgs),
                          copy_target=False, copy_sources=False)
    LOGGER.info("Merged %u messages", len(msgs))
    # Load the merged data into the database
    obj_num = kcidb_io.count(data)
    LOGGER.debug("Loading %u objects...", obj_num)
    DB_CLIENT.load(data)
    LOGGER.info("Loaded %u objects", obj_num)

    # Acknowledge all the loaded messages
    for msg in msgs:
        LOAD_QUEUE_SUBSCRIBER.ack(msg[0])
    LOGGER.debug("ACK'ed %u messages", len(msgs))

    # Forward the loaded data to the "loaded" topic
    LOADED_QUEUE_PUBLISHER.publish(data)
    LOGGER.info("Forwarded %u objects", obj_num)


def kcidb_spool_notifications(event, context):
    """
    Spool notifications about KCIDB data arriving from a Pub Sub subscription
    """
    # Get arriving data
    new_io = kcidb.mq.IOSubscriber.decode_data(
                        base64.b64decode(event["data"]))
    LOGGER.debug("DATA: %s", json.dumps(new_io))
    # Load the arriving data (if stored) and all its parents and children
    base_io = DB_CLIENT.complement(new_io)
    # Spool notifications from subscriptions
    for notification in \
            kcidb.monitor.match_new_io(base_io, new_io, copy=False):
        if not SELECTED_SUBSCRIPTIONS or \
           notification.subscription in SELECTED_SUBSCRIPTIONS:
            LOGGER.info("POSTING %s", notification.id)
            SPOOL_CLIENT.post(notification)
        else:
            LOGGER.info("DROPPING ID %s", notification.id)
            LOGGER.debug("DROPPING MESSAGE:\n%s",
                         notification.render().as_string())


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
    # Set From address, if specified
    if SMTP_FROM_ADDR:
        message['From'] = SMTP_FROM_ADDR
    # Connect to the SMTP server
    smtp = smtplib.SMTP(host=SMTP_HOST, port=SMTP_PORT)
    smtp.ehlo()
    smtp.starttls()
    smtp.ehlo()
    smtp.login(SMTP_USER, SMTP_PASSWORD)
    try:
        # Send message
        LOGGER.info("SENDING %s", notification_id)
        smtp.send_message(message, to_addrs=SMTP_TO_ADDRS)
    finally:
        # Disconnect from the SMTP server
        smtp.quit()
    # Acknowledge notification as sent
    SPOOL_CLIENT.ack(notification_id)
