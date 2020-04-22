"""Google Cloud Functions for Kernel CI reporting"""

import os
import base64
import logging
import smtplib
import kcidb


PROJECT_ID = os.environ["GCP_PROJECT"]

LOGGER = logging.getLogger()
kcidb.misc.logging_setup(
    kcidb.misc.LOGGING_LEVEL_MAP[os.environ.get("KCIDB_LOG_LEVEL", "NONE")]
)

DATASET = os.environ["KCIDB_DATASET"]
MQ_LOADED_TOPIC = os.environ["KCIDB_MQ_LOADED_TOPIC"]

SMTP_HOST = os.environ["KCIDB_SMTP_HOST"]
SMTP_PORT = int(os.environ["KCIDB_SMTP_PORT"])
SMTP_USER = os.environ["KCIDB_SMTP_USER"]
SMTP_PASSWORD_SECRET = os.environ["KCIDB_SMTP_PASSWORD_SECRET"]
SMTP_PASSWORD = kcidb.misc.get_secret(PROJECT_ID, SMTP_PASSWORD_SECRET)
SMTP_FROM_ADDR = os.environ.get("KCIDB_SMTP_FROM_ADDR", None)
SMTP_TO_ADDRS = os.environ.get("KCIDB_SMTP_TO_ADDRS", None)

DB_CLIENT = kcidb.db.Client(DATASET)
SPOOL_CLIENT = kcidb.spool.Client()
MQ_LOADED_PUBLISHER = kcidb.mq.Publisher(PROJECT_ID, MQ_LOADED_TOPIC)


# pylint: disable=unused-argument

def kcidb_load(event, context):
    """
    Load KCIDB data from a Pub Sub subscription into the dataset
    """
    # Get new data
    io_new = kcidb.mq.Subscriber.decode_data(base64.b64decode(event["data"]))
    # Store it in the database
    DB_CLIENT.load(io_new)
    # Forward the data to the "loaded" MQ topic
    MQ_LOADED_PUBLISHER.publish(io_new)


def kcidb_spool_notifications(event, context):
    """
    Spool notifications about KCIDB data arriving from a Pub Sub subscription
    """
    # Get arriving data
    new_io = kcidb.mq.Subscriber.decode_data(base64.b64decode(event["data"]))
    # Load the arriving data (if stored) and all its parents and children
    base_io = DB_CLIENT.complement(new_io)
    # Spool notifications from subscriptions
    for notification in kcidb.subscriptions.match_new_io(base_io, new_io):
        LOGGER.info("POSTING %s", notification.id)
        SPOOL_CLIENT.post(notification)


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
