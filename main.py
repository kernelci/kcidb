"""Google Cloud Functions for Kernel CI reporting"""

import os
import base64
# import smtplib
from google.cloud import secretmanager
import kcidb


def get_secret(project_id, secret_id):
    """
    Get the latest version of a secret from secret manager.

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


PROJECT_ID = os.environ["GCP_PROJECT"]
DATASET = os.environ["KCIDB_DATASET"]
SMTP_HOST = os.environ.get("KCIDB_SMTP_HOST", "")
SMTP_TO_ADDRS = os.environ.get("KCIDB_SMTP_TO_ADDRS", None)
MQ_LOADED_TOPIC = os.environ["KCIDB_MQ_LOADED_TOPIC"]

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
    # Get loaded data
    io_loaded = kcidb.mq.Subscriber.decode_data(
        base64.b64decode(event["data"])
    )
    # Load the complement: the loaded data along with all the linked
    # parents and children
    io_complement = DB_CLIENT.complement(io_loaded)
    # Convert loaded and complemented data to object-oriented representation
    oo_loaded = kcidb.oo.from_io(io_loaded)
    oo_complement = kcidb.oo.from_io(io_complement)
    # Remove all objects with missing parents
    oo_rooted_complement = kcidb.oo.remove_orphans(oo_complement)
    # Delist everything except loaded or modified objects, but keep references
    oo_loaded_rooted_complement = kcidb.oo.apply_mask(oo_rooted_complement,
                                                      oo_loaded)
    # Spool notifications from subscriptions
    for notification in kcidb.subscriptions.match(oo_loaded_rooted_complement):
        print("NOTIFICATION ID:", notification.id)
        SPOOL_CLIENT.put(notification)


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
    # Connect to the SMTP server
    # smtp = smtplib.SMTP(SMTP_HOST)
    try:
        # Send message
        # smtp.send_message(message, to_addrs=SMTP_TO_ADDRS)
        print(message.as_string())
    finally:
        # Disconnect from the SMTP server
        # smtp.quit()
        pass
    # Acknowledge notification as sent
    SPOOL_CLIENT.ack(notification_id)
