"""Google Cloud Functions for Kernel CI reporting"""

import os
import sys
import json
import base64
import smtplib
import kcidb

DATASET = os.environ["KCIDB_DATASET"]
SMTP_HOST = os.environ.get("KCIDB_SMTP_HOST", "")
SMTP_TO_ADDRS = os.environ.get("KCIDB_SMTP_TO_ADDRS", None)

DB_CLIENT = kcidb.db.Client(DATASET)
SPOOL_CLIENT = kcidb.spool.Client()


# pylint: disable=unused-argument
def kcidb_consume(event, context):
    """
    Consume KCIDB data from a Pub Sub subscription
    """
    # Get new data
    io_new = kcidb.mq.Subscriber.decode_data(base64.b64decode(event["data"]))
    print("RECEIVED")
    json.dump(io_new, sys.stdout)
    # Store it in the database
    DB_CLIENT.load(io_new)
    # Load the complement: the new data along with all the linked
    # parents and children
    io_complement = DB_CLIENT.complement(io_new)
    # Convert new and complemented data to object-oriented representation
    oo_new = kcidb.oo.from_io(io_new)
    oo_complement = kcidb.oo.from_io(io_complement)
    # Remove all objects with missing parents
    oo_rooted_complement = kcidb.oo.remove_orphans(oo_complement)
    # Delist everything except new or modified objects, but keep references
    oo_new_rooted_complement = kcidb.oo.apply_mask(oo_rooted_complement,
                                                   oo_new)
    print("NOTIFIED")
    json.dump(kcidb.oo.to_io(oo_new_rooted_complement), sys.stdout)
    # Spool notifications from subscriptions
    for notification in kcidb.subscriptions.match(oo_new_rooted_complement):
        SPOOL_CLIENT.put(notification)


def kcidb_handle_notification():
    """
    Handle notifications put onto the spool
    """
    # Pick the notification if we can
    message = SPOOL_CLIENT.pick(id)
    if not message:
        return
    # Connect to the SMTP server
    smtp = smtplib.SMTP(SMTP_HOST)
    try:
        # Send message
        smtp.send_message(message, to_addrs=SMTP_TO_ADDRS)
    finally:
        # Disconnect from the SMTP server
        smtp.quit()
    # Acknowledge notification as sent
    SPOOL_CLIENT.ack(id)
