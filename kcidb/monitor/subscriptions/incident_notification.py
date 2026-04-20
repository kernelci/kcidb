"""Incident notifications"""

from kcidb.monitor.output import NotificationMessage as Message


def match_incident(incident):
    """Send notification upon incident creation"""

    print(f"Generating notification for {incident.id}")
    # if not incident.issue.subsriber:
    #     return ()

    # static subscriber for testing
    subscriber = "Jeny Sadadia <jeny.sadadia@collabora.com>"

    return (Message(
        subject='Incident notification for {% include "incident_summary.txt.j2" %}',
        to=[subscriber],
        body='{% include "incident_description.txt.j2" %}',
    ),)
