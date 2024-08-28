"""Codec summary subscription"""
from datetime import (timezone, datetime, timedelta)

from kcidb.monitor.output import NotificationMessage as Message


def match_revision(revision):
    """Match revisions for codec summary report from Maestro"""

    if revision.builds_valid is None:
        return ()

    # If the revision is not from 'maestro' origin
    if not {c.origin for c in revision.checkouts} & {'maestro'}:
        return ()

    # If fluster tests not found
    if not revision.tests_root["fluster"].nodes:
        return ()

    # Send notification 3 hours after a revision is created/updated
    return (Message(
        subject='KernelCI report for fluster tests: '
                '{% include "fluster_revision_summary.txt.j2" %}',
        to=["Denis Yuji Shimizu <denis.shimizu@collabora.com>"],
        body='{% include "fluster_revision_description.txt.j2" %}',
        cc=["KernelCI Results Staging <kernelci-results-staging@groups.io>",
            "Helen Mae Koike Fornazier <helen.koike@collabora.com>",
            "Gustavo Padovan <gustavo.padovan@collabora.com>",
            "Jeny Sadadia <jeny.sadadia@collabora.com>"],
        due=datetime.now(timezone.utc) + timedelta(hours=3)
    ),)
