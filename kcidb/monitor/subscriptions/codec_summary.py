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

    # If only "validate-fluster-results" found or environment compatible
    # is not defined for any tests
    if all(
        t.path.endswith('.validate-fluster-results') or
        not t.environment_compatible
        for t in revision.tests_root["fluster"].tests
    ):
        return ()

    # Don't generate summary report if revision has fluster job nodes
    # and doesn't have any specific test nodes as codec summary
    # report is intended to provide stats of number of passed, failed, and
    # total tests of fluster jobs
    # Below is the maestro convention for fluster tests:
    # Job node path: "fluster.chromeos.v4l2.gstreamer_vp9"
    # Test node path: "fluster.chromeos.v4l2.gstreamer_vp9.<test-name>"
    if all(
        len(t.path.split('.')) <= 4
        for t in revision.tests_root["fluster"].tests
    ):
        return ()

    # Send notification 3 hours after a revision is created/updated
    return (Message(
        subject='KernelCI report for fluster tests: '
                '{% include "fluster_revision_summary.txt.j2" %}',
        to=["Denis Yuji Shimizu <denis.shimizu@collabora.com>"],
        body='{% include "fluster_revision_description.txt.j2" %}',
        cc=["KernelCI Results Staging <kernelci-results-staging@groups.io>",
            "Helen Mae Koike Fornazier <helen.koike@collabora.com>",
            "Jeny Sadadia <jeny.sadadia@collabora.com>"],
        due=datetime.now(timezone.utc) + timedelta(hours=3)
    ),)
