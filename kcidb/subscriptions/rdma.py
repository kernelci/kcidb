"""RDMA tree subscriptions"""

from kcidb.misc import NotificationMessage as Message


def match_revision(revision):
    """Match revisions of interest to RDMA tree developers"""
    recipients = ["rdma-dev-team@redhat.com"]
    for build in revision.builds_.values():
        if not build.valid:
            return (Message(recipients, "Build failed"),)
        for test in build.tests_.values():
            if test.status in ("FAIL", "ERROR"):
                return (Message(recipients, "Tests failed"),)
    return ()
