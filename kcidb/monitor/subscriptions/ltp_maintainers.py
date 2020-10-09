"""LTP maintainer subscription"""

from kcidb.monitor.output import NotificationMessage as Message


def match_revision(revision):
    """Match revisions of interest to LTP maintainers"""
    recipients = ["LTP Mailing List <ltp@lists.linux.it>"]
    for build in revision.builds_.values():
        for test in build.tests_.values():
            if test.path == "ltp":
                if test.status == "FAIL":
                    return (Message(recipients, "LTP failed for "),)
                if test.status == "ERROR":
                    return (Message(recipients, "LTP aborted for "),)
    return ()
