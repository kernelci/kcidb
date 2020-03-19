"""LTP maintainer subscription"""

from kcidb.misc import NotificationMessage as Message


def match_revision(revision):
    """Match revisions of interest to LTP maintainers"""
    recipients = ["LTP Mailing List <ltp@lists.linux.it>"]
    for build in revision.builds_:
        for test in build.tests_:
            if test.path == "ltp":
                if test.status == "FAIL":
                    return (Message(recipients, "LTP failed"),)
                if test.status == "ERROR":
                    return (Message(recipients, "LTP aborted"),)
    return ()
