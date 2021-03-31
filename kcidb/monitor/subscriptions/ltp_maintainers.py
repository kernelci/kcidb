"""LTP maintainer subscription"""

from kcidb.monitor.output import NotificationMessage as Message


def match_revision(revision):
    """Match revisions of interest to LTP maintainers"""
    recipients = ["LTP Mailing List <ltp@lists.linux.it>"]
    status = revision.tests_root["ltp"].status
    if status == "FAIL":
        return (Message(recipients, "LTP failed for "),)
    if status == "ERROR":
        return (Message(recipients, "LTP aborted for "),)
    return ()
