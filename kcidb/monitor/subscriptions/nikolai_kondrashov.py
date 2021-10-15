"""Nikolai Kondrashov's subscription"""

from kcidb.monitor.output import NotificationMessage as Message

RECIPIENTS = ["Nikolai Kondrashov <Nikolai.Kondrashov@redhat.com>"]


def match_revision(revision):
    """Match revisions of interest to Nikolai Kondrashov"""
    if revision.checkouts_valid is not None:
        if not revision.checkouts_valid:
            return (Message(RECIPIENTS, "Checkouts failed for "),)
        if revision.builds_valid is not None:
            if not revision.builds_valid:
                return (Message(RECIPIENTS, "Builds failed for "),)
            if revision.tests_root.waived is False and \
               revision.tests_root.status not in \
               (None, "PASS", "DONE", "SKIP"):
                return (Message(RECIPIENTS, "Tests failed for "),)
    return ()
