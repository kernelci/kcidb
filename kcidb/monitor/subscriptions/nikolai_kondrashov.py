"""Nikolai Kondrashov's subscription"""

from kcidb.monitor.output import NotificationMessage as Message


def match_revision(revision):
    """Match revisions of interest to Nikolai Kondrashov"""
    subject_sfx = ' failed for {% include "revision_summary.txt.j2" %}'
    msg_args = dict(
        to=["Nikolai Kondrashov <Nikolai.Kondrashov@redhat.com>"],
        body='{% include "revision_description.txt.j2" %}',
    )
    if revision.checkouts_valid is not None:
        if not revision.checkouts_valid:
            return (Message(subject='Checkouts' + subject_sfx, **msg_args),)
        if revision.builds_valid is not None:
            if not revision.builds_valid:
                return (Message(subject='Builds' + subject_sfx, **msg_args),)
            if revision.tests_root.waived is False and \
               revision.tests_root.status not in \
               (None, "PASS", "DONE", "SKIP"):
                return (Message(subject='Tests' + subject_sfx, **msg_args),)
    return ()
