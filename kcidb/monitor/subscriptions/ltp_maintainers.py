"""An example LTP maintainer subscription"""

from kcidb.monitor.output import NotificationMessage as Message


def _disabled_match_revision(revision):
    """Match revisions of interest to LTP maintainers"""
    summary = '{% include "revision_summary.txt.j2" %}'
    msg_args = dict(
        to=["LTP Mailing List <ltp@lists.linux.it>"],
        body='{% include "revision_description.txt.j2" %}',
    )
    status = revision.tests_root["ltp"].status
    if status == "FAIL":
        return (Message(subject="LTP failed for " + summary, **msg_args),)
    if status == "ERROR":
        return (Message(subject="LTP aborted for " + summary, **msg_args),)
    return ()
