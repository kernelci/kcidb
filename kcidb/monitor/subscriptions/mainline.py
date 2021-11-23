"""An example mainline tree subscriptions"""

from kcidb.monitor.output import NotificationMessage as Message


def _disabled_match_revision(revision):
    """Match revisions of interest to stable tree developers"""
    repo_url = \
        "https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git"
    subject_sfx = ' failed for {% include "revision_summary.txt.j2" %}'
    msg_args = dict(
        to=["Linux Kernel Mailing List <linux-kernel@vger.kernel.org>"],
        body='{% include "revision_description.txt.j2" %}',
    )
    if repo_url not in revision.repo_branch_checkouts:
        return ()
    if revision.builds_valid is None:
        return ()
    if not revision.builds_valid:
        return (Message(subject='Builds' + subject_sfx, **msg_args),)
    for test in revision.tests:
        # Ignore syzbot until we have issues/incidents
        if test.origin == "syzbot":
            continue
        if test.status == "FAIL" and not test.waived:
            return (Message(subject='Tests' + subject_sfx, **msg_args),)
    return ()
