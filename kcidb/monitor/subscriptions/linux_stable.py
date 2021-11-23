"""An example stable tree subscriptions"""

from kcidb.monitor.output import NotificationMessage as Message

REPO_URL_BASE = "https://git.kernel.org/pub/scm/linux/kernel/git"
REPO_URL_SET = {
    REPO_URL_BASE + suffix for suffix in [
        "/stable/linux-stable-rc.git",
        "/stable/linux-stable.git",
        "/sashal/linux-stable.git",
        "/stable/stable-queue.git",
    ]
}


def _disabled_match_revision(revision):
    """Match revisions of interest to stable tree developers"""
    subject_sfx = ' failed for {% include "revision_summary.txt.j2" %}'
    msg_args = dict(
        to=["Linux Stable maillist <stable@vger.kernel.org>"],
        body='{% include "revision_description.txt.j2" %}',
    )
    if not set(revision.repo_branch_checkouts) & REPO_URL_SET:
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
