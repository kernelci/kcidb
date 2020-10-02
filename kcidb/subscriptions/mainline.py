"""Mainline tree subscriptions"""

from kcidb.misc import NotificationMessage as Message

REPO_URL = \
    "https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git"
RECIPIENTS = ["Linux Kernel Mailing List <linux-kernel@vger.kernel.org>"]


def match_revision(revision):
    """Match revisions of interest to stable tree developers"""
    if revision.git_repository_url != REPO_URL:
        return ()
    for build in revision.builds_.values():
        if not build.valid:
            return (Message(RECIPIENTS, "Builds failed for "),)
        for test in build.tests_.values():
            # Ignore syzbot until we have issues/incidents
            if test.origin == "syzbot":
                continue
            if test.status == "FAIL" and not test.waived:
                return (Message(RECIPIENTS, "Tests failed for "),)
    return ()
