"""Stable tree subscriptions"""

from kcidb.misc import NotificationMessage as Message

REPO_URL_BASE = "https://git.kernel.org/pub/scm/linux/kernel/git"
REPO_URL_LIST = [
    REPO_URL_BASE + suffix for suffix in [
        "/stable/linux-stable-rc.git",
        "/stable/linux-stable.git",
        "/sashal/linux-stable.git",
        "/stable/stable-queue.git",
    ]
]


def match_revision(revision):
    """Match revisions of interest to stable tree developers"""
    recipients = ["Linux Stable maillist <stable@vger.kernel.org>"]
    if revision.git_repository_url not in REPO_URL_LIST:
        return ()
    for build in revision.builds_.values():
        if not build.valid:
            return (Message(recipients, "Builds failed for "),)
        for test in build.tests_.values():
            if test.status == "FAIL" and not test.waived:
                return (Message(recipients, "Tests failed for "),)
    return ()
