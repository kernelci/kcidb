"""Linux stable-rc subscription"""
from datetime import (timezone, datetime, timedelta)

from kcidb.monitor.output import NotificationMessage as Message


def match_revision(revision):
    """Match revisions of stable-rc tree"""
    # Repo we're interested in
    repo_url = "https://git.kernel.org/pub/scm/linux/kernel/git/stable/" \
        "linux-stable-rc.git"

    # If the revision is not from stable-rc repo,
    # or there are no finished builds

    if repo_url not in revision.repo_branch_checkouts:
        return ()

    if revision.builds_valid is None:
        return ()

    # We need to send the notification one hour after the last build came in.
    # However, we currently have no way of distinguishing build updates
    # from updates to build's tests, and we can't send a notification
    # about a revision from a build update yet. So we're sending the
    # notification one hour after any revision update.
    return (Message(
        subject='KernelCI report for stable-rc: '
                '{% include "stable_rc_revision_summary.txt.j2" %}',
        to=["Jeny Sadadia <jeny.sadadia@collabora.com>",
            "Gustavo Padovan <gustavo.padovan@collabora.com>",
            "Shreeya Patel <shreeya.patel@collabora.com>"],
        body='{% include "stable_rc_revision_description.txt.j2" %}',
        cc=["KernelCI Results Staging <kernelci-results-staging@groups.io>"],
        due=datetime.now(timezone.utc) + timedelta(hours=1)
    ),)
