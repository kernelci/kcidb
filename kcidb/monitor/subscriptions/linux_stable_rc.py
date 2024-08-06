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

    # Only take into account results from maestro and broonie as of now
    revision_objs = [revision.checkouts, revision.builds, revision.tests]
    for revision_obj in revision_objs:
        revision_obj_copy = revision_obj.copy()
        for obj in revision_obj_copy:
            if obj.origin not in ("maestro", "broonie"):
                revision_obj.remove(obj)

    # Send notification 3 hours after a revision is created/updated
    return (Message(
        subject='KernelCI report for stable-rc: '
                '{% include "stable_rc_revision_summary.txt.j2" %}',
        to=["Jeny Sadadia <jeny.sadadia@collabora.com>",
            "Gustavo Padovan <gustavo.padovan@collabora.com>",
            "Shreeya Patel <shreeya.patel@collabora.com>"],
        body='{% include "stable_rc_revision_description.txt.j2" %}',
        cc=["KernelCI Results Staging <kernelci-results-staging@groups.io>"],
        due=datetime.now(timezone.utc) + timedelta(hours=3)
    ),)
