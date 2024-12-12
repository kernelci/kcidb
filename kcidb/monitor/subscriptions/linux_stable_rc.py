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

    selected_origins = {'maestro', 'broonie'}

    # If no builds found from 'maestro' or 'broonie' origin
    # or status is unknown for all the builds
    if all(b.origin not in selected_origins or b.valid is None
           for b in revision.builds):
        return ()

    # Send notification 3 hours after a revision is created/updated
    return (Message(
        subject='KernelCI report for stable-rc: '
                '{% include "stable_revision_summary.txt.j2" %}',
        to=["Jeny Sadadia <jeny.sadadia@collabora.com>",
            "Gustavo Padovan <gustavo.padovan@collabora.com>",
            "Shreeya Patel <shreeya.patel@collabora.com>"],
        body='{% include "stable_rc_revision_description.txt.j2" %}',
        cc=["KernelCI Results Staging <kernelci-results-staging@groups.io>"],
        due=datetime.now(timezone.utc) + timedelta(hours=3),
        context={'main_repo_url': repo_url}
    ),)
