"""Linux stable-rt subscription"""
from datetime import (timezone, datetime, timedelta)

from kcidb.monitor.output import NotificationMessage as Message


def match_revision(revision):
    """Match revisions of stable-rt tree"""
    # Repo we're interested in
    repo_url = "https://git.kernel.org/pub/scm/linux/kernel/git/rt/" \
        "linux-stable-rt.git"

    # If the revision is not from stable-rt repo,
    # or there are no finished builds

    if repo_url not in revision.repo_branch_checkouts:
        return ()

    selected_origins = {'maestro', 'broonie'}
    # If the revision is not from 'maestro' or 'broonie' origin
    if not {c.origin for c in revision.checkouts} & selected_origins:
        return ()

    # Return if there are no builds or status is unknown for all the builds
    if revision.builds_valid is None:
        return ()

    # If no builds found from 'maestro' or 'broonie' origin
    builds = [build for build in revision.builds
              if build.origin in selected_origins]
    if not builds:
        return ()

    # Send notification 3 hours after a revision is created/updated
    return (Message(
        subject='KernelCI report for stable-rt: '
                '{% include "stable_revision_summary.txt.j2" %}',
        to=["stable-rt <stable-rt@vger.kernel.org>"],
        body='{% include "stable_rt_revision_description.txt.j2" %}',
        cc=["KernelCI Results Staging <kernelci-results-staging@groups.io>",
            "Jeny Sadadia <jeny.sadadia@collabora.com>",
            "Shreeya Patel <shreeya.patel@collabora.com>",
            "Helen Mae Koike Fornazier <helen.koike@collabora.com>",
            ],
        due=datetime.now(timezone.utc) + timedelta(hours=3)
    ),)
