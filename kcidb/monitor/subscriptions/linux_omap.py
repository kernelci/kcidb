"""Linux omap subscription"""
from datetime import (timezone, datetime, timedelta)

from kcidb.monitor.output import NotificationMessage as Message


def match_revision(revision):
    """Match revisions of omap tree"""
    # Repo we're interested in
    repo_url = "https://git.kernel.org/pub/scm/linux/kernel/git/tmlind/" \
        "linux-omap.git"

    # If the revision is not from omap repo,
    # or there are no finished builds

    if repo_url not in revision.repo_branch_checkouts:
        return ()

    if revision.builds_valid is None:
        return ()

    # If the revision is not from 'maestro' or 'broonie' origin
    if not {c.origin for c in revision.checkouts} & {'maestro', 'broonie'}:
        return ()

    # Send notification 3 hours after a revision is created/updated
    return (Message(
        subject='KernelCI report for omap: '
                '{% include "stable_revision_summary.txt.j2" %}',
        to=["Kevin Hilman <khilman@baylibre.com>"],
        cc=["KernelCI Results Staging <kernelci-results-staging@groups.io>",
            "Jeny Sadadia <jeny.sadadia@collabora.com>",
            "Gustavo Padovan <gustavo.padovan@collabora.com>",
            "Helen Mae Koike Fornazier <helen.koike@collabora.com>"],
        body='{% include "stable_rc_revision_description.txt.j2" %}',
        due=datetime.now(timezone.utc) + timedelta(hours=3),
        context={'main_repo_url': repo_url}
    ),)
