"""Mark Brown's subscription"""
from datetime import (timezone, datetime, timedelta)

from kcidb.monitor.output import NotificationMessage as Message


def match_revision(revision):
    """Match revisions of interest to Mark Brown"""
    # Repos we're interested in
    repo_urls = {
        f"https://git.kernel.org/pub/scm/linux/kernel/git/{r}.git"
        for r in (
            "arm64/linux",
            "soc/soc",
            "broonie/ci",
            "broonie/misc",
            "broonie/regmap",
            "broonie/regulator",
            "broonie/sound",
            "broonie/spi",
        )
    }
    # If the revision is not from one of our repos,
    # or there are no finished builds
    if not repo_urls & set(revision.repo_branch_checkouts) or \
            revision.builds_valid is None:
        return ()

    # We need to send the notification one hour after the last build came in.
    # However, we currently have no way of distinguishing build updates
    # from updates to build's tests, and we can't send a notification
    # about a revision from a build update yet. So we're sending the
    # notification one hour after any revision update.
    return (Message(
        subject='Testing done for {% include "revision_summary.txt.j2" %}',
        to=["Mark Brown <broonie@kernel.org>"],
        body='{% include "revision_description.txt.j2" %}',
        due=datetime.now(timezone.utc) + timedelta(hours=1)
    ),)
