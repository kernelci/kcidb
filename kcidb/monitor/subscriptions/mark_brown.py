"""Mark Brown's subscription"""

from kcidb.monitor.output import NotificationMessage as Message


def match_revision(revision):
    """Match revisions of interest to Mark Brown"""
    repo_urls = {
        "https://git.kernel.org/pub/scm/linux/kernel/git/arm64/linux.git",
        "https://git.kernel.org/pub/scm/linux/kernel/git/soc/soc.git",
    }
    subject_sfx = ' failed for {% include "revision_summary.txt.j2" %}'
    msg_args = dict(
        to=["Mark Brown <broonie@kernel.org>"],
        body='{% include "revision_description.txt.j2" %}',
    )
    if revision.checkouts_valid and revision.builds_valid is not None and \
       repo_urls & set(revision.repo_branch_checkouts):
        if not revision.builds_valid:
            return (Message(subject='Builds' + subject_sfx, **msg_args),)
        if revision.tests_root.waived is False and \
           revision.tests_root.status not in \
           (None, "PASS", "DONE", "SKIP"):
            return (Message(subject='Tests' + subject_sfx, **msg_args),)
    return ()
