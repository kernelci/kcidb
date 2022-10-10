"""Test subscription"""

from kcidb.monitor.output import NotificationMessage as Message


def generate_message(obj):
    """Generate a test notification message for an object."""
    name = obj.get_type().name
    return Message(
        to=["test@kernelci.org"],
        subject=f'Test {name}: '
        f'{{% include "{name}_summary.txt.j2" %}}',
        body=f'Test {name} detected!\n\n'
        f'{{% include "{name}_description.txt.j2" %}}',
        id=name
    )


def match_revision(revision):
    """Match test revisions"""
    for checkout in revision.checkouts:
        if checkout.origin == "test":
            return (generate_message(revision),)
    return ()


def match_checkout(checkout):
    """Match test checkouts"""
    if checkout.origin == "test":
        return (generate_message(checkout),)
    return ()


def match_build(build):
    """Match test builds"""
    if build.origin == "test":
        return (generate_message(build),)
    return ()


def match_test(test):
    """Match test tests"""
    if test.origin == "test":
        return (generate_message(test),)
    return ()


def match_bug(bug):
    """Match bugs"""
    for issue in bug.issues:
        if issue.origin == "test":
            return (generate_message(bug),)
    return ()


def match_issue(issue):
    """Match issues"""
    if issue.origin == "test":
        return (generate_message(issue),)
    return ()


def match_incident(incident):
    """Match incidents"""
    if incident.origin == "test":
        return (generate_message(incident),)
    return ()
