"""Test subscription"""

from kcidb.misc import NotificationMessage as Message


def match_revision(revision):
    """Match test revisions"""
    if revision.id.startswith("test:"):
        return (
            Message(["test@kernelci.org"],
                    "Test revision: ",
                    "Test revision detected!\n\n",
                    "revision"),
        )
    return ()


def match_build(build):
    """Match test builds"""
    if build.id.startswith("test:"):
        return (
            Message(["test@kernelci.org"],
                    "Test build: ",
                    "Test build detected!\n\n",
                    "build"),
        )
    return ()


def match_test(test):
    """Match test tests"""
    if test.id.startswith("test:"):
        return (
            Message(["test@kernelci.org"],
                    "Test test: ",
                    "Test test detected!\n\n",
                    "test"),
        )
    return ()
