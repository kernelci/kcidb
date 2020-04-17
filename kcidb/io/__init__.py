"""Kernel CI reporting I/O data"""

from kcidb.io import schema

# Silence flake8 "imported but unused" warning
__all__ = ["schema", "new"]


def new():
    """
    Create an empty I/O data set.

    Returns:
        An empty I/O data set adhering to the latest schema version.
    """
    data = dict(version=dict(major=schema.LATEST.major,
                             minor=schema.LATEST.minor))
    assert schema.is_valid_latest(data)
    return data
