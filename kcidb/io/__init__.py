"""Kernel CI reporting I/O data"""

from copy import deepcopy
from kcidb.io import schema

# Silence flake8 "imported but unused" warning
__all__ = ["schema", "new", "merge"]


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


def merge(data_a, data_b, copy=True):
    """
    Merge two I/O data sets.

    Args:
        data_a: First dataset to merge.
        data_b: Second dataset to merge.
        copy:   True if both "data_a" and "data_b" contents are copied before
                upgrading and merging. False if neither are copied, both are
                upgraded in place, if necessary, and "data_b" objects are
                simply grafted into "data_a". Default is True.

    Returns:
        The merged data, adhering to the latest schema version.
    """
    assert schema.is_valid(data_a)
    assert schema.is_valid(data_b)

    if copy:
        data_a = deepcopy(data_a)
        data_b = deepcopy(data_b)

    data_a = schema.upgrade(data_a, copy=False)
    data_b = schema.upgrade(data_b, copy=False)

    for obj_list_name in schema.LATEST.tree:
        if obj_list_name:
            data_a[obj_list_name] = \
                data_a.get(obj_list_name, []) + data_b.get(obj_list_name, [])

    assert schema.is_valid_latest(data_a)
    return data_a
