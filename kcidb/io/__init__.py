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


def get_obj_num(data):
    """
    Calculate number of objects of any type in an I/O data set adhering to the
    latest schema.

    Args:
        data:   The data set to count the objects in.
                Must adhere to the latest schema.

    Returns:
        The number of objects in the data set.
    """
    assert schema.is_valid_latest(data)
    return sum(len(data[k]) for k in schema.LATEST.tree if k and k in data)


def merge(target, sources, copy_target=True, copy_sources=True):
    """
    Merge multiple I/O data into a destination.

    Args:
        target:         The data to merge into.
        sources:        An iterable containing data sets to merge from.
        copy_target:    True if "target" contents should be copied before
                        upgrading and modifying. False if not.
                        Default is True.
        copy_sources:   True if "source" contents should be copied before
                        upgrading and referencing. False if not.
                        Default is True.

    Returns:
        The merged data, adhering to the latest schema version.
    """
    assert schema.is_valid(target)

    if copy_target:
        target = deepcopy(target)
    target = schema.upgrade(target, copy=False)

    for source in sources:
        assert schema.is_valid(source)
        if copy_sources:
            source = deepcopy(source)
        source = schema.upgrade(source, copy=False)
        for obj_list_name in schema.LATEST.tree:
            if obj_list_name:
                target[obj_list_name] = \
                    target.get(obj_list_name, []) + \
                    source.get(obj_list_name, [])

    assert schema.is_valid_latest(target)
    return target
