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


def merge(target, source, copy_target=True, copy_source=True):
    """
    Merge one I/O data into another.

    Args:
        target:         The data to merge into.
        source:         The data to merge from.
        copy_target:    True if "target" contents should be copied before
                        upgrading and modifying. False if not.
                        Default is True.
        copy_source:    True if "source" contents should be copied before
                        upgrading and referencing. False if not.
                        Default is True.

    Returns:
        The merged data, adhering to the latest schema version.
    """
    assert schema.is_valid(target)
    assert schema.is_valid(source)

    # Make sure the data is copied, if requested
    if copy_target:
        target = deepcopy(target)
    if copy_source:
        source = deepcopy(source)

    # Upgrade without copying, even if modifications are needed
    target = schema.upgrade(target, copy=False)
    source = schema.upgrade(source, copy=False)

    for obj_list_name in schema.LATEST.tree:
        if obj_list_name:
            target[obj_list_name] = \
                target.get(obj_list_name, []) + source.get(obj_list_name, [])

    assert schema.is_valid_latest(target)
    return target
