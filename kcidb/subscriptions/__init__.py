"""Kernel CI report subscriptions"""

import pkgutil
import importlib
from kcidb import io, oo
from kcidb.misc import Notification


def _load_match_map():
    """
    Load subscription match functions from kcidb.subscriptions.* modules.

    Returns:
        A dictionary of object list names and a list of tuples, each
        containing a subscription name (subscription module name) and the
        object type's matching function. The matching function accepts an
        object of corresponding type, and returns an iterable of
        kcidb.misc.NotificationMessage objects, or None, which is equivalent
        to an empty iterable.
    """
    match_map = {
        obj_list_name: []
        for obj_list_name in io.schema.LATEST.tree
        if obj_list_name
    }

    # For each kcidb.subscriptions.* module
    for _, module_name, _ in pkgutil.walk_packages(path=__path__):
        module = importlib.import_module(__name__ + "." + module_name)
        # For each object list name
        for obj_list_name in match_map:
            # Record the subscription (module) name and match function, if any
            assert obj_list_name.endswith("s")
            function_name = "match_" + obj_list_name[:-1]
            if function_name in module.__dict__:
                function = module.__dict__[function_name]
                assert callable(function)
                match_map[obj_list_name].append((module_name, function))

    return match_map


# The default map of subscription matching functions:
# a dictionary of object list names and a list of tuples, each containing a
# subscription name (subscription module name) and the object type's matching
# function. The matching function must accept an object of corresponding type,
# and return an iterable of kcidb.misc.NotificationMessage objects, or None,
# which is equivalent to an empty iterable.
MATCH_MAP = _load_match_map()


def match_oo(oo_data, match_map=None):
    """
    Generate notifications for any subscriptions matching OO data.

    Args:
        oo_data:    The object-oriented (OO) representation of report data to
                    generate notifications for.
        match_map:  The map of subscription match functions: a dictionary with
                    OO data object list names and a list of tuples, each
                    containing the name of the subscription and a match
                    function.

                    Each function must accept an object from the corresponding
                    object list in OO data, and return an iterable producing
                    kcidb.misc.NotificationMessage objects, or None, which is
                    equivalent to an empty iterable.

                    The default is a dictionary of matching functions from all
                    kcidb.subscriptions.* modules, where each is called
                    "match_<OBJ_NAME>", where "<OBJ_NAME>" is an object list
                    name without the "s" ending.

    Returns:
        The list of notifications: kcidb.misc.Notification objects.
    """
    assert oo.is_valid(oo_data)

    if match_map is None:
        match_map = MATCH_MAP
    notifications = []
    # For each object list and its subscription function list
    for obj_list_name, subscription_function_list in match_map.items():
        assert isinstance(obj_list_name, str)
        assert obj_list_name
        assert obj_list_name in io.schema.LATEST.tree
        assert isinstance(subscription_function_list, list)
        # For each subscription's name and its match function
        for subscription, function in subscription_function_list:
            assert isinstance(subscription, str)
            assert callable(function)
            # For each object in the OO data object list
            for obj in oo_data.get(obj_list_name, {}).values():
                # For each matched notification message, if any
                for message in function(obj) or []:
                    notifications.append(
                        Notification(obj_list_name, obj, subscription, message)
                    )
    return notifications


def match_new_io(base_io, new_io, match_map=None, copy=True):
    """
    Generate notifications for new I/O data being added to base I/O data.

    Args:
        base_io:    The existing (base) I/O data being added to, and possibly
                    referred to by the arriving (new) I/O data. Will be used
                    to complete the data being notified about. Can already
                    contain the new I/O data, it will be considered "new"
                    regardless.
        new_io:     The arriving (new) I/O data being added to the existing
                    (base) data. Can refer to the existing I/O data. The new
                    I/O data can already be added to the base I/O data,
                    anything in the new data will be considered "new"
                    regardless.
        match_map:  The map of subscription match functions: a dictionary with
                    OO data object list names and a list of tuples, each
                    containing the name of the subscription and a match
                    function.

                    Each function must accept an object from the corresponding
                    object list in OO data, and return an iterable producing
                    kcidb.misc.NotificationMessage objects, or None, which is
                    equivalent to an empty iterable.

                    The default is a dictionary of matching functions from all
                    kcidb.subscriptions.* modules, where each is called
                    "match_<OBJ_NAME>", where "<OBJ_NAME>" is an object list
                    name without the "s" ending.
        copy:       True, if the data should be copied before
                    referencing/modifying. False, if the data could be
                    referenced and modified in-place.
                    Optional, default is True.

    Returns:
        The list of notifications: kcidb.misc.Notification objects.
    """
    assert io.schema.is_valid(base_io)
    assert io.schema.is_valid(new_io)

    # Merge the new data into the base (*copy* new data as we'll need it)
    merged_io = io.merge(base_io, new_io, copy_target=copy, copy_source=True)
    # Convert both to OO representation
    merged_oo = oo.from_io(merged_io, copy=False)
    new_oo = oo.from_io(new_io, copy=copy)
    # Remove all objects with missing parents from the merged data
    rooted_oo = oo.remove_orphans(merged_oo)
    # Delist everything except loaded or modified objects, but keep references
    masked_oo = oo.apply_mask(rooted_oo, new_oo)
    # Generate notifications
    return match_oo(masked_oo, match_map)
