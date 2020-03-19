"""Kernel CI report subscriptions"""

import pkgutil
import importlib
from kcidb.io import schema
from kcidb import oo
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
        for obj_list_name in schema.LATEST.tree
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


# A dictionary of object list names and a list of tuples, each containing a
# subscription name (subscription module name) and the object type's matching
# function. The matching function must accept an object of corresponding type,
# and return an iterable of kcidb.misc.NotificationMessage objects, or None,
# which is equivalent to an empty iterable.
MATCH_MAP = _load_match_map()


def match(oo_data, match_map=None):
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
        assert obj_list_name in schema.LATEST.tree
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
