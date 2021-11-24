"""Kernel CI reporting - monitor"""

# Silence flake8 "imported but unused" warning
from kcidb.monitor import (subscriptions, output, spool, misc) # noqa


def match(oo_data, match_map=None):
    """
    Generate notifications for subscriptions matching supplied OO data.

    Args:
        oo_data:    A dictionary of object type names and lists containing
                    objects of the corresponding type. E.g. as returned by
                    kcidb.oo.Client.query().
        match_map:  The map of subscription match functions: a dictionary with
                    OO data object type names and a list of tuples, each
                    containing the name of the subscription and a match
                    function.

                    Each function must accept an object from the corresponding
                    object list in OO data, and return an iterable producing
                    kcidb.monitor.output.NotificationMessage objects, or None,
                    which is equivalent to an empty iterable.

                    The default is a dictionary of matching functions from all
                    kcidb.monitor.subscriptions.* modules, where each is
                    called "match_<OBJ_NAME>", where "<OBJ_NAME>" is an object
                    list name without the "s" ending.
    """
    assert isinstance(oo_data, dict)
    if match_map is None:
        match_map = subscriptions.MATCH_MAP
    assert isinstance(match_map, dict)
    assert set(oo_data) <= set(match_map)
    notifications = []
    # For each object type and its subscription function list
    for obj_type_name, subscription_function_list in match_map.items():
        assert isinstance(obj_type_name, str)
        assert obj_type_name
        assert isinstance(subscription_function_list, list)
        # For each subscription's name and its match function
        for subscription, function in subscription_function_list:
            assert isinstance(subscription, str)
            assert callable(function)
            # For each object in the OO data object list
            for obj in oo_data.get(obj_type_name, []):
                # For each matched notification message, if any
                for message in function(obj) or []:
                    assert isinstance(message, output.NotificationMessage)
                    notifications.append(
                        output.Notification(obj, subscription, message)
                    )
    return notifications
