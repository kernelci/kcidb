"""Kernel CI reporting - monitor - subscriptions"""

import pkgutil
import importlib
import kcidb_io as io

__all__ = [
    "MATCH_MAP"
]


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


# A map of subscription matching functions:
# a dictionary of object list names and a list of tuples, each containing a
# subscription name (subscription module name) and the object type's matching
# function. The matching function must accept an object of corresponding type,
# and return an iterable of kcidb.misc.NotificationMessage objects, or None,
# which is equivalent to an empty iterable.
MATCH_MAP = _load_match_map()
