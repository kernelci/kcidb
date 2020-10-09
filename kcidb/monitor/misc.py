"""Kernel CI reporting - monitor - misc definitions"""


def is_valid_firestore_id(value):
    """
    Check if a value is valid for use as a Google Firestore
    collection or document ID, according to
    https://firebase.google.com/docs/firestore/quotas

    Args:
        value: The value to check.

    Returns:
        True if the value is a valid Firestore document/collection ID,
        False if not.
    """
    try:
        return isinstance(value, str) and \
               len(value.encode()) <= 1500 and \
               value != "." and \
               value != ".." and \
               "/" not in value and \
               not (value.startswith("__") and value.endswith("__"))
    except UnicodeError:
        return False
