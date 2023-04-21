"""Kernel CI reporting - monitor - misc definitions."""


def is_valid_firestore_id(value):
    """
    Validate a value as a valid Google Firestore ID.

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
