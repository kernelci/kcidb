"""Kernel CI reporting database - misc definitions"""

import re


class Error(Exception):
    """An abstract error"""


class UnknownDriver(Error):
    """Database driver is unknown"""

    def __init__(self, driver_name):
        """
        Initialize the exception.

        Args:
            driver_name:    Name of the unknown driver
        """
        super().__init__(f"Unknown database driver: {driver_name!r}")


class NotFound(Error):
    """A database doesn't exist"""

    def __init__(self, database):
        """Initialize the exception."""
        assert isinstance(database, str)
        super().__init__(f"Database {database!r} not found")


class UnsupportedSchema(Error):
    """Database schema version is not supported"""


def format_spec_list(specs):
    """
    Format a database specification list string out of a list of specification
    strings.

    Args:
        specs   An iterable of database specification strings to format.

    Returns:
        The formatted specification list string.
    """
    return " ".join(
        re.sub(r"(\\|\s)", r"\\\1", spec)
        for spec in specs
    )


def parse_spec_list(spec_list_str):
    """
    Create a generator parsing and returning database specification strings
    from a (backslash-escaped) whitespace-separated list.

    Args:
        spec_list_str:   The list string to parse.

    Returns:
        The generator returning the parsed database specification strings.
    """
    assert isinstance(spec_list_str, str)

    escape = False
    spec = ""
    for char in spec_list_str:
        if char == "\\":
            escape = True
            continue
        if escape:
            spec += char
            escape = False
            continue
        if char.isspace():
            if spec:
                yield spec
                spec = ""
            continue
        spec += char

    if escape:
        raise Exception(
            f"Incomplete escape sequence at the end of database "
            f"specification list string {spec_list_str!r}"
        )

    if spec:
        yield spec


def instantiate_spec(drivers, spec):
    """
    Create an instance of a driver described in a spec string, picking drivers
    from the supplied dictionary.

    Args:
        drivers:    The dictionary of driver names and types to instantiate
                    drivers with.
        spec:       The database specification to use.

    Returns:
        The created driver instances.

    Raises:
        UnknownDriver       - an unknown (sub-)driver encountered in the
                              (component) specification string
        NotFound            - the database does not exist
        UnsupportedSchema   - the database schema is not supported by the
                              driver
    """
    assert isinstance(drivers, dict)
    assert all(isinstance(k, str) and callable(v)
               for k, v in drivers.items())
    assert isinstance(spec, str)

    spec_parts = spec.split(":", 1)
    name = spec_parts[0]
    params = spec_parts[1] if len(spec_parts) > 1 else None
    if name not in drivers:
        raise UnknownDriver(name)
    return drivers[name](params)


def instantiate_spec_list(drivers, spec_list_str):
    """
    Create a generator returning driver instances created from a database
    specification list string, picking driver types from the supplied
    dictionary.

    Args:
        drivers:        The dictionary of driver names and types to
                        instantiate drivers with.
        spec_list_str:  The database specification list string to parse.

    Returns:
        A generator returning the created driver instances.

    Raises:
        UnknownDriver       - an unknown (sub-)driver encountered in a
                              (component) specification string
        NotFound            - a database does not exist
        UnsupportedSchema   - a database schema is not supported by the
                              driver
    """
    assert isinstance(drivers, dict)
    assert all(isinstance(k, str) and callable(v)
               for k, v in drivers.items())
    assert isinstance(spec_list_str, str)

    for spec in parse_spec_list(spec_list_str):
        yield instantiate_spec(drivers, spec)
