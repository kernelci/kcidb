"""Kernel CI reporting database - misc definitions"""


class Error(Exception):
    """An abstract error"""


class NotFound(Error):
    """A database doesn't exist"""

    def __init__(self, database):
        """Initialize the exception."""
        assert isinstance(database, str)
        super().__init__(f"Database {database!r} not found")


class UnsupportedSchema(Error):
    """Database schema version is not supported"""

    def __init__(self, major, minor):
        """
        Initialize the exception.

        Args:
            major:  Database schema major version number
            minor:  Database schema minor version number
        """
        super().__init__(f"Database schema v{major}.{minor} is unsupported")
