"""Kernel CI reporting I/O schema - misc definitions"""

import jsonschema


class Version:
    """A version of the schema"""
    # pylint: disable=too-many-arguments
    def __init__(self, major, minor, json, previous=None, inherit=None):
        """
        Initialize the version.

        Args:
            major:      The major version number. A non-negative integer.
                        Increases represent backward-incompatible changes.
                        E.g. deleting or renaming a property, changing a
                        property type, restricting values, making a property
                        required, or adding a new required property.
            minor:      The minor version number. A non-negative integer.
                        Increases represent backward-compatible changes. E.g.
                        relaxing value restrictions, making a property
                        optional, or adding a new optional property.
            json:       The JSON schema for this version.
            previous:   The previous schema version, or None if none.
                        Must have lower major number, if not None.
            inherit:    The data inheritance function. Must accept data
                        adhering to the "previous" version of the schema as
                        the only argument, and return the data adhering to
                        this version. Can modify the argument. Can be None,
                        meaning no transformation needed. Must be None if
                        "previous" is None.
        """
        assert isinstance(major, int) and major >= 0
        assert isinstance(minor, int) and minor >= 0
        assert json is not None
        assert previous is None or \
            isinstance(previous, Version) and (major > previous.major)
        assert inherit is None or previous is not None and callable(inherit)

        self.major = major
        self.minor = minor
        self.previous = previous
        self.json = json
        self.inherit = inherit

    def validate(self, data):
        """
        Validate the data against this schema version.

        Args:
            data:   The data to validate. Will not be changed.

        Returns:
            The validated (but unchanged) data.

        Raises:
            `jsonschema.exceptions.ValidationError` if the data did not adhere
            to this version of the schema.
        """
        jsonschema.validate(instance=data, schema=self.json)
        return data

    def is_valid(self, data):
        """
        Check if data is valid according to this schema version.

        Args:
            data:   The data to check against the schema.

        Returns:
            True if the data is valid, false otherwise.
        """
        try:
            self.validate(data)
        except jsonschema.exceptions.ValidationError:
            return False
        return True

    def upgrade(self, data):
        """
        Upgrade the data to this version from any of the previous schema
        versions.

        Args:
            data:   The data to upgrade. Must adhere to this version or any of
                    the previous versions.

        Returns:
            The upgraded data.
        """
        # Check for "previous" outside except block to avoid re-raising
        if self.previous:
            try:
                self.validate(data)
            except jsonschema.exceptions.ValidationError:
                data = self.previous.upgrade(data)
                if self.inherit:
                    data = self.inherit(data)
                assert self.is_valid(data)
        else:
            self.validate(data)
        return data
