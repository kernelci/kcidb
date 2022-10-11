"""
Kernel CI SQLite report database miscellaneous schema definitions.
"""
import json
from enum import Enum
import dateutil.parser


class Constraint(Enum):
    """A column's constraint"""
    PRIMARY_KEY = "PRIMARY KEY"
    NOT_NULL = "NOT NULL"


class Column:
    """A column description"""

    @staticmethod
    def pack(value):
        """
        Pack the JSON representation of the column value into the SQLite
        representation.
        """
        return value

    @staticmethod
    def unpack(value):
        """
        Unpack the SQLite representation of the column value into the JSON
        representation.
        """
        return value

    def __init__(self, type, constraint=None):
        """
        Initialize the column description.

        Args:
            type:           The name of the SQLite type (affinity) to use for
                            this column.
            constraint:     The column's constraint.
                            A member of the Constraint enum, or None,
                            meaning no constraint.
        """
        assert isinstance(type, str)
        assert constraint is None or isinstance(constraint, Constraint)
        self.type = type
        self.constraint = constraint

    def format_nameless_def(self):
        """
        Format the "column-def" part of SQLite's "CREATE TABLE" statement
        (https://sqlite.org/syntax/column-def.html) without the "column-name"
        part.

        Returns: The formatted (nameless) "column-def".
        """
        nameless_def = self.type
        if self.constraint:
            nameless_def += " " + self.constraint.value
        return nameless_def


class BoolColumn(Column):
    """A boolean column"""

    @staticmethod
    def unpack(value):
        """
        Unpack the SQLite representation of the column value into the JSON
        representation.
        """
        return bool(value) if value is not None else None

    def __init__(self, constraint=None):
        """
        Initialize the column description.

        Args:
            constraint:     The column's constraint.
                            A member of the Constraint enum, or None,
                            meaning no constraint.
        """
        assert constraint is None or isinstance(constraint, Constraint)
        super().__init__("INT", constraint=constraint)


class TextColumn(Column):
    """A text column"""

    def __init__(self, constraint=None):
        """
        Initialize the column description.

        Args:
            constraint:     The column's constraint.
                            A member of the Constraint enum, or None,
                            meaning no constraint.
        """
        assert constraint is None or isinstance(constraint, Constraint)
        super().__init__("TEXT", constraint=constraint)


class IntegerColumn(Column):
    """An integer column"""

    def __init__(self, constraint=None):
        """
        Initialize the column description.

        Args:
            constraint:     The column's constraint.
                            A member of the Constraint enum, or None,
                            meaning no constraint.
        """
        assert constraint is None or isinstance(constraint, Constraint)
        super().__init__("INTEGER", constraint=constraint)


class JSONColumn(TextColumn):
    """A JSON-encoded column"""

    @staticmethod
    def pack(value):
        """
        Pack the JSON representation of the column value into the SQLite
        representation.
        """
        return json.dumps(value) if value is not None else None

    @staticmethod
    def unpack(value):
        """
        Unpack the SQLite representation of the column value into the JSON
        representation.
        """
        return json.loads(value) if value is not None else None

    def __init__(self, constraint=None):
        """
        Initialize the column description.

        Args:
            constraint:     The column's constraint.
                            A member of the Constraint enum, or None,
                            meaning no constraint.
        """
        assert constraint is None or isinstance(constraint, Constraint)

        super().__init__(constraint=constraint)


class TimestampColumn(TextColumn):
    """A normalized timestamp column"""

    @staticmethod
    def pack(value):
        """
        Pack the JSON representation of the column value into the SQLite
        representation.
        """
        return dateutil.parser.isoparse(value).isoformat(
            timespec='microseconds'
        )

    def __init__(self, constraint=None):
        """
        Initialize the column description.

        Args:
            constraint:     The column's constraint.
                            A member of the Constraint enum, or None,
                            meaning no constraint.
        """
        assert constraint is None or isinstance(constraint, Constraint)

        super().__init__(constraint=constraint)
