"""
Kernel CI PostgreSQL report database - misc schema definitions
"""
import json
from kcidb.db.sql.schema import Constraint, Column, Table as _SQLTable


class BoolColumn(Column):
    """A boolean column schema"""

    def __init__(self, constraint=None,
                 conflict_func=None, metadata_expr=None):
        """
        Initialize the column schema.

        Args:
            constraint:     The column's constraint.
                            A member of the Constraint enum, or None,
                            meaning no constraint.
            conflict_func:  The (non-empty) string containing the name of the
                            SQL function to use to resolve insertion conflicts
                            for this column. None to resolve
                            non-deterministically.
            metadata_expr:  A (non-empty) SQL expression string to use as the
                            value for this (metadata) column, if not supplied
                            explicitly. None to consider this a normal column.
        """
        assert constraint is None or isinstance(constraint, Constraint)
        super().__init__("BOOLEAN", constraint=constraint,
                         conflict_func=conflict_func,
                         metadata_expr=metadata_expr)


class VarcharColumn(Column):
    """A character varying column schema"""

    def __init__(self, length, constraint=None,
                 conflict_func=None, metadata_expr=None):
        """
        Initialize the column schema.

        Args:
            length:         The maximum length of the strings that could be
                            stored in the column, in characters.
            constraint:     The column's constraint.
                            A member of the Constraint enum, or None,
                            meaning no constraint.
            conflict_func:  The (non-empty) string containing the name of the
                            SQL function to use to resolve insertion conflicts
                            for this column. None to resolve
                            non-deterministically.
            metadata_expr:  A (non-empty) SQL expression string to use as the
                            value for this (metadata) column, if not supplied
                            explicitly. None to consider this a normal column.
        """
        assert isinstance(length, int)
        assert length > 0
        assert constraint is None or isinstance(constraint, Constraint)
        super().__init__(f"CHARACTER VARYING ({length})",
                         constraint=constraint,
                         conflict_func=conflict_func,
                         metadata_expr=metadata_expr)


class TextColumn(Column):
    """A text column schema"""

    def __init__(self, constraint=None,
                 conflict_func=None, metadata_expr=None):
        """
        Initialize the column schema.

        Args:
            constraint:     The column's constraint.
                            A member of the Constraint enum, or None,
                            meaning no constraint.
            conflict_func:  The (non-empty) string containing the name of the
                            SQL function to use to resolve insertion conflicts
                            for this column. None to resolve
                            non-deterministically.
            metadata_expr:  A (non-empty) SQL expression string to use as the
                            value for this (metadata) column, if not supplied
                            explicitly. None to consider this a normal column.
        """
        assert constraint is None or isinstance(constraint, Constraint)
        super().__init__("TEXT", constraint=constraint,
                         conflict_func=conflict_func,
                         metadata_expr=metadata_expr)


class JSONColumn(Column):
    """A JSON column schema"""

    @staticmethod
    def pack(value):
        """
        Pack the JSON representation of the column value into the PostgreSQL
        representation.
        """
        return json.dumps(value)

    def __init__(self, constraint=None,
                 conflict_func=None, metadata_expr=None):
        """
        Initialize the column schema.

        Args:
            constraint:     The column's constraint.
                            A member of the Constraint enum, or None,
                            meaning no constraint.
            conflict_func:  The (non-empty) string containing the name of the
                            SQL function to use to resolve insertion conflicts
                            for this column. None to resolve
                            non-deterministically.
            metadata_expr:  A (non-empty) SQL expression string to use as the
                            value for this (metadata) column, if not supplied
                            explicitly. None to consider this a normal column.
        """
        assert constraint is None or isinstance(constraint, Constraint)
        super().__init__("JSONB", constraint=constraint,
                         conflict_func=conflict_func,
                         metadata_expr=metadata_expr)


class TimestampColumn(Column):
    """A timestamp column schema"""

    @staticmethod
    def unpack(value):
        """
        Unpack the PostgreSQL representation of the column value into the JSON
        representation.
        """
        return value.isoformat(timespec='microseconds')

    def __init__(self, constraint=None,
                 conflict_func=None, metadata_expr=None):
        """
        Initialize the column schema.

        Args:
            constraint:     The column's constraint.
                            A member of the Constraint enum, or None,
                            meaning no constraint.
            conflict_func:  The (non-empty) string containing the name of the
                            SQL function to use to resolve insertion conflicts
                            for this column. None to resolve
                            non-deterministically.
            metadata_expr:  A (non-empty) SQL expression string to use as the
                            value for this (metadata) column, if not supplied
                            explicitly. None to consider this a normal column.
        """
        assert constraint is None or isinstance(constraint, Constraint)
        super().__init__("TIMESTAMP WITH TIME ZONE", constraint=constraint,
                         conflict_func=conflict_func,
                         metadata_expr=metadata_expr)


class IntegerColumn(Column):
    """An integer number column schema"""

    def __init__(self, constraint=None,
                 conflict_func=None, metadata_expr=None):
        """
        Initialize the column schema.

        Args:
            constraint:     The column's constraint.
                            A member of the Constraint enum, or None,
                            meaning no constraint.
            conflict_func:  The (non-empty) string containing the name of the
                            SQL function to use to resolve insertion conflicts
                            for this column. None to resolve
                            non-deterministically.
            metadata_expr:  A (non-empty) SQL expression string to use as the
                            value for this (metadata) column, if not supplied
                            explicitly. None to consider this a normal column.
        """
        assert constraint is None or isinstance(constraint, Constraint)
        super().__init__("INTEGER", constraint=constraint,
                         conflict_func=conflict_func,
                         metadata_expr=metadata_expr)


class FloatColumn(Column):
    """A floating-point number column schema"""

    def __init__(self, constraint=None,
                 conflict_func=None, metadata_expr=None):
        """
        Initialize the column schema.

        Args:
            constraint:     The column's constraint.
                            A member of the Constraint enum, or None,
                            meaning no constraint.
            conflict_func:  The (non-empty) string containing the name of the
                            SQL function to use to resolve insertion conflicts
                            for this column. None to resolve
                            non-deterministically.
            metadata_expr:  A (non-empty) SQL expression string to use as the
                            value for this (metadata) column, if not supplied
                            explicitly. None to consider this a normal column.
        """
        assert constraint is None or isinstance(constraint, Constraint)
        super().__init__("DOUBLE PRECISION", constraint=constraint,
                         conflict_func=conflict_func,
                         metadata_expr=metadata_expr)


class Table(_SQLTable):
    """A table schema"""
    def __init__(self, columns, primary_key=None):
        """
        Initialize the table schema.

        Args:
            columns:        A dictionary of column names consisting of
                            dot-separated parts (keys), and the column
                            schemas. Columns cannot specify PRIMARY_KEY
                            constraint, if primary_key_columns is specified.
            primary_key:    A list of names of columns constituting the
                            primary key. None or an empty list to use the
                            column with the PRIMARY_KEY constraint instead.
        """
        # TODO: Switch to hardcoding "_" key_sep in base class
        super().__init__("%s", columns, primary_key, key_sep="_")
