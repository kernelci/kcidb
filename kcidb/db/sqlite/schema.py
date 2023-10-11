"""
Kernel CI SQLite report database miscellaneous schema definitions.
"""
import json
import dateutil.parser
from kcidb.db.sql.schema import Constraint, Column, Table as _SQLTable


class BoolColumn(Column):
    """A boolean column"""

    @staticmethod
    def unpack(value):
        """
        Unpack the SQLite representation of the column value into the JSON
        representation.
        """
        return bool(value) if value is not None else None

    def __init__(self, constraint=None,
                 conflict_func=None, metadata_expr=None):
        """
        Initialize the column description.

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
        super().__init__("INT", constraint=constraint,
                         conflict_func=conflict_func,
                         metadata_expr=metadata_expr)


class TextColumn(Column):
    """A text column"""

    def __init__(self, constraint=None,
                 conflict_func=None, metadata_expr=None):
        """
        Initialize the column description.

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


class IntegerColumn(Column):
    """An integer column"""

    def __init__(self, constraint=None,
                 conflict_func=None, metadata_expr=None):
        """
        Initialize the column description.

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

    def __init__(self, constraint=None,
                 conflict_func=None, metadata_expr=None):
        """
        Initialize the column description.

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

        super().__init__(constraint=constraint,
                         conflict_func=conflict_func,
                         metadata_expr=metadata_expr)


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

    def __init__(self, constraint=None,
                 conflict_func=None, metadata_expr=None):
        """
        Initialize the column description.

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

        super().__init__(constraint=constraint,
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
        # TODO: Switch to using "_" key_sep, and hardcoding it in base class
        super().__init__("?", columns, primary_key, key_sep=".")

    def format_create(self, name):
        """
        Format the "CREATE" command for the table.

        Args:
            name:       The name of the target table of the command.

        Returns:
            The formatted "CREATE" command.
        """
        return super().format_create(name) + " WITHOUT ROWID"
