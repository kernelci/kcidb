"""
Kernel CI PostgreSQL report database - misc schema definitions
"""
import json
from kcidb.db.sql.schema import Constraint, Column, \
    Table as _SQLTable, View as _SQLView, Index as _SQLIndex


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


class TextArrayColumn(Column):
    """A text array column schema"""

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
        super().__init__("TEXT[]", constraint=constraint,
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


class View(_SQLView):
    """A view 'schema'"""

    def __init__(self, select, refresh_period=0):
        """
        Initialize the view 'schema'.

        Args:
            select:         The select statement of the view.
            refresh_period: The materialized view's refresh period, integer
                            number of 0 < minutes <= 60.
                            Zero for a regular view.
        """
        assert isinstance(select, str)
        assert isinstance(refresh_period, int)
        assert 0 <= refresh_period <= 60
        super().__init__(select, refresh_period)

    def format_create(self, name):
        """
        Format the creation command for the view.

        Args:
            name:       The name to give the view.

        Returns:
            The command creating the view.
        """
        if self.refresh_period:
            return (
                f"CREATE MATERIALIZED VIEW IF NOT EXISTS {name} AS\n"
                f"{self.select}\n"
                f"WITH DATA"
            )
        return (
            f"CREATE OR REPLACE VIEW {name} AS\n"
            f"{self.select}\n"
        )

    def format_setup(self, name):
        """
        Format the view maintenance setup commands.

        Args:
            name:       The name of the view to be maintained.

        Returns:
            The tuple of commands setting up the view maintenance.
        """
        if self.refresh_period:
            schedule = '0 * * * *' if self.refresh_period == 60 \
                else f'*/{self.refresh_period} * * * *'
            return (
                f"SELECT * FROM dblink(\n"
                f"    'dbname=postgres user=' || current_user,\n"
                f"    $$\n"
                f"        SELECT cron.schedule_in_database(\n"
                f"            '$$ || current_database() || "
                f"$$_refresh_{name}',\n"
                f"            '{schedule}',\n"
                f"            'REFRESH MATERIALIZED VIEW CONCURRENTLY "
                f"{name}',\n"
                f"            '$$ || current_database() || $$'\n"
                f"        ) AS id\n"
                f"    $$\n"
                f") AS result(id BIGINT)",
            )
        return tuple()

    def format_refresh(self, name):
        """
        Format the view refresh commands.

        Args:
            name:       The name of the view to refresh

        Returns:
            The command refreshing the view.
        """
        if self.refresh_period:
            return (f"REFRESH MATERIALIZED VIEW CONCURRENTLY {name}",)
        return tuple()

    def format_teardown(self, name):
        """
        Format the view maintenance teardown commands.

        Args:
            name:       The name of the view to stop maintenance of.

        Returns:
            The command tearing down the view maintenance.
        """
        if self.refresh_period:
            return (
                f"SELECT * FROM dblink(\n"
                f"    'dbname=postgres user=' || current_user,\n"
                f"    $$\n"
                f"        SELECT cron.unschedule(\n"
                f"            '$$ || current_database() || "
                f"$$_refresh_{name}'\n"
                f"        ) AS success\n"
                f"    $$\n"
                f") AS result(success BOOL)",
            )
        return tuple()

    def format_drop(self, name):
        """
        Format the drop command for the view.

        Args:
            name:       The name of the view being dropped.

        Returns:
            The command dropping the view.
        """
        return (
            f"DROP {['', 'MATERIALIZED '][self.refresh_period > 0]}VIEW "
            f"{name}"
        )


class Index(_SQLIndex):
    """An index schema"""
    def __init__(self, table, columns, method=None, unique=False):
        """
        Initialize the index schema.

        Args:
            table:      The name of the table this index belongs to.
            columns:    The list of names of table columns belonging to the
                        index. The names consist of dot-separated parts, same
                        as used for the Table creation parameters.
            method:     The index method string, if different from default.
                        None, if the default should be used.
            unique:     True if the index is unique, False otherwise.
        """
        assert isinstance(table, str)
        assert isinstance(columns, list)
        assert all(isinstance(c, str) for c in columns)
        assert method is None or isinstance(method, str) and method
        assert isinstance(unique, bool)
        # TODO: Switch to hardcoding "_" key_sep in base class
        super().__init__(table, columns, key_sep="_", unique=unique)
        self.method = method

    def format_create(self, name):
        """
        Format the "CREATE INDEX" command for the table.

        Args:
            name:       The name of the target index of the command.

        Returns:
            The formatted "CREATE INDEX" command.
        """
        method = "" if self.method is None else f" USING {self.method}"
        return (
            f"CREATE {['', 'UNIQUE '][self.unique]}INDEX "
            f"IF NOT EXISTS {name} ON {self.table}{method} (" +
            ", ".join(self.columns.values()) +
            ")"
        )
