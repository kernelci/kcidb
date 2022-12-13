"""
Kernel CI report database - generic SQL schema definitions
"""

import re
from enum import Enum


class Constraint(Enum):
    """A column's constraint"""
    PRIMARY_KEY = "PRIMARY KEY"
    NOT_NULL = "NOT NULL"


class Column:
    """A column schema"""

    @staticmethod
    def pack(value):
        """
        Pack the JSON representation of the column value into the database
        representation.
        """
        return value

    @staticmethod
    def unpack(value):
        """
        Unpack the database representation of the column value into the JSON
        representation.
        """
        return value

    def __init__(self, type, constraint=None):
        """
        Initialize the column schema.

        Args:
            type:           The name of the database type to use
                            for this column.
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
        Format a column definition without the name.

        Returns: The formatted column definition.
        """
        nameless_def = self.type
        if self.constraint:
            nameless_def += " " + self.constraint.value
        return nameless_def


class TableColumn:
    """A column within a table schema"""

    # Regular expression fully-matching column names, which don't need quoting
    NAME_RE = re.compile("[a-zA-Z_][a-zA-Z0-9_]*")

    @classmethod
    def quote_name(cls, name):
        """
        Quote the name of a column for safe use within SQL statements.

        Args:
            name:   The name to quote.

        Returns: The quoted name.
        """
        if name:
            if cls.NAME_RE.fullmatch(name):
                return name
            return re.sub(r'(^|$|")', r'"\1', name)
        return '""'

    def __init__(self, name, schema, key_sep):
        """
        Initialize the table schema column.

        Args:
            name:       The name of the column consisting of dot-separated
                        parts ("keys").
            schema:     The column's schema (an instance of Column).
            key_sep:    String used to replace dots in column names ("key"
                        separator)
        """
        assert isinstance(name, str)
        assert isinstance(schema, Column)
        assert isinstance(key_sep, str)
        # Name parts (keys)
        self.keys = name.split(".")
        # Column name within the table
        self.name = self.quote_name(key_sep.join(self.keys))
        # Column schema
        self.schema = schema


class Table:
    """A table schema"""

    def __init__(self, placeholder, columns, primary_key=None, key_sep="_"):
        """
        Initialize the table schema.

        Args:
            placeholder:    The query parameter placeholder string to use in
                            queries.
            columns:        A dictionary of column names consisting of
                            dot-separated parts (keys), and the column
                            schemas. Columns cannot specify PRIMARY_KEY
                            constraint, if primary_key is specified.
            primary_key:    A list of names of columns constituting the
                            primary key. None to use the column with the
                            PRIMARY_KEY constraint instead.
            key_sep:        String used to replace dots in column names ("key"
                            separator)
        """
        assert isinstance(placeholder, str) and str
        assert isinstance(columns, dict)
        assert all(
            isinstance(name, str) and
            isinstance(column, Column)
            for name, column in columns.items()
        )
        # A map of columns with PRIMARY_KEY constraint set
        primary_key_columns = {
            name: column
            for name, column in columns.items()
            if column.constraint == Constraint.PRIMARY_KEY
        }
        assert \
            len(primary_key_columns) <= 1 \
            if primary_key is None else \
            isinstance(primary_key, list) and \
            all(isinstance(column_name, str) and
                column_name in columns
                for column_name in primary_key) and \
            len(primary_key_columns) == 0
        assert isinstance(key_sep, str)
        # Query parameter placeholder
        self.placeholder = placeholder
        # Column list
        self.columns = [
            TableColumn(name, column, key_sep)
            for name, column in columns.items()
        ]
        # A string of comma-separated column names for use in commands
        self.columns_list = ", ".join(column.name for column in self.columns)
        # A list of columns in the explicitly-specified primary key
        self.primary_key = None if primary_key is None else [
            column
            for column_name in primary_key
            for column in self.columns
            if column.name == column_name
        ]

    def format_create(self, name):
        """
        Format the "CREATE" command for the table.

        Args:
            name:       The name of the target table of the command.

        Returns:
            The formatted "CREATE" command.
        """
        items = [
            column.name + " " + column.schema.format_nameless_def()
            for column in self.columns
        ]
        if self.primary_key:
            items.append(
                "PRIMARY KEY(" +
                ", ".join(column.name for column in self.primary_key) +
                ")"
            )
        return "CREATE TABLE IF NOT EXISTS " + name + \
            " (\n    " + ",\n    ".join(items) + "\n)"

    def format_insert(self, name, prio_db):
        """
        Format the "INSERT/UPDATE" command template for loading a row into a
        database, observing deduplication logic.

        Args:
            name:       The name of the target table of the command.
            prio_db:    If true, format the UPDATE part of the command so that
                        the values already in the database take priority over
                        the loaded ones, and vice versa otherwise.
        Returns:
            The formatted "INSERT/UPDATE" command template, expecting
            parameters packed by the pack() method.
        """
        assert isinstance(name, str)
        return \
            f"INSERT INTO {name} (\n" + \
            ",\n".join(f"    {c.name}" for c in self.columns) + \
            "\n)\nVALUES (\n    " + \
            ", ".join((self.placeholder, ) * len(self.columns)) + \
            "\n)\nON CONFLICT (" + \
            ", ".join(
                c.name for c in self.columns
                if c.schema.constraint == Constraint.PRIMARY_KEY or
                c in (self.primary_key or [])
            ) + ") DO UPDATE SET\n" + \
            ",\n".join(
                f"    {c.name} = COALESCE(" + (
                    f"{name}.{c.name}, excluded.{c.name}"
                    if prio_db else
                    f"excluded.{c.name}, {name}.{c.name}"
                ) + ")"
                for c in self.columns
                if c.schema.constraint != Constraint.PRIMARY_KEY and
                c not in (self.primary_key or [])
            )

    def format_dump(self, name):
        """
        Format the "SELECT" command for dumping the table contents, returning
        data suitable for unpacking with unpack*() methods.

        Args:
            name:   The name of the target table of the command.

        Returns:
            The formatted "SELECT" command.
        """
        assert isinstance(name, str)
        return f"SELECT {self.columns_list} FROM {name}"

    def format_delete(self, name):
        """
        Format the "DELETE" command for emptying the table (removing all
        rows).

        Args:
            name:   The name of the target table of the command.

        Returns:
            The formatted "DELETE" command.
        """
        # Placate older pylint
        assert self
        assert isinstance(name, str)
        return f"DELETE FROM {name}"

    def pack(self, obj):
        """
        Pack a JSON object into its database representation for use with the
        "INSERT" command formatted by the format_insert() method.

        Args:
            obj:    The object to pack.

        Returns:
            The packed object.
        """
        assert isinstance(obj, dict)
        packed_obj = []
        for column in self.columns:
            node = obj
            for i, key in enumerate(column.keys):
                if key in node:
                    if i + 1 == len(column.keys):
                        packed_obj.append(column.schema.pack(node[key]))
                        break
                    node = node[key]
                    assert isinstance(node, dict)
                else:
                    packed_obj.append(None)
                    break
        return packed_obj

    def pack_iter(self, obj_seq):
        """
        Create a generator packing JSON objects from the specified sequence
        into their database representation, for use with the "INSERT"
        command formatted by the format_insert() method.

        Args:
            obj_seq:    The object sequence to create the generator for.

        Returns:
            The generator packing the object sequence.
        """
        for obj in obj_seq:
            yield self.pack(obj)

    def unpack(self, obj, drop_null=True):
        """
        Unpack a database representation of an object into its JSON
        representation.

        Args:
            obj:        The object to unpack.
            drop_null:  Drop fields with NULL values, if true.
                        Keep them otherwise.

        Returns:
            The unpacked object.
        """
        unpacked_obj = {}
        for column, value in zip(self.columns, obj):
            if value is None and drop_null:
                continue
            node = unpacked_obj
            for key in column.keys[:-1]:
                if key not in node:
                    node[key] = {}
                node = node[key]
            node[column.keys[-1]] = \
                None if value is None else column.schema.unpack(value)
        return unpacked_obj

    def unpack_iter(self, obj_seq, drop_null=True):
        """
        Create a generator unpacking database object representations from
        the specified sequence into their JSON representation.

        Args:
            obj_seq:    The object sequence to create the generator for.
            drop_null:  Drop fields with NULL values, if true.
                        Keep them otherwise.

        Returns:
            The generator unpacking the object sequence.
        """
        for obj in obj_seq:
            yield self.unpack(obj, drop_null)
