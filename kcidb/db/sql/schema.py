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

    def __init__(self, type, constraint=None,
                 conflict_func=None, metadata_expr=None):
        """
        Initialize the column schema.

        Args:
            type:           The name of the database type to use
                            for this column.
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
        assert isinstance(type, str)
        assert constraint is None or isinstance(constraint, Constraint)
        assert conflict_func is None or \
            isinstance(conflict_func, str) and conflict_func
        assert metadata_expr is None or \
            isinstance(metadata_expr, str) and metadata_expr
        self.type = type
        self.constraint = constraint
        self.conflict_func = conflict_func
        self.metadata_expr = metadata_expr

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

    @classmethod
    def adapt_name(cls, name, key_sep):
        """
        Convert a dot-separated name to a quoted table column name (safe for
        use in SQL statements), and the list of its keys, using the specified
        "key" separator.

        Args:
            name:       The name of the column consisting of dot-separated
                        parts ("keys").
            key_sep:    String used to replace dots in column names ("key"
                        separator)

        Returns: The quoted name, and the list of name "keys".
        """
        assert isinstance(name, str)
        assert isinstance(key_sep, str)
        # Name parts (keys)
        keys = name.split(".")
        # Column name within the table, quoted for use in SQL statements
        return cls.quote_name(key_sep.join(keys)), keys

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
        # Column name within the table, quoted for use in SQL statements,
        # and column name parts (keys)
        self.name, self.keys = self.adapt_name(name, key_sep)
        # Column schema
        self.schema = schema

    def format_def(self):
        """
        Format the column's definition.

        Returns: The formatted column definition.
        """
        return self.name + " " + self.schema.format_nameless_def()


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
                            primary key. None or an empty list to use the
                            column with the PRIMARY_KEY constraint instead.
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
        # The number of columns with PRIMARY_KEY constraint set
        primary_key_constraints = sum(
            c.constraint == Constraint.PRIMARY_KEY for c in columns.values()
        )
        assert primary_key is None or isinstance(primary_key, list)
        if primary_key is None:
            primary_key = []
        assert (
            (
                set(primary_key) <= set(columns) and
                primary_key_constraints == 0
            )
            if primary_key else
            primary_key_constraints <= 1
        )
        assert isinstance(key_sep, str)

        # Query parameter placeholder
        self.placeholder = placeholder
        # Column name -> table column map
        self.columns = {
            name: TableColumn(name, column, key_sep)
            for name, column in columns.items()
        }
        # A list of columns in the explicitly-specified primary key
        self.primary_key = [self.columns[name] for name in primary_key]

    def format_create(self, name):
        """
        Format the "CREATE" command for the table.

        Args:
            name:       The name of the target table of the command.

        Returns:
            The formatted "CREATE" command.
        """
        items = [column.format_def() for column in self.columns.values()]
        if self.primary_key:
            items.append(
                "PRIMARY KEY(" +
                ", ".join(column.name for column in self.primary_key) +
                ")"
            )
        return "CREATE TABLE IF NOT EXISTS " + name + \
            " (\n    " + ",\n    ".join(items) + "\n)"

    def format_insert(self, name, prio_db, with_metadata):
        """
        Format the "INSERT/UPDATE" command template for loading a row into a
        database, observing deduplication logic.

        Args:
            name:           The name of the target table of the command.
            prio_db:        If true, format the UPDATE part of the command so
                            that the values already in the database take
                            priority over the loaded ones, and vice versa
                            otherwise.
            with_metadata:  True, if metadata fields should be inserted too.
                            False, if not.
        Returns:
            The formatted "INSERT/UPDATE" command template, expecting
            parameters packed by the pack() method.
        """
        assert isinstance(name, str)
        assert isinstance(with_metadata, bool)
        return \
            f"INSERT INTO {name} (\n" + \
            ",\n".join(f"    {c.name}" for c in self.columns.values()) + \
            "\n)\nVALUES (\n    " + \
            ", ".join(
                self.placeholder
                if with_metadata or not c.schema.metadata_expr
                else c.schema.metadata_expr
                for c in self.columns.values()
            ) + \
            "\n)\nON CONFLICT (" + \
            ", ".join(
                c.name for c in self.columns.values()
                if c.schema.constraint == Constraint.PRIMARY_KEY or
                c in self.primary_key
            ) + ") DO UPDATE SET\n" + \
            ",\n".join(
                f"    {c.name} = " + (
                    f"{c.schema.conflict_func}("
                    f"COALESCE({name}.{c.name}, excluded.{c.name}), "
                    f"COALESCE(excluded.{c.name}, {name}.{c.name})"
                    f")"
                    if c.schema.conflict_func else
                    (
                        f"COALESCE({name}.{c.name}, excluded.{c.name})"
                        if prio_db else
                        f"COALESCE(excluded.{c.name}, {name}.{c.name})"
                    )
                )
                for c in self.columns.values()
                if c.schema.constraint != Constraint.PRIMARY_KEY and
                c not in self.primary_key
            )

    def format_dump(self, name, with_metadata):
        """
        Format the "SELECT" command for dumping the table contents, returning
        data suitable for unpacking with unpack*() methods.

        Args:
            name:           The name of the target table of the command.
            with_metadata:  True, if metadata fields should be dumped too.
                            False, if not.

        Returns:
            The formatted "SELECT" command.
        """
        assert isinstance(name, str)
        assert isinstance(with_metadata, bool)
        return "SELECT " + \
            ", ".join(
                c.name for c in self.columns.values()
                if with_metadata or not c.schema.metadata_expr
            ) + \
            f" FROM {name}"

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

    def pack(self, obj, with_metadata):
        """
        Pack a JSON object into its database representation for use with the
        "INSERT" command formatted by the format_insert() method.

        Args:
            obj:            The object to pack.
            with_metadata:  True, if any metadata fields in the JSON object
                            should be included into the packed object.
                            False, if not.

        Returns:
            The packed object.
        """
        assert isinstance(obj, dict)
        assert isinstance(with_metadata, bool)
        packed_obj = []
        for column in self.columns.values():
            if not with_metadata and column.schema.metadata_expr:
                continue
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

    def pack_iter(self, obj_seq, with_metadata):
        """
        Create a generator packing JSON objects from the specified sequence
        into their database representation, for use with the "INSERT"
        command formatted by the format_insert() method.

        Args:
            obj_seq:        The object sequence to create the generator for.
            with_metadata:  True, if any metadata fields in the JSON objects
                            should be included into the packed objects.
                            False, if not.

        Returns:
            The generator packing the object sequence.
        """
        assert isinstance(with_metadata, bool)
        for obj in obj_seq:
            yield self.pack(obj, with_metadata)

    def unpack(self, obj, with_metadata, drop_null=True):
        """
        Unpack a database representation of an object into its JSON
        representation.

        Args:
            obj:            The object to unpack.
            with_metadata:  Expect metadata columns in the object.
            drop_null:      Drop fields with NULL values, if true.
                            Keep them otherwise.

        Returns:
            The unpacked object.
        """
        unpacked_obj = {}
        columns = filter(
            lambda c: with_metadata or not c.schema.metadata_expr,
            self.columns.values()
        )
        for column, value in zip(columns, obj):
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

    def unpack_iter(self, obj_seq, with_metadata, drop_null=True):
        """
        Create a generator unpacking database object representations from
        the specified sequence into their JSON representation.

        Args:
            obj_seq:        The object sequence to create the generator for.
            with_metadata:  Expect metadata columns in the objects.
            drop_null:      Drop fields with NULL values, if true.
                            Keep them otherwise.

        Returns:
            The generator unpacking the object sequence.
        """
        for obj in obj_seq:
            yield self.unpack(obj,
                              with_metadata=with_metadata,
                              drop_null=drop_null)


class View:
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
        self.select = select
        self.refresh_period = refresh_period

    def format_create(self, name):
        """
        Format the creation command for the view.

        Args:
            name:       The name to give the view.

        Returns:
            The command creating the view.
        """
        raise NotImplementedError

    def format_setup(self, name):
        """
        Format the view maintenance setup commands.

        Args:
            name:       The name of the view to be maintained.

        Returns:
            The tuple of commands setting up the view maintenance.
        """
        raise NotImplementedError

    def format_refresh(self, name):
        """
        Format the view refresh commands.

        Args:
            name:       The name of the view to refresh

        Returns:
            The command refreshing the view.
        """
        raise NotImplementedError

    def format_teardown(self, name):
        """
        Format the view maintenance teardown commands.

        Args:
            name:       The name of the view to stop maintenance of.

        Returns:
            The command tearing down the view maintenance.
        """
        raise NotImplementedError

    def format_drop(self, name):
        """
        Format the drop command for the view.

        Args:
            name:       The name of the view being dropped.

        Returns:
            The command dropping the view.
        """
        raise NotImplementedError


class Index:
    """An index schema"""

    # The type of table columns used in the index
    TableColumn = TableColumn

    def __init__(self, table, columns, key_sep="_", unique=False):
        """
        Initialize the index schema.

        Args:
            table:      The name of the table this index belongs to.
            columns:    The list of names of table columns belonging to the
                        index. The names consist of dot-separated parts, same
                        as used for the Table creation parameters.
            key_sep:    String used to replace dots in column names ("key"
                        separator) when adapting them to the tables.
            unique:     True if the index is unique, False otherwise.
        """
        assert isinstance(table, str)
        assert isinstance(columns, list)
        assert all(isinstance(c, str) for c in columns)
        assert isinstance(unique, bool)
        self.table = table
        # Dot-separated column names => quoted table column names
        self.columns = {
            name: self.TableColumn.adapt_name(name, key_sep)[0]
            for name in columns
        }
        self.unique = unique

    def format_create(self, name):
        """
        Format the "CREATE INDEX" command for the table.

        Args:
            name:       The name of the target index of the command.

        Returns:
            The formatted "CREATE INDEX" command.
        """
        return (
            f"CREATE {['', 'UNIQUE '][self.unique]}INDEX "
            f"IF NOT EXISTS {name} ON {self.table} (" +
            ", ".join(self.columns.values()) +
            ")"
        )
