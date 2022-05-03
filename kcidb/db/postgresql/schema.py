"""
Kernel CI PostgreSQL report database schema.

Always corresponds to the current I/O schema.
"""
import json
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
        Initialize the column schema.

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
        Format a column definition without the name.

        Returns: The formatted column definition.
        """
        nameless_def = self.type
        if self.constraint:
            nameless_def += " " + self.constraint.value
        return nameless_def


class BoolColumn(Column):
    """A boolean column schema"""

    def __init__(self, constraint=None):
        """
        Initialize the column schema.

        Args:
            constraint:     The column's constraint.
                            A member of the Constraint enum, or None,
                            meaning no constraint.
        """
        assert constraint is None or isinstance(constraint, Constraint)
        super().__init__("BOOLEAN", constraint=constraint)


class VarcharColumn(Column):
    """A character varying column schema"""

    def __init__(self, length, constraint=None):
        """
        Initialize the column schema.

        Args:
            length:         The maximum length of the strings that could be
                            stored in the column, in characters.
            constraint:     The column's constraint.
                            A member of the Constraint enum, or None,
                            meaning no constraint.
        """
        assert isinstance(length, int)
        assert length > 0
        assert constraint is None or isinstance(constraint, Constraint)
        super().__init__(f"CHARACTER VARYING ({length})",
                         constraint=constraint)


class TextColumn(Column):
    """A text column schema"""

    def __init__(self, constraint=None):
        """
        Initialize the column schema.

        Args:
            constraint:     The column's constraint.
                            A member of the Constraint enum, or None,
                            meaning no constraint.
        """
        assert constraint is None or isinstance(constraint, Constraint)
        super().__init__("TEXT", constraint=constraint)


class JSONColumn(Column):
    """A JSON column schema"""

    @staticmethod
    def pack(value):
        """
        Pack the JSON representation of the column value into the PostgreSQL
        representation.
        """
        return json.dumps(value)

    def __init__(self, constraint=None):
        """
        Initialize the column schema.

        Args:
            constraint:     The column's constraint.
                            A member of the Constraint enum, or None,
                            meaning no constraint.
        """
        assert constraint is None or isinstance(constraint, Constraint)
        super().__init__("JSONB", constraint=constraint)


class TimestampColumn(Column):
    """A timestamp column schema"""

    @staticmethod
    def unpack(value):
        """
        Unpack the SQLite representation of the column value into the JSON
        representation.
        """
        return value.isoformat()

    def __init__(self, constraint=None):
        """
        Initialize the column schema.

        Args:
            constraint:     The column's constraint.
                            A member of the Constraint enum, or None,
                            meaning no constraint.
        """
        assert constraint is None or isinstance(constraint, Constraint)
        super().__init__("TIMESTAMP WITH TIME ZONE", constraint=constraint)


class FloatColumn(Column):
    """A floating-point number column schema"""

    def __init__(self, constraint=None):
        """
        Initialize the column schema.

        Args:
            constraint:     The column's constraint.
                            A member of the Constraint enum, or None,
                            meaning no constraint.
        """
        assert constraint is None or isinstance(constraint, Constraint)
        super().__init__("DOUBLE PRECISION", constraint=constraint)


class TableColumn:
    """A column within a table schema"""
    def __init__(self, name, schema):
        """
        Initialize the table schema column.

        Args:
            name:   The name of the column consisting of dot-separated parts
                    (keys).
            schema: The column's schema (an instance of Column).
        """
        assert isinstance(name, str)
        assert isinstance(schema, Column)
        # Name parts (keys)
        self.keys = name.split(".")
        # Column name within the table
        self.name = "_".join(self.keys)
        # Column schema
        self.schema = schema


class Table:
    """A table schema"""

    def __init__(self, columns):
        """
        Initialize the table schema.

        Args:
            columns:    A dictionary of column names consisting of
                        dot-separated parts (keys), and the column schemas.
        """
        assert isinstance(columns, dict)
        assert all(
            isinstance(name, str) and
            isinstance(column, Column)
            for name, column in columns.items()
        )
        # Column list
        self.columns = [
            TableColumn(name, column)
            for name, column in columns.items()
        ]
        # Column list definition for CREATE TABLE command,
        # with one column per line.
        self.columns_def = ",\n".join(
            column.name + " " + column.schema.format_nameless_def()
            for column in self.columns
        )
        # A string of comma-separated column names for use in commands
        self.columns_list = ", ".join(column.name for column in self.columns)

    def format_insert(self, name, prio_db):
        """
        Format the "INSERT" command template for loading a row into a
        database, observing deduplication logic.

        Args:
            name:       The name of the target table of the command.
            prio_db:    If the object being loaded is already in the database,
                        prioritize the database values, if true, and the
                        values being loaded otherwise.

        Returns:
            The formatted "INSERT" command template, expecting parameters
            packed by the pack() method.
        """
        assert isinstance(name, str)
        return \
            f"INSERT INTO {name} (\n" + \
            ",\n".join(f"    {c.name}" for c in self.columns) + \
            "\n)\nVALUES (\n    " + \
            ", ".join(("%s", ) * len(self.columns)) + \
            "\n)\nON CONFLICT (" + \
            ", ".join(
                c.name for c in self.columns
                if c.schema.constraint == Constraint.PRIMARY_KEY
            ) + ") DO UPDATE SET\n" + \
            ",\n".join(
                f"    {c.name} = COALESCE(" + (
                    f"{name}.{c.name}, excluded.{c.name}"
                    if prio_db else
                    f"excluded.{c.name}, {name}.{c.name}"
                ) + ")"
                for c in self.columns
                if c.schema.constraint != Constraint.PRIMARY_KEY
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

    def pack(self, obj):
        """
        Pack a JSON object into its PostgreSQL representation for use with the
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
        into their PostgreSQL representation, for use with the "INSERT"
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
        Unpack a PostgreSQL representation of an object into its JSON
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
        Create a generator unpacking PostgreSQL object representations from
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


# A map of table names to CREATE TABLE statements
TABLES = dict(
    checkouts=Table({
        "id": TextColumn(constraint=Constraint.PRIMARY_KEY),
        "origin": TextColumn(constraint=Constraint.NOT_NULL),
        "tree_name": TextColumn(),
        "git_repository_url": TextColumn(),
        "git_commit_hash": TextColumn(),
        "git_commit_name": TextColumn(),
        "git_repository_branch": TextColumn(),
        "patchset_files": JSONColumn(),
        "patchset_hash": TextColumn(),
        "message_id": TextColumn(),
        "comment": TextColumn(),
        "start_time": TimestampColumn(),
        "contacts": JSONColumn(),
        "log_url": TextColumn(),
        "log_excerpt": VarcharColumn(16384),
        "valid": BoolColumn(),
        "misc": JSONColumn(),
    }),
    builds=Table({
        "checkout_id": TextColumn(constraint=Constraint.NOT_NULL),
        "id": TextColumn(constraint=Constraint.PRIMARY_KEY),
        "origin": TextColumn(constraint=Constraint.NOT_NULL),
        "comment": TextColumn(),
        "start_time": TimestampColumn(),
        "duration": FloatColumn(),
        "architecture": TextColumn(),
        "command": TextColumn(),
        "compiler": TextColumn(),
        "input_files": JSONColumn(),
        "output_files": JSONColumn(),
        "config_name": TextColumn(),
        "config_url": TextColumn(),
        "log_url": TextColumn(),
        "log_excerpt": VarcharColumn(16384),
        "valid": BoolColumn(),
        "misc": JSONColumn(),
    }),
    tests=Table({
        "build_id": TextColumn(constraint=Constraint.NOT_NULL),
        "id": TextColumn(constraint=Constraint.PRIMARY_KEY),
        "origin": TextColumn(constraint=Constraint.NOT_NULL),
        "environment.comment": TextColumn(),
        "environment.misc": JSONColumn(),
        "path": TextColumn(),
        "comment": TextColumn(),
        "log_url": TextColumn(),
        "log_excerpt": VarcharColumn(16384),
        "status": TextColumn(),
        "waived": BoolColumn(),
        "start_time": TimestampColumn(),
        "duration": FloatColumn(),
        "output_files": JSONColumn(),
        "misc": JSONColumn()
    }),
)

# Queries and their columns for each type of raw object-oriented data.
# Both should have columns in the same order.
# NOTE: Relying on dictionaries preserving order in Python 3.6+
OO_QUERIES = dict(
    revision=dict(
        statement="SELECT\n"
                  "   git_commit_hash,\n"
                  "   patchset_hash,\n"
                  "   FIRST(patchset_files),\n"
                  "   FIRST(git_commit_name),\n"
                  "   FIRST(contacts)\n"
                  "FROM checkouts\n"
                  "GROUP BY git_commit_hash, patchset_hash",
        schema=Table(dict(
            git_commit_hash=TextColumn(),
            patchset_hash=TextColumn(),
            patchset_files=JSONColumn(),
            git_commit_name=TextColumn(),
            contacts=JSONColumn(),
        )),
    ),
    checkout=dict(
        statement="SELECT\n"
                  "   id,\n"
                  "   git_commit_hash,\n"
                  "   patchset_hash,\n"
                  "   origin,\n"
                  "   git_repository_url,\n"
                  "   git_repository_branch,\n"
                  "   tree_name,\n"
                  "   message_id,\n"
                  "   start_time,\n"
                  "   log_url,\n"
                  "   log_excerpt,\n"
                  "   comment,\n"
                  "   valid,\n"
                  "   misc\n"
                  "FROM checkouts",
        schema=Table(dict(
            id=TextColumn(),
            git_commit_hash=TextColumn(),
            patchset_hash=TextColumn(),
            origin=TextColumn(),
            git_repository_url=TextColumn(),
            git_repository_branch=TextColumn(),
            tree_name=TextColumn(),
            message_id=TextColumn(),
            start_time=TimestampColumn(),
            log_url=TextColumn(),
            log_excerpt=TextColumn(),
            comment=TextColumn(),
            valid=BoolColumn(),
            misc=JSONColumn(),
        )),
    ),
    build=dict(
        statement="SELECT\n"
                  "   id,\n"
                  "   checkout_id,\n"
                  "   origin,\n"
                  "   start_time,\n"
                  "   duration,\n"
                  "   architecture,\n"
                  "   command,\n"
                  "   compiler,\n"
                  "   input_files,\n"
                  "   output_files,\n"
                  "   config_name,\n"
                  "   config_url,\n"
                  "   log_url,\n"
                  "   log_excerpt,\n"
                  "   comment,\n"
                  "   valid,\n"
                  "   misc\n"
                  "FROM builds",
        schema=Table(dict(
            id=TextColumn(),
            checkout_id=TextColumn(),
            origin=TextColumn(),
            start_time=TimestampColumn(),
            duration=FloatColumn(),
            architecture=TextColumn(),
            command=TextColumn(),
            compiler=TextColumn(),
            input_files=JSONColumn(),
            output_files=JSONColumn(),
            config_name=TextColumn(),
            config_url=TextColumn(),
            log_url=TextColumn(),
            log_excerpt=TextColumn(),
            comment=TextColumn(),
            valid=BoolColumn(),
            misc=JSONColumn(),
        )),
    ),
    test=dict(
        statement="SELECT\n"
                  "   id,\n"
                  "   build_id,\n"
                  "   origin,\n"
                  "   path,\n"
                  "   environment_comment,\n"
                  "   environment_misc,\n"
                  "   log_url,\n"
                  "   log_excerpt,\n"
                  "   status,\n"
                  "   waived,\n"
                  "   start_time,\n"
                  "   duration,\n"
                  "   output_files,\n"
                  "   comment,\n"
                  "   misc\n"
                  "FROM tests",
        schema=Table(dict(
            id=TextColumn(),
            build_id=TextColumn(),
            origin=TextColumn(),
            path=TextColumn(),
            environment_comment=TextColumn(),
            environment_misc=JSONColumn(),
            log_url=TextColumn(),
            log_excerpt=TextColumn(),
            status=TextColumn(),
            waived=BoolColumn(),
            start_time=TimestampColumn(),
            duration=FloatColumn(),
            output_files=JSONColumn(),
            comment=TextColumn(),
            misc=JSONColumn(),
        )),
    ),
)
