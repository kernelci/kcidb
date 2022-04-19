"""
Kernel CI SQLite report database schema.

Always corresponds to the current I/O schema.
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


# A map of table names to CREATE TABLE statements
TABLES = dict(
    checkouts=dict(
        columns={
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
            "log_excerpt": TextColumn(),
            "valid": BoolColumn(),
            "misc": JSONColumn(),
        }
    ),
    builds=dict(
        columns={
            "checkout_id": TextColumn(constraint=Constraint.NOT_NULL),
            "id": TextColumn(constraint=Constraint.PRIMARY_KEY),
            "origin": TextColumn(constraint=Constraint.NOT_NULL),
            "comment": TextColumn(),
            "start_time": TimestampColumn(),
            "duration": Column("REAL"),
            "architecture": TextColumn(),
            "command": TextColumn(),
            "compiler": TextColumn(),
            "input_files": JSONColumn(),
            "output_files": JSONColumn(),
            "config_name": TextColumn(),
            "config_url": TextColumn(),
            "log_url": TextColumn(),
            "log_excerpt": TextColumn(),
            "valid": BoolColumn(),
            "misc": JSONColumn(),
        },
    ),
    tests=dict(
        columns={
            "build_id": TextColumn(constraint=Constraint.NOT_NULL),
            "id": TextColumn(constraint=Constraint.PRIMARY_KEY),
            "origin": TextColumn(constraint=Constraint.NOT_NULL),
            "environment.id": TextColumn(),
            "environment.comment": TextColumn(),
            "environment.misc": JSONColumn(),
            "path": TextColumn(),
            "comment": TextColumn(),
            "log_url": TextColumn(),
            "log_excerpt": TextColumn(),
            "status": TextColumn(),
            "waived": BoolColumn(),
            "start_time": TimestampColumn(),
            "duration": Column("REAL"),
            "output_files": JSONColumn(),
            "misc": JSONColumn()
        },
    ),
)

# Queries and their columns for each type of raw object-oriented data.
# Both should have columns in the same order.
# NOTE: Relying on dictionaries preserving order in Python 3.6+
OO_QUERIES = dict(
    revision=dict(
        statement="SELECT\n"
                  "   git_commit_hash,\n"
                  "   patchset_hash,\n"
                  "   patchset_files,\n"
                  "   git_commit_name,\n"
                  "   contacts\n"
                  "FROM checkouts\n"
                  "GROUP BY git_commit_hash, patchset_hash",
        columns=dict(
            git_commit_hash=TextColumn(),
            patchset_hash=TextColumn(),
            patchset_files=JSONColumn(),
            git_commit_name=TextColumn(),
            contacts=JSONColumn(),
        ),
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
        columns=dict(
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
        ),
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
        columns=dict(
            id=TextColumn(),
            checkout_id=TextColumn(),
            origin=TextColumn(),
            start_time=TimestampColumn(),
            duration=Column("REAL"),
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
        ),
    ),
    test=dict(
        statement="SELECT\n"
                  "   id,\n"
                  "   build_id,\n"
                  "   origin,\n"
                  "   path,\n"
                  "   \"environment.comment\" AS environment_comment,\n"
                  "   \"environment.misc\" AS environment_misc,\n"
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
        columns=dict(
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
            duration=Column("REAL"),
            output_files=JSONColumn(),
            comment=TextColumn(),
            misc=JSONColumn(),
        ),
    ),
)
