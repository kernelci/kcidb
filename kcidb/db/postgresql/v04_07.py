"""Kernel CI report database - PostgreSQL schema v4.7"""

import kcidb.io as io
from kcidb.misc import merge_dicts
from kcidb.db.postgresql.schema import Index
from .v04_02 import TIMESTAMP_COLUMN
from .v04_06 import Schema as PreviousSchema
from .schema import \
    Table, Constraint, BoolColumn, IntegerColumn, TextColumn, JSONColumn


# It's OK, pylint: disable=too-many-ancestors
class Schema(PreviousSchema):
    """PostgreSQL database schema v4.7"""

    # The schema's version.
    version = (4, 7)
    # The I/O schema the database schema supports
    io = io.schema.V4_4

    # A map of table names and Table constructor arguments
    # For use by descendants
    TABLES_ARGS = merge_dicts(
        PreviousSchema.TABLES_ARGS,
        checkouts=dict(
            columns=merge_dicts(
                PreviousSchema.TABLES_ARGS["checkouts"]["columns"],
                git_commit_generation=IntegerColumn(),
            ),
        ),
        transitions=dict(
            columns={
                "_timestamp": TIMESTAMP_COLUMN,
                "id": TextColumn(constraint=Constraint.NOT_NULL),
                "version": IntegerColumn(constraint=Constraint.NOT_NULL),
                "origin": TextColumn(constraint=Constraint.NOT_NULL),
                "issue_id": TextColumn(constraint=Constraint.NOT_NULL),
                "issue_version":
                    IntegerColumn(constraint=Constraint.NOT_NULL),
                "appearance": BoolColumn(),
                "revision_before.git_commit_hash": TextColumn(),
                "revision_before.patchset_hash": TextColumn(),
                "revision_after.git_commit_hash": TextColumn(),
                "revision_after.patchset_hash": TextColumn(),
                "comment": TextColumn(),
                "misc": JSONColumn(),
            },
            primary_key=["id", "version"]
        ),
    )

    # A map of table names and schemas
    TABLES = {
        name: Table(**args) for name, args in TABLES_ARGS.items()
    }

    # A map of index names and schemas
    INDEXES = merge_dicts(PreviousSchema.INDEXES, dict(
        checkouts_git_commit_generation=Index(
            "checkouts", ["git_commit_generation"]
        ),
        transitions__timestamp=Index("transitions", ["_timestamp"]),
        transitions_origin=Index("transitions", ["origin"]),
        transitions_issue_id=Index("transitions", ["issue_id"]),
        transitions_revision_before_git_commit_hash_patchset_hash=Index(
            "transitions",
            ["revision_before_git_commit_hash",
             "revision_before_patchset_hash"]
        ),
        transitions_revision_after_git_commit_hash_patchset_hash=Index(
            "transitions",
            ["revision_after_git_commit_hash",
             "revision_after_patchset_hash"]
        ),
    ))

    # Queries and their columns for each type of raw object-oriented data.
    # Both should have columns in the same order.
    OO_QUERIES = merge_dicts(
        PreviousSchema.OO_QUERIES,
        checkout=merge_dicts(
            PreviousSchema.OO_QUERIES["checkout"],
            statement="SELECT\n"
                      "    id,\n"
                      "    git_commit_hash,\n"
                      # Implement this column
                      "    git_commit_generation,\n"
                      "    patchset_hash,\n"
                      "    origin,\n"
                      "    git_repository_url,\n"
                      "    git_repository_branch,\n"
                      "    tree_name,\n"
                      "    message_id,\n"
                      "    start_time,\n"
                      "    log_url,\n"
                      "    log_excerpt,\n"
                      "    comment,\n"
                      "    valid,\n"
                      "    misc\n"
                      "FROM checkouts",
        ),
        # Implement transitions
        transition=merge_dicts(
            PreviousSchema.OO_QUERIES["transition"],
            statement="SELECT\n"
                      "    id,\n"
                      "    version,\n"
                      "    origin,\n"
                      "    issue_id,\n"
                      "    issue_version,\n"
                      "    appearance,\n"
                      "    revision_before_git_commit_hash,\n"
                      "    revision_before_patchset_hash,\n"
                      "    revision_after_git_commit_hash,\n"
                      "    revision_after_patchset_hash,\n"
                      "    comment,\n"
                      "    misc\n"
                      "FROM (\n"
                      "    SELECT\n"
                      "        id,\n"
                      "        version,\n"
                      "        origin,\n"
                      "        issue_id,\n"
                      "        issue_version,\n"
                      "        appearance,\n"
                      "        revision_before_git_commit_hash,\n"
                      "        revision_before_patchset_hash,\n"
                      "        revision_after_git_commit_hash,\n"
                      "        revision_after_patchset_hash,\n"
                      "        comment,\n"
                      "        misc,\n"
                      "        ROW_NUMBER() OVER (\n"
                      "            PARTITION BY id\n"
                      "            ORDER BY version DESC\n"
                      "        ) AS precedence\n"
                      "    FROM transitions\n"
                      ") AS prioritized_transitions\n"
                      "WHERE precedence = 1",
        ),
    )

    @classmethod
    def _inherit(cls, conn):
        """
        Inerit the database data from the previous schema version (if any).

        Args:
            conn:   Connection to the database to inherit. The database must
                    comply with the previous version of the schema.
        """
        assert isinstance(conn, cls.Connection)
        with conn, conn.cursor() as cursor:
            # Add the git_commit_generation column to checkouts
            column = cls.TABLES["checkouts"].columns["git_commit_generation"]
            cursor.execute(f"""
                ALTER TABLE checkouts ADD COLUMN {column.format_def()}
            """)
            # Create new tables
            for table_name, table_schema in cls.TABLES.items():
                if table_name not in PreviousSchema.TABLES:
                    try:
                        cursor.execute(table_schema.format_create(table_name))
                    except Exception as exc:
                        raise Exception(
                            f"Failed creating table {table_name!r}"
                        ) from exc
            # Create new indexes
            for index_name, index_schema in cls.INDEXES.items():
                if index_name not in PreviousSchema.INDEXES:
                    try:
                        cursor.execute(index_schema.format_create(index_name))
                    except Exception as exc:
                        raise Exception(
                            f"Failed creating index {index_name!r}"
                        ) from exc
