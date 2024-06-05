"""Kernel CI report database - SQLite schema v4.3"""

import logging
import kcidb.io as io
from kcidb.misc import merge_dicts
from kcidb.db.sqlite.schema import \
    Constraint, BoolColumn, IntegerColumn, TextColumn, \
    JSONColumn, Table
from .v04_02 import Schema as PreviousSchema, TIMESTAMP_COLUMN

# Module's logger
LOGGER = logging.getLogger(__name__)


class Schema(PreviousSchema):
    """SQLite database schema v4.3"""

    # The schema's version.
    version = (4, 3)
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

    # Queries and their columns for each type of raw object-oriented data.
    # Both should have columns in the same order.
    OO_QUERIES = merge_dicts(
        PreviousSchema.OO_QUERIES,
        checkout=merge_dicts(
            PreviousSchema.OO_QUERIES["checkout"],
            statement="SELECT\n"
                      "    id,\n"
                      "    git_commit_hash,\n"
                      # Add this column
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
        # Add transitions
        transition=merge_dicts(
            PreviousSchema.OO_QUERIES["transition"],
            statement="SELECT\n"
                      "    id,\n"
                      "    version,\n"
                      "    origin,\n"
                      "    issue_id,\n"
                      "    issue_version,\n"
                      "    appearance,\n"
                      "    \"revision_before.git_commit_hash\" AS "
                      "revision_before_git_commit_hash,\n"
                      "    \"revision_before.patchset_hash\" AS "
                      "revision_before_patchset_hash,\n"
                      "    \"revision_after.git_commit_hash\" AS "
                      "revision_after_git_commit_hash,\n"
                      "    \"revision_after.patchset_hash\" AS "
                      "revision_after_patchset_hash,\n"
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
                      "        \"revision_before.git_commit_hash\",\n"
                      "        \"revision_before.patchset_hash\",\n"
                      "        \"revision_after.git_commit_hash\",\n"
                      "        \"revision_after.patchset_hash\",\n"
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
        with conn:
            cursor = conn.cursor()
            try:
                # Add the git_commit_generation column to checkouts
                column = cls.TABLES["checkouts"]. \
                    columns["git_commit_generation"]
                cursor.execute(f"""
                    ALTER TABLE checkouts ADD COLUMN {column.format_def()}
                """)
                # Create new tables
                for table_name, table_schema in cls.TABLES.items():
                    if table_name not in PreviousSchema.TABLES:
                        try:
                            cursor.execute(
                                table_schema.format_create(table_name)
                            )
                        except Exception as exc:
                            raise Exception(
                                f"Failed creating table {table_name!r}"
                            ) from exc
            finally:
                cursor.close()
