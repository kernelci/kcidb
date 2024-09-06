"""Kernel CI report database - SQLite schema v4.4"""

import logging
import kcidb.io as io
from kcidb.misc import merge_dicts
from kcidb.db.sqlite.schema import \
    BoolColumn, TextColumn, JSONColumn, Table, TimestampColumn
from .v04_03 import Schema as PreviousSchema

# Module's logger
LOGGER = logging.getLogger(__name__)


class Schema(PreviousSchema):
    """SQLite database schema v4.4"""

    # The schema's version.
    version = (4, 4)
    # The I/O schema the database schema supports
    io = io.schema.V4_5

    # A map of table names and Table constructor arguments
    # For use by descendants
    TABLES_ARGS = merge_dicts(
        PreviousSchema.TABLES_ARGS,
        checkouts=merge_dicts(
            PreviousSchema.TABLES_ARGS["checkouts"],
            columns=merge_dicts(
                PreviousSchema.TABLES_ARGS["checkouts"]["columns"],
                {
                    "git_commit_tags": JSONColumn(),
                    "git_commit_message": TextColumn(),
                    "git_repository_branch_tip": BoolColumn(),
                }
            )
        )
    )

    # A map of table names and schemas
    TABLES = {
        name: Table(**args) for name, args in TABLES_ARGS.items()
    }

    # Queries and their columns for each type of raw object-oriented data.
    # Both should have columns in the same order.
    OO_QUERIES = merge_dicts(
        PreviousSchema.OO_QUERIES,
        checkout=dict(
            statement="SELECT\n"
                      "    id,\n"
                      "    git_commit_hash,\n"
                      "    git_commit_tags,\n"
                      "    git_commit_message,\n"
                      "    patchset_hash,\n"
                      "    origin,\n"
                      "    git_repository_url,\n"
                      "    git_repository_branch,\n"
                      "    git_repository_branch_tip,\n"
                      "    tree_name,\n"
                      "    message_id,\n"
                      "    start_time,\n"
                      "    log_url,\n"
                      "    log_excerpt,\n"
                      "    comment,\n"
                      "    valid,\n"
                      "    misc\n"
                      "FROM checkouts",
            schema=Table(dict(
                id=TextColumn(),
                git_commit_hash=TextColumn(),
                git_commit_tags=JSONColumn(),
                git_commit_message=TextColumn(),
                patchset_hash=TextColumn(),
                origin=TextColumn(),
                git_repository_url=TextColumn(),
                git_repository_branch=TextColumn(),
                git_repository_branch_tip=BoolColumn(),
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
                # For all tables
                for name, schema in cls.TABLES.items():
                    if name not in PreviousSchema.TABLES:
                        continue
                    for column_name in sorted(
                        set(cls.TABLES_ARGS[name]["columns"]) -
                        set(PreviousSchema.TABLES_ARGS[name]["columns"])
                    ):
                        cursor.execute(f"""
                            ALTER TABLE {name} ADD COLUMN
                            {schema.columns[column_name].format_def()}
                        """)
            finally:
                cursor.close()
