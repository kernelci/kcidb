"""Kernel CI report database - PostgreSQL schema v4.9"""

from kcidb.misc import merge_dicts
import kcidb.io as io
from kcidb.db.postgresql.schema import \
    TextColumn, TextArrayColumn, \
    BoolColumn, Table, Index
from .v04_08 import Schema as PreviousSchema


# It's OK, pylint: disable=too-many-ancestors
class Schema(PreviousSchema):
    """PostgreSQL database schema v4.9"""

    # The schema's version.
    version = (4, 9)
    # The I/O schema the database schema supports
    io = io.schema.V4_5

    # A map of table names and Table constructor arguments
    # For use by descendants
    # Add environment_compatible, and number_* fields
    TABLES_ARGS = merge_dicts(
        PreviousSchema.TABLES_ARGS,
        checkouts=merge_dicts(
            PreviousSchema.TABLES_ARGS["checkouts"],
            columns=merge_dicts(
                PreviousSchema.TABLES_ARGS["checkouts"]["columns"],
                {
                    "git_commit_tags": TextArrayColumn(),
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

    # A map of index names and schemas
    INDEXES = merge_dicts(PreviousSchema.INDEXES, dict(
        checkouts_git_commit_tags=Index(
            "checkouts", ["git_commit_tags"], method="GIN"
        ),
        checkouts_git_repository_branch_tip=Index(
            "checkouts", ["git_repository_url"]
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
            # For all tables
            for name, schema in cls.TABLES.items():
                if name not in PreviousSchema.TABLES:
                    continue
                new_column_names = \
                    set(cls.TABLES_ARGS[name]["columns"]) - \
                    set(PreviousSchema.TABLES_ARGS[name]["columns"])
                if not new_column_names:
                    continue
                cursor.execute(
                    f"ALTER TABLE {name}" + ",".join(
                        " ADD COLUMN " + schema.columns[n].format_def()
                        for n in new_column_names
                    )
                )

            # For all indexes
            for index_name, index_schema in cls.INDEXES.items():
                if index_name not in PreviousSchema.INDEXES:
                    try:
                        cursor.execute(index_schema.format_create(index_name))
                    except Exception as exc:
                        raise Exception(
                            f"Failed creating index {index_name!r}"
                        ) from exc
