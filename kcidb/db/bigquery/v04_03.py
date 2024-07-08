"""Kernel CI report database - BigQuery schema v4.3"""

import logging
from google.cloud.bigquery.schema import SchemaField as Field
from google.cloud.bigquery.table import Table
from kcidb.misc import merge_dicts
import kcidb.io as io
from .v04_02 import Schema as PreviousSchema, TIMESTAMP_FIELD

# Module's logger
LOGGER = logging.getLogger(__name__)

# Git commit generation number field
GIT_COMMIT_GENERATION_FIELD = Field(
    "git_commit_generation", "INTEGER",
    description="The commit generation number",
)

# Revision ID fields
REVISION_ID_FIELDS = (
    Field(
        "git_commit_hash", "STRING",
        description="The full commit hash of the checked out base "
                    "source code",
    ),
    Field(
        "patchset_hash", "STRING",
        description="The patchset hash",
    ),
)


class Schema(PreviousSchema):
    """BigQuery database schema v4.3"""

    # The schema's version.
    version = (4, 3)
    # The I/O schema the database schema supports
    io = io.schema.V4_4

    # A map of table names to their BigQuery schemas
    TABLE_MAP = merge_dicts(PreviousSchema.TABLE_MAP, dict(
        checkouts=PreviousSchema.TABLE_MAP["checkouts"] + [
            GIT_COMMIT_GENERATION_FIELD,
        ],
        transitions=[
            TIMESTAMP_FIELD,
            Field(
                "id", "STRING",
                description="Transition ID",
            ),
            Field(
                "version", "INTEGER",
                description="Transition version number",
            ),
            Field(
                "origin", "STRING",
                description="The name of the CI system which submitted "
                            "the transition",
            ),
            Field(
                "issue_id", "STRING",
                description="ID of the transitioning issue",
            ),
            Field(
                "issue_version", "INTEGER",
                description="Version number of the transitioning issue",
            ),
            Field(
                "appearance", "BOOL",
                description="True if this is an issue appearance, "
                            "false if disappearance.",
            ),
            Field(
                "revision_before", "RECORD", fields=REVISION_ID_FIELDS,
                description="ID of the last-known revision before the "
                            "transition"
            ),
            Field(
                "revision_after", "RECORD", fields=REVISION_ID_FIELDS,
                description="ID of the first-known revision after the "
                            "transition",
            ),
            Field(
                "comment", "STRING",
                description="A human-readable comment regarding the "
                            "transition",
            ),
            Field(
                "misc", "STRING",
                description="Miscellaneous extra data about the "
                            "transition in JSON format",
            ),
        ],
    ))

    # A map of table names and their "primary key" fields
    KEYS_MAP = dict(
        **PreviousSchema.KEYS_MAP,
        transitions=("id", "version",),
    )

    # A map of table names to the dictionary of fields and the names of their
    # aggregation function, if any (the default is "ANY_VALUE").
    AGGS_MAP = merge_dicts(
        PreviousSchema.AGGS_MAP,
        dict(transitions={TIMESTAMP_FIELD.name: "MAX"})
    )

    # Queries for each type of raw object-oriented data
    OO_QUERIES = merge_dicts(
        PreviousSchema.OO_QUERIES,
        checkout="SELECT\n"
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
        # Implement transitions
        transition="SELECT\n"
                   "    id,\n"
                   "    version,\n"
                   "    origin,\n"
                   "    issue_id,\n"
                   "    issue_version,\n"
                   "    appearance,\n"
                   "    revision_before.git_commit_hash AS "
                   "revision_before_git_commit_hash,\n"
                   "    revision_before.patchset_hash AS "
                   "revision_before_patchset_hash,\n"
                   "    revision_after.git_commit_hash AS "
                   "revision_after_git_commit_hash,\n"
                   "    revision_after.patchset_hash AS "
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
                   "        revision_before,\n"
                   "        revision_after,\n"
                   "        comment,\n"
                   "        misc,\n"
                   "        ROW_NUMBER() OVER (\n"
                   "            PARTITION BY id\n"
                   "            ORDER BY version DESC\n"
                   "        ) AS precedence\n"
                   "    FROM transitions\n"
                   ")\n"
                   "WHERE precedence = 1",
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
        # Add the "git_commit_generation" field to _checkouts
        conn.query_create(f"""
            ALTER TABLE `_checkouts`
            ADD COLUMN IF NOT EXISTS
            `{GIT_COMMIT_GENERATION_FIELD.name}`
            {GIT_COMMIT_GENERATION_FIELD.field_type}
            OPTIONS(description={GIT_COMMIT_GENERATION_FIELD.description!r})
        """).result()
        # Update the checkouts view
        view_ref = conn.dataset_ref.table('checkouts')
        view = Table(view_ref)
        view.view_query = cls._format_view_query(conn, 'checkouts')
        conn.client.update_table(view, ["view_query"])
        # Create new tables
        for table_name in cls.TABLE_MAP:
            if table_name not in PreviousSchema.TABLE_MAP:
                cls._create_table(conn, table_name)
