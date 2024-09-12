"""Kernel CI report database - BigQuery schema v4.4"""

import logging
from google.cloud.bigquery.schema import SchemaField as Field
from google.cloud.bigquery.table import Table
import kcidb.io as io
from kcidb.misc import merge_dicts
from .v04_03 import Schema as PreviousSchema

# Module's logger
LOGGER = logging.getLogger(__name__)


class Schema(PreviousSchema):
    """BigQuery database schema v4.4"""

    # The schema's version.
    version = (4, 4)
    # The I/O schema the database schema supports
    io = io.schema.V4_5

    # A map of table names to their BigQuery schemas
    TABLE_MAP = merge_dicts(
        PreviousSchema.TABLE_MAP,
        checkouts=PreviousSchema.TABLE_MAP["checkouts"] + [
            Field(
                "git_commit_tags", "STRING", mode="REPEATED",
                description="The list of (annotated) tags, found in the "
                            "checked-out repository, pointing directly "
                            "at the commit being checked out. I.e. as "
                            "output by \"git tag --points-at <commit>\"."
            ),
            Field(
                "git_commit_message", "STRING",
                description="The complete message of the commit being "
                            "checked-out, both the subject and the body. "
                            "I.e. as output by \"git show -s --format=%B\"."
            ),
            Field(
                "git_repository_branch_tip", "BOOL",
                description="True if at the moment of checkout (specified in "
                            "\"start_time\") the checked out commit was at "
                            "the tip of the specified branch in the "
                            "specified repository. False if it was further "
                            "back in history.\n"
                            "\n"
                            "This information is used to reconstruct the "
                            "approximate history of the branch changes for "
                            "display and analyzis, in lieu of actual commit "
                            "graph walking."
            ),
        ]
    )

    # Queries for each type of raw object-oriented data
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
        # Add the new fields to the "checkouts" table.
        checkouts_table = Table(conn.dataset_ref.table("_checkouts"))
        checkouts_table.schema = cls.TABLE_MAP["checkouts"]
        conn.client.update_table(checkouts_table, ["schema"])
        # Update the view
        checkouts_view = Table(conn.dataset_ref.table("checkouts"))
        checkouts_view.view_query = cls._format_view_query(conn, "checkouts")
        conn.client.update_table(checkouts_view, ["view_query"])
