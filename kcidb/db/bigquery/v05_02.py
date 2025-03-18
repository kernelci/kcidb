"""Kernel CI report database - BigQuery schema v5.2"""

import logging
from google.cloud.bigquery.schema import SchemaField as Field
from google.cloud.bigquery.table import Table
import kcidb.io as io
from kcidb.misc import merge_dicts
from .v05_01 import Schema as PreviousSchema

# Module's logger
LOGGER = logging.getLogger(__name__)


# Don't be so narrow-minded, pylint: disable=too-many-ancestors
class Schema(PreviousSchema):
    """BigQuery database schema v5.2"""

    # The schema's version.
    version = (5, 2)
    # The I/O schema the database schema supports
    io = io.schema.V5_2

    # A map of table names to their BigQuery schemas
    TABLE_MAP = merge_dicts(
        PreviousSchema.TABLE_MAP,
        issues=PreviousSchema.TABLE_MAP["issues"] + [
            Field(
                "categories", "STRING", mode="REPEATED",
                description="The categories the issue belongs to",
            ),
        ]
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
        # Add the new fields to the "issues" table.
        issues_table = Table(conn.dataset_ref.table("_issues"))
        issues_table.schema = cls.TABLE_MAP["issues"]
        conn.client.update_table(issues_table, ["schema"])
        # Update the view
        issues_view = Table(conn.dataset_ref.table("issues"))
        issues_view.view_query = cls._format_view_query(conn, "issues")
        conn.client.update_table(issues_view, ["view_query"])
