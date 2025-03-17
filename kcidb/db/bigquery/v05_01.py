"""Kernel CI report database - BigQuery schema v5.1"""

import logging
from google.cloud.bigquery.schema import SchemaField as Field
from google.cloud.bigquery.table import Table
import kcidb.io as io
from kcidb.misc import merge_dicts
from .v05_00 import Schema as PreviousSchema

# Module's logger
LOGGER = logging.getLogger(__name__)


# Don't be so narrow-minded, pylint: disable=too-many-ancestors
class Schema(PreviousSchema):
    """BigQuery database schema v5.1"""

    # The schema's version.
    version = (5, 1)
    # The I/O schema the database schema supports
    io = io.schema.V5_1

    # A map of table names to their BigQuery schemas
    TABLE_MAP = merge_dicts(
        PreviousSchema.TABLE_MAP,
        checkouts=PreviousSchema.TABLE_MAP["checkouts"] + [
            Field(
                "origin_builds_finish_time", "TIMESTAMP",
                description="The time the origin of the checkout finished "
                            "all the builds it planned for it.",
            ),
            Field(
                "origin_tests_finish_time", "TIMESTAMP",
                description="The time the origin of the checkout finished "
                            "all the tests it planned for it.",
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
        # Add the new fields to the "checkouts" table.
        checkouts_table = Table(conn.dataset_ref.table("_checkouts"))
        checkouts_table.schema = cls.TABLE_MAP["checkouts"]
        conn.client.update_table(checkouts_table, ["schema"])
        # Update the view
        checkouts_view = Table(conn.dataset_ref.table("checkouts"))
        checkouts_view.view_query = cls._format_view_query(conn, "checkouts")
        conn.client.update_table(checkouts_view, ["view_query"])
