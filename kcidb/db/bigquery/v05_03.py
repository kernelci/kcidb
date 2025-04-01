"""Kernel CI report database - BigQuery schema v5.3"""

import logging
from google.cloud.bigquery.schema import SchemaField as Field
from google.cloud.bigquery.table import Table
import kcidb.io as io
from kcidb.misc import merge_dicts
from .v05_02 import Schema as PreviousSchema

# Module's logger
LOGGER = logging.getLogger(__name__)


# Don't be so narrow-minded, pylint: disable=too-many-ancestors
class Schema(PreviousSchema):
    """BigQuery database schema v5.3"""

    # The schema's version.
    version = (5, 3)
    # The I/O schema the database schema supports
    io = io.schema.V5_3

    # A map of table names to their BigQuery schemas
    TABLE_MAP = merge_dicts(
        PreviousSchema.TABLE_MAP,
        tests=PreviousSchema.TABLE_MAP["tests"] + [
            Field(
                "input_files", "RECORD", mode="REPEATED",
                fields=PreviousSchema.RESOURCE_FIELDS,
                description="A list of test input files. "
                            "E.g. rootfs, configuration, samples.",
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
        # Add the new fields to the "tests" table.
        tests_table = Table(conn.dataset_ref.table("_tests"))
        tests_table.schema = cls.TABLE_MAP["tests"]
        conn.client.update_table(tests_table, ["schema"])
        # Update the view
        tests_view = Table(conn.dataset_ref.table("tests"))
        tests_view.view_query = cls._format_view_query(conn, "tests")
        conn.client.update_table(tests_view, ["view_query"])
