"""Kernel CI report database - BigQuery schema v4.2"""

import datetime
import logging
from google.cloud.bigquery import ScalarQueryParameter
from google.cloud.bigquery.schema import SchemaField as Field
from google.cloud.bigquery.table import Table
import kcidb.io as io
from .v04_01 import Schema as PreviousSchema

# Module's logger
LOGGER = logging.getLogger(__name__)

# The new _timestamp field added to every table in this schema
TIMESTAMP_FIELD = Field(
    "_timestamp", "TIMESTAMP",
    default_value_expression="CURRENT_TIMESTAMP",
    description="The time the row was added"
)


class Schema(PreviousSchema):
    """BigQuery database schema v4.2"""

    # The schema's version.
    version = (4, 2)
    # The I/O schema the database schema supports
    io = io.schema.V4_3

    # A map of table names to their BigQuery schemas
    TABLE_MAP = {
        name: [TIMESTAMP_FIELD, *fields]
        for name, fields in PreviousSchema.TABLE_MAP.items()
    }

    # A map of table names to the dictionary of fields and the names of their
    # aggregation function, if any (the default is "ANY_VALUE").
    AGGS_MAP = {
        name: {TIMESTAMP_FIELD.name: "MAX"}
        for name in TABLE_MAP
    }

    @classmethod
    def _inherit(cls, conn):
        """
        Inerit the database data from the previous schema version (if any).

        Args:
            conn:   Connection to the database to inherit. The database must
                    comply with the previous version of the schema.
        """
        assert isinstance(conn, cls.Connection)
        # For all tables/views
        for table_name, table_fields in cls.TABLE_MAP.items():
            # Add the _timestamp field to the raw table
            conn.query_create(f"""
                ALTER TABLE `_{table_name}`
                ADD COLUMN IF NOT EXISTS
                `{TIMESTAMP_FIELD.name}` {TIMESTAMP_FIELD.field_type}
                OPTIONS(description={TIMESTAMP_FIELD.description!r})
            """).result()
            # Set the _timestamp field default to current timestamp
            conn.query_create(f"""
                ALTER TABLE `_{table_name}`
                ALTER COLUMN `{TIMESTAMP_FIELD.name}`
                SET DEFAULT {TIMESTAMP_FIELD.default_value_expression}
            """).result()
            # Set missing _timestamp fields to start_time, or current time
            expr = "CURRENT_TIMESTAMP"
            if any(f.name == "start_time" for f in table_fields):
                expr = f"IFNULL(start_time, {expr})"
            conn.query_create(f"""
                UPDATE `_{table_name}`
                SET `{TIMESTAMP_FIELD.name}` = {expr}
                WHERE `{TIMESTAMP_FIELD.name}` IS NULL
            """).result()
            # Update the view
            view_ref = conn.dataset_ref.table(table_name)
            view = Table(view_ref)
            view.view_query = cls._format_view_query(conn, table_name)
            conn.client.update_table(view, ["view_query"])

    def purge(self, before):
        """
        Remove all the data from the database that arrived before the
        specified time, if the database supports that.

        Args:
            before: An "aware" datetime.datetime object specifying the
                    the earliest (database server) time the data to be
                    *preserved* should've arrived. Any other data will be
                    purged.
                    Can be None to have nothing removed. The latter can be
                    used to test if the database supports purging.

        Returns:
            True if the database supports purging, and the requested data was
            purged. False if the database doesn't support purging.
        """
        assert before is None or \
            isinstance(before, datetime.datetime) and before.tzinfo
        if before is not None:
            for table_name in self.TABLE_MAP:
                self.conn.query_create(
                    f"DELETE FROM `_{table_name}`"
                    f"WHERE `{TIMESTAMP_FIELD.name}` < @before",
                    [ScalarQueryParameter(
                        "before", TIMESTAMP_FIELD.field_type, before
                    )]
                ).result()
        return True
