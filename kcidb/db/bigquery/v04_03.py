"""Kernel CI report database - BigQuery schema v4.3"""

import logging
from google.cloud.bigquery.schema import SchemaField as Field
from google.cloud.bigquery.table import Table
import kcidb.io as io
from kcidb.misc import merge_dicts
from .v04_02 import Schema as PreviousSchema

# Module's logger
LOGGER = logging.getLogger(__name__)


class Schema(PreviousSchema):
    """BigQuery database schema v4.3"""

    # The schema's version.
    version = (4, 3)
    # The I/O schema the database schema supports
    io = io.schema.V4_4

    # Test environment fields
    ENVIRONMENT_FIELDS = PreviousSchema.ENVIRONMENT_FIELDS + (
        Field(
            "compatible", "STRING", mode="REPEATED",
            description="The values from the root-level "
                        "'compatible' property of the system's "
                        "device tree, if any, in the same order"
        ),
    )

    # Number fields
    NUMBER_FIELDS = (
        Field(
            "value", "FLOAT64",
            description="The floating-point output value",
        ),
        Field(
            "unit", "STRING",
            description="The (compound) unit symbol(s) the value "
                        "is measured in",
        ),
        Field(
            "prefix", "STRING",
            description="The type of prefix to add to the "
                        "unit, after the value is scaled. "
                        "If unit is not specified, the prefix "
                        "alone is used in its place. "
                        "Either \"metric\" or \"binary\".",
        ),
    )

    # A map of table names to their BigQuery schemas
    TABLE_MAP = merge_dicts(
        PreviousSchema.TABLE_MAP,
        tests=list(filter(
            lambda f: f.name != "environment",
            PreviousSchema.TABLE_MAP["tests"]
        )) + [
            Field(
                "environment", "RECORD", fields=ENVIRONMENT_FIELDS,
                description="The environment the test ran in. "
                            "E.g. a host, a set of hosts, or a lab; "
                            "amount of memory/storage/CPUs, for each host; "
                            "process environment variables, etc.",
            ),
            Field(
                "number", "RECORD", fields=NUMBER_FIELDS,
                description="The numerical output produced by the test",
            ),
        ]
    )

    # Queries for each type of raw object-oriented data
    OO_QUERIES = merge_dicts(
        PreviousSchema.OO_QUERIES,
        test=merge_dicts(
            PreviousSchema.OO_QUERIES["test"],
            statement="SELECT\n"
                      "    id,\n"
                      "    build_id,\n"
                      "    origin,\n"
                      "    path,\n"
                      "    environment.comment AS environment_comment,\n"
                      "    environment.compatible AS environment_compatible,\n"
                      "    environment.misc AS environment_misc,\n"
                      "    status,\n"
                      "    number.value AS number_value,\n"
                      "    number.unit AS number_unit,\n"
                      "    number.prefix AS number_prefix,\n"
                      "    start_time,\n"
                      "    duration,\n"
                      "    output_files,\n"
                      "    log_url,\n"
                      "    log_excerpt,\n"
                      "    comment,\n"
                      "    misc\n"
                      "FROM tests",
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
        # Add the new fields to the "tests" table.
        tests_table = Table(conn.dataset_ref.table("_tests"))
        tests_table.schema = cls.TABLE_MAP["tests"]
        conn.client.update_table(tests_table, ["schema"])
        # Update the view
        tests_view = Table(conn.dataset_ref.table("tests"))
        tests_view.view_query = cls._format_view_query(conn, "tests")
        conn.client.update_table(tests_view, ["view_query"])
