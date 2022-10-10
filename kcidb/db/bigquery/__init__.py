"""Kernel CI report database - BigQuery driver"""

import textwrap
from kcidb.db.schematic import Driver as SchematicDriver
from kcidb.db.bigquery.v04_01 import Schema as LatestSchema


class Driver(SchematicDriver):
    """Kernel CI BigQuery report database driver"""

    # The class representing latest database schema recognized by the driver
    LatestSchema = LatestSchema

    # Driver documentation
    _DOC = textwrap.dedent("""\
        The BigQuery driver allows connection to a Google Cloud project
        and corresponding BigQuery dataset.
    """)
