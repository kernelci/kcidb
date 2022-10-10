"""Kernel CI report database - PostgreSQL driver"""

import textwrap
from kcidb.db.schematic import Driver as SchematicDriver
from kcidb.db.postgresql.v04_01 import Schema as LatestSchema


class Driver(SchematicDriver):
    """Kernel CI PostgreSQL report database driver"""

    # The class representing latest database schema recognized by the driver
    LatestSchema = LatestSchema

    # Driver documentation
    _DOC = textwrap.dedent("""\
        The PostgreSQL driver allows connection to a PostgreSQL database.
    """)
