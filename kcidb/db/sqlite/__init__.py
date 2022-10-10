"""Kernel CI report database - SQLite driver"""

import textwrap
from kcidb.db.schematic import Driver as SchematicDriver
from kcidb.db.sqlite.v04_01 import Schema as LatestSchema


class Driver(SchematicDriver):
    """Kernel CI SQLite report database driver"""

    # The class representing latest database schema recognized by the driver
    LatestSchema = LatestSchema

    # Driver documentation
    _DOC = textwrap.dedent("""\
        The SQLite driver allows connection to an SQLite file database or
        an in-memory database.
    """)
