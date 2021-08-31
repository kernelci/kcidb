"""Kernel CI report database - JSON-initialized in-memory database driver"""

import sys
import textwrap
import kcidb_io as io
import kcidb.misc
from kcidb.db.sqlite import Driver as SQLiteDriver


class Driver(SQLiteDriver):
    """Kernel CI I/O JSON-initialized in-memory database driver"""

    PARAMS_DOC = textwrap.dedent("""\
        Format: [FILE]

        [FILE]  An optional path to a file containing I/O JSON to read as
                initial database data. If not specified, standard input is
                read. The file is never modified.
    """)

    def __init__(self, params):
        """
        Initialize the BigQuery driver.

        Args:
            params: A path to a file containing I/O JSON to read as database
                    data, or None to read standard input.
        """
        assert params is None or isinstance(params, str)
        with sys.stdin if params is None \
                else open(params, "r", encoding='utf8') as json_file:
            super().__init__(":memory:")
            self.init()
            for data in kcidb.misc.json_load_stream_fd(json_file.fileno()):
                data = io.schema.upgrade(io.schema.validate(data), copy=False)
                self.load(data)
