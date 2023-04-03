"""
KCIDB object-oriented (OO) data representation-management
command-line argument parsing
"""

import kcidb.db.argparse
import kcidb.orm.argparse


class ArgumentParser(kcidb.argparse.ArgumentParser):
    """
    Command-line argument parser with common OO arguments added.
    """

    def __init__(self, *args, database=None, **kwargs):
        """
        Initialize the parser, adding common OO arguments.

        Args:
            args:       Positional arguments to initialize ArgumentParser
                        with.
            database:   The default database specification to use, or None to
                        make database specification required.
            kwargs:     Keyword arguments to initialize ArgumentParser with.
        """
        super().__init__(*args, **kwargs)
        kcidb.db.argparse.add_args(self, database=database)
        kcidb.orm.argparse.add_args(self)


class OutputArgumentParser(kcidb.argparse.OutputArgumentParser):
    """
    Command-line argument parser for tools outputting JSON,
    with common OO arguments added.
    """

    def __init__(self, *args, database=None, **kwargs):
        """
        Initialize the parser, adding JSON output arguments.

        Args:
            args:       Positional arguments to initialize ArgumentParser
                        with.
            database:   The default database specification to use, or None to
                        make database specification required.
            kwargs:     Keyword arguments to initialize ArgumentParser with.
        """
        super().__init__(*args, **kwargs)
        kcidb.db.argparse.add_args(self, database=database)
        kcidb.orm.argparse.add_args(self)
