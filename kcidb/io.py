"""KCIDB package-specific I/O definitions"""

# Inherit all public definitions from kcidb_io package
# We know what we're doing, flake8 and
# pylint: disable=wildcard-import,unused-wildcard-import
from kcidb_io import *  # noqa: F403

# The I/O schema version used by KCIDB
SCHEMA = schema.V5_0  # noqa: F405
