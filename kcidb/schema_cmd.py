"""kcidb-schema command-line executable"""

import argparse
import json
import sys
from kcidb import io_schema


def main():
    """Run the executable"""
    description = 'kcidb-schema - Output I/O JSON schema'
    parser = argparse.ArgumentParser(description=description)
    parser.parse_args()
    json.dump(io_schema.JSON, sys.stdout, indent=4, sort_keys=True)
