"""Kernel CI reporting"""

import argparse
import json
import sys
import jsonschema
from kcidb import db, io

# pylint: disable=invalid-name,fixme
# TODO Remove once users switched to kcidb.io.schema
# Compatibility alias
io_schema = io.schema


class Client:
    """Kernel CI reporting client"""

    def __init__(self, dataset_name):
        """
        Initialize a reporting client

        Args:
            dataset_name:   The name of the Kernel CI dataset. The dataset
                            should be located within the Google Cloud project
                            specified in the credentials file pointed to by
                            GOOGLE_APPLICATION_CREDENTIALS environment
                            variable.
        """
        self.db_client = db.Client(dataset_name)

    def submit(self, data):
        """
        Submit reports.

        Args:
            data:   The JSON report data to submit.
                    Must adhere to the I/O schema (kcidb.io.schema.JSON).
        """
        self.db_client.load(data)

    def query(self):
        """
        Query reports.

        Returns:
            The JSON report data adhering to the I/O schema
            (kcidb.io.schema.JSON).
        """
        return self.db_client.query()


def submit_main():
    """Execute the kcidb-submit command-line tool"""
    description = \
        'kcidb-submit - Submit Kernel CI reports'
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        '-d', '--dataset',
        help='Dataset name',
        required=True
    )
    args = parser.parse_args()
    data = json.load(sys.stdin)
    io.schema.validate(data)
    client = db.Client(args.dataset)
    client.load(data)


def query_main():
    """Execute the kcidb-query command-line tool"""
    description = \
        'kcidb-query - Query Kernel CI reports'
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        '-d', '--dataset',
        help='Dataset name',
        required=True
    )
    args = parser.parse_args()
    client = db.Client(args.dataset)
    json.dump(client.query(), sys.stdout, indent=4, sort_keys=True)


def schema_main():
    """Execute the kcidb-schema command-line tool"""
    description = 'kcidb-schema - Output I/O JSON schema'
    parser = argparse.ArgumentParser(description=description)
    parser.parse_args()
    json.dump(io.schema.JSON, sys.stdout, indent=4, sort_keys=True)


def validate_main():
    """Execute the kcidb-validate command-line tool"""
    description = 'kcidb-validate - Validate I/O JSON data'
    parser = argparse.ArgumentParser(description=description)
    parser.parse_args()

    try:
        data = json.load(sys.stdin)
    except json.decoder.JSONDecodeError as err:
        print(err, file=sys.stderr)
        return 1

    try:
        io.schema.validate(data)
    except jsonschema.exceptions.ValidationError as err:
        print(err, file=sys.stderr)
        return 2
    return 0
