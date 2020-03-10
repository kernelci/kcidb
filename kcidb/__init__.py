"""Kernel CI reporting"""

import argparse
import json
import sys
import jsonschema
from kcidb import db, io, oo

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
                    Must adhere to a version of I/O schema.
        """
        assert io.schema.is_valid(data)
        self.db_client.load(data)

    def query(self, patterns, children=False, parents=False):
        """
        Match and fetch report objects.

        Args:
            patterns:   A dictionary of object list names, and lists of LIKE
                        patterns, for IDs of objects to match.
            children:   True if children of matched objects should be matched
                        as well.
            parents:    True if parents of matched objects should be matched
                        as well.

        Returns:
            The fetched JSON data adhering to the latest I/O schema version.

        Raises:
            `IncompatibleSchema` if the dataset schema is incompatible with
            the latest I/O schema.
        """
        data = self.db_client.query(patterns, children, parents)
        assert io.schema.is_valid_latest(data)
        return data


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
    data = io.schema.upgrade(data, copy=False)
    client = db.Client(args.dataset)
    client.load(data)


def query_main():
    """Execute the kcidb-query command-line tool"""
    return db.query_main("kcidb-query - Query Kernel CI reports")


def schema_main():
    """Execute the kcidb-schema command-line tool"""
    description = 'kcidb-schema - Output latest I/O JSON schema'
    parser = argparse.ArgumentParser(description=description)
    parser.parse_args()
    json.dump(io.schema.LATEST.json, sys.stdout, indent=4, sort_keys=True)


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


def upgrade_main():
    """Execute the kcidb-upgrade command-line tool"""
    description = 'kcidb-upgrade - Upgrade I/O JSON data to latest schema'
    parser = argparse.ArgumentParser(description=description)
    parser.parse_args()

    try:
        data = json.load(sys.stdin)
    except json.decoder.JSONDecodeError as err:
        print(err, file=sys.stderr)
        return 1

    try:
        data = io.schema.upgrade(data, copy=False)
    except jsonschema.exceptions.ValidationError as err:
        print(err, file=sys.stderr)
        return 2

    json.dump(data, sys.stdout, indent=4, sort_keys=True)
    return 0


def summarize_main():
    """Execute the kcidb-summarize command-line tool"""
    description = 'kcidb-summarize - Output summaries of report objects'
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        'obj_list_name',
        metavar='LIST',
        choices=[n for n in io.schema.LATEST.tree if n],
        help='Name of the object list to output (%(choices)s)'
    )
    parser.add_argument(
        'ids',
        metavar='ID',
        nargs='*',
        default=[],
        help='ID of the object to limit output to'
    )
    args = parser.parse_args()
    oo_data = oo.from_io(io.schema.upgrade(json.load(sys.stdin), copy=False))
    obj_map = oo_data.get(args.obj_list_name, {})
    for obj_id in args.ids or obj_map:
        if obj_id in obj_map:
            print(obj_map[obj_id].summarize())
    return 0


def describe_main():
    """Execute the kcidb-describe command-line tool"""
    description = 'kcidb-describe - Output descriptions of report objects'
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        'obj_list_name',
        metavar='LIST',
        choices=[n for n in io.schema.LATEST.tree if n],
        help='Name of the object list to output'
    )
    parser.add_argument(
        'ids',
        metavar='ID',
        nargs='*',
        default=[],
        help='ID of the object to limit output to'
    )
    args = parser.parse_args()
    oo_data = oo.from_io(io.schema.upgrade(json.load(sys.stdin), copy=False))
    obj_map = oo_data.get(args.obj_list_name, {})
    for obj_id in args.ids or obj_map:
        if obj_id in obj_map:
            sys.stdout.write(obj_map[obj_id].describe())
            sys.stdout.write("\x00")
    return 0
