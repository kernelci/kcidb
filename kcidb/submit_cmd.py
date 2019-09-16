"""kcidb-submit command-line executable"""

import argparse
import sys
import json
from google.cloud import bigquery
from kcidb import io_schema


def main():
    """Run the executable"""
    description = 'kcidb-submit - Submit test results to kernelci.org database'
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        '-d', '--dataset',
        help='Dataset name',
        required=True
    )
    args = parser.parse_args()

    data = json.load(sys.stdin)
    io_schema.validate(data)

    client = bigquery.Client()
    dataset_ref = client.dataset(args.dataset)
    if "test_list" in data:
        table_ref = dataset_ref.table("tests")
        job = client.load_table_from_json(data["test_list"], table_ref)
        job.result()
