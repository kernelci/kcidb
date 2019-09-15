"""kcidb-query command-line executable"""

import argparse
import json
import sys
from google.cloud import bigquery
from kcidb import io_schema

def main():
    """Run the executable"""
    description = 'kcidb-query - Query test results from kernelci.org database'
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        '-d', '--dataset',
        help='Dataset name',
        required=True
    )
    args = parser.parse_args()

    client = bigquery.Client()
    query_job = client.query("SELECT * FROM `" + args.dataset + ".tests` ")
    test_case_list = [dict(row.items()) for row in query_job]
    io_schema.validate(test_case_list)
    json.dump(test_case_list, sys.stdout, indent=4, sort_keys=True)
