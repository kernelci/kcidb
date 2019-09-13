"""kcidb-query command-line executable"""

import argparse
from google.cloud import bigquery

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
    for row in query_job:
        print(row)
