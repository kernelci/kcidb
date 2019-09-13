"""kcidb-submit command-line executable"""

import argparse
from google.cloud import bigquery

def main():
    """Run the executable"""
    description = 'kcidb-submit - Submit test results to kernelci.org database'
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        '-d', '--dataset',
        help='Dataset name',
        required=True
    )
    parser.add_argument(
        '-n', '--name',
        help='Test name',
        required=True
    )
    parser.add_argument(
        '-r', '--result',
        help='Test result (PASS/FAIL)',
        required=True,
        choices=['PASS', 'FAIL']
    )
    args = parser.parse_args()

    client = bigquery.Client()
    dataset_ref = client.dataset(args.dataset)
    table_ref = dataset_ref.table("tests")
    data = [{"name": args.name, "result": args.result}]
    job = client.load_table_from_json(data, table_ref)
    job.result()
