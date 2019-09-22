"""kcidb-query command-line executable"""

import argparse
import json
import sys
from google.cloud import bigquery
from kcidb import io_schema
from kcidb import db_schema


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
    data = dict(version="1")
    for obj_list_name in db_schema.TABLE_MAP:
        query_job = client.query("SELECT * FROM `" +
                                 args.dataset + "." + obj_list_name + "`")
        obj_list = []
        for row in query_job:
            obj = dict(item for item in row.items() if item[1] is not None)
            # Parse the "misc" fields
            if "misc" in obj:
                obj["misc"] = json.loads(obj["misc"])
            obj_list.append(obj)
        data[obj_list_name] = obj_list
    io_schema.validate(data)
    json.dump(data, sys.stdout, indent=4, sort_keys=True)
