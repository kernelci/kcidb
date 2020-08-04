"""Kernel CI reporting test catalog"""

import sys
import yaml
import requests
import jsonschema
from kcidb import misc
from kcidb.tests import schema


def validate_main():
    """Execute the kcidb-tests-validate command-line tool"""
    description = 'kcidb-tests-validate - Validate test catalog YAML'
    parser = misc.ArgumentParser(description=description)
    parser.add_argument(
        "-u", "--urls",
        action='store_true',
        help="Verify URLs in the catalog are accessible"
    )
    args = parser.parse_args()

    try:
        catalog = yaml.safe_load(sys.stdin)
    except yaml.YAMLError as err:
        print(misc.format_exception_stack(err), file=sys.stderr)
        return 1

    try:
        schema.validate(catalog)
    except jsonschema.exceptions.ValidationError as err:
        print(misc.format_exception_stack(err), file=sys.stderr)
        return 2

    if args.urls:
        try:
            for test in catalog.values():
                requests.head(test['home']).raise_for_status()
        except requests.RequestException as err:
            print(misc.format_exception_stack(err), file=sys.stderr)
            return 3

    return 0
