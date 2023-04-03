"""Kernel CI reporting test catalog"""

import sys
import yaml
import requests
from kcidb import misc, argparse
from kcidb.tests import schema


def validate_main():
    """Execute the kcidb-tests-validate command-line tool"""
    sys.excepthook = misc.log_and_print_excepthook
    description = 'kcidb-tests-validate - Validate test catalog YAML'
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "-u", "--urls",
        action='store_true',
        help="Verify URLs in the catalog are accessible"
    )
    args = parser.parse_args()
    catalog = yaml.safe_load(sys.stdin)
    schema.validate(catalog)
    if args.urls:
        for test in catalog.values():
            try:
                requests.head(test['home'], timeout=60).raise_for_status()
            except requests.exceptions.SSLError:
                requests.head(
                    test['home'], timeout=60, verify=False
                ).raise_for_status()
