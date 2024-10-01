#!/usr/bin/env python3

import json
import re
import sys
import copy
import sqlite3
import hashlib
import logging
import subprocess
from jsonschema import validate, ValidationError
import kcidb  # Assuming kcidb.io.SCHEMA is available here
import argparse
import requests
import gzip

# Constants
DB_NAME = 'patterns.db'
ORIGIN = 'maestro'
KCIDB_IO_VERSION = {
    "major": 4,
    "minor": 3
}

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_log(url, snippet_lines=0):
    """Fetches a text log given its url.

    Returns:
      If the log file couldn't be retrieved by any reason: None
      Otherwise:
        If snippet_lines == 0: the full log
        If snippet_lines > 0: the first snippet_lines log lines
        If snippet_lines < 0: the last snippet_lines log lines
    """
    response = requests.get(url)
    if not len(response.content):
        return None
    try:
        raw_bytes = gzip.decompress(response.content)
        text = raw_bytes.decode('utf-8')
    except gzip.BadGzipFile:
        text = response.text
    if snippet_lines > 0:
        lines = text.splitlines()
        return '\n'.join(lines[:snippet_lines])
    elif snippet_lines < 0:
        lines = text.splitlines()
        return '\n'.join(lines[snippet_lines:])
    return text

class PatternDatabase:
    def __init__(self, db_name=DB_NAME):
        self.db_name = db_name
        self.setup_database()

    def setup_database(self):
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS patterns (
                    issue_id TEXT UNIQUE,
                    issue_version INTEGER,
                    pattern_object JSON
                )
            ''')
            conn.commit()

    def add_pattern(self, issue_id, issue_version, pattern_object):
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO patterns (issue_id, issue_version, pattern_object)
                VALUES (?, ?, json(?))
                ON CONFLICT(issue_id) DO UPDATE SET
                issue_version=excluded.issue_version,
                pattern_object=excluded.pattern_object
            ''', (issue_id, issue_version, json.dumps(pattern_object)))
            conn.commit()

    def remove_pattern(self, issue_id):
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM patterns WHERE issue_id = ?', (issue_id,))
            conn.commit()

    def get_all_patterns(self):
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT issue_id, issue_version, pattern_object FROM patterns')
            while True:
                row = cursor.fetchone()
                if row is None:
                    break
                yield row

    def update_patterns(self, issue_objects):
        for issue in issue_objects['issues']:
            if "pattern_object" not in issue["misc"]:
                self.remove_pattern(issue["id"])
                continue

            pattern_object = issue["misc"]["pattern_object"]
            if not PatternValidator().validate_pattern_object(pattern_object):
                logger.error("Pattern object validation failed for issue id: %s", issue["id"])
                continue

            self.add_pattern(issue["id"], issue["version"], pattern_object)


class PatternValidator:
    def __init__(self):
        self.schema = copy.deepcopy(kcidb.io.SCHEMA.json)
        self.remove_required_fields(self.schema)

    def remove_required_fields(self, schema):
        if "required" in schema:
            del schema["required"]
        if "properties" in schema:
            for key, subschema in schema["properties"].items():
                self.remove_required_fields(subschema)
        if "items" in schema:
            self.remove_required_fields(schema["items"])
        if "$defs" in schema:
            for key, subschema in schema["$defs"].items():
                self.remove_required_fields(subschema)

    def validate_pattern_object(self, pattern_object):
        pattern_object_clone = copy.deepcopy(pattern_object)
        try:
            validate(instance=pattern_object_clone, schema=self.schema)
        except ValidationError as err:
            logger.error("Validation error: %s", err)
            return False
        return True

    def match_fields(self, pattern_obj, kcidb_obj):
        """
        Matches fields from the pattern_obj against the fields in kcidb_obj.

        Args:
            pattern_obj (dict): The pattern object containing fields to match.
            kcidb_obj (dict): The KCIDB object containing fields to be matched against.

        Returns:
            bool: True if all fields match, False otherwise.
        """

        def match_value(pattern_value, kcidb_value):
            """Check if a pattern value matches a KCIDB value."""
            if isinstance(pattern_value, str) and isinstance(kcidb_value, str):
                return bool(re.match(pattern_value, kcidb_value))
            return pattern_value == kcidb_value

        def match_item(pattern_item, kcidb_item):
            """Match all sub-fields in pattern_item against those in kcidb_item."""
            for sub_key, sub_pattern_value in pattern_item.items():
                if sub_key == "log_regex": # log_regex is special, ignore it here
                    return True
                if sub_key not in kcidb_item:
                    logger.debug(f"Field {sub_key} does not exist in KCIDB entry")
                    return False
                if not match_value(sub_pattern_value, kcidb_item[sub_key]):
                    logger.debug(f"Field {sub_key} with value {kcidb_item[sub_key]} does not match {sub_pattern_value}")
                    return False
            return True

        for key, pattern_value in pattern_obj.items():
            # Ensure the key exists in the KCIDB object
            if key not in kcidb_obj:
                logger.debug(f"Key {key} not found in kcidb_obj")
                return False

            if isinstance(pattern_value, list):
                # Both lists are empty, consider it a match
                if not pattern_value and not kcidb_obj[key]:
                    continue
                # One list is empty, consider it a mismatch
                if not pattern_value or not kcidb_obj[key]:
                    logger.debug(f"Key {key} is empty in one of the objects")
                    return False

                # Check if the first items in the list match
                if not match_item(pattern_value[0], kcidb_obj[key][0]):
                    return False
            else:
                # Match individual items
                if not match_item(pattern_value, kcidb_obj[key]):
                    return False

        if 'tests' in pattern_obj and 'log_regex' in pattern_obj['tests'][0]:
            return self.check_log_match(kcidb_obj, pattern_obj, "tests")
        if 'builds' in pattern_obj and 'log_regex' in pattern_obj['builds'][0]:
            return self.check_log_match(kcidb_obj, pattern_obj, "builds")

        return True

    def check_log_match(self, kcidb_io_object, pattern_object, key):
        def search_regex(log_regex, log_excerpt, log_url):
            if re.search(log_regex, log_excerpt):
                return True

            log_content = get_log(log_url)
            if log_content and re.search(log_regex, log_content):
                return True

            return False

        log_regex = pattern_object[key][0].get("log_regex")
        if log_regex and key in kcidb_io_object and kcidb_io_object[key]:
            log_excerpt = kcidb_io_object[key][0].get("log_excerpt", "")
            log_url = kcidb_io_object[key][0].get("log_url", "")
            return search_regex(log_regex, log_excerpt, log_url)
        return False


class IncidentGenerator:
    def __init__(self, db_name=DB_NAME):
        self.db = PatternDatabase(db_name)
        self.validator = PatternValidator()

    def create_incident(self, kcidb_io_object, issue_id, issue_version):
        if 'tests' in kcidb_io_object and kcidb_io_object['tests']:
            type_id_key = "test_id"
            type_id_value = kcidb_io_object['tests'][0]['id']
        elif 'builds' in kcidb_io_object and kcidb_io_object['builds']:
            type_id_key = "build_id"
            type_id_value = kcidb_io_object['builds'][0]['id']
        else:
            raise ValueError("The KCIDB IO object must contain at least one non-empty test or build")

        unique_string = f"{issue_id}{issue_version}{type_id_value}"
        incident_id = f"{ORIGIN}:{hashlib.sha256(unique_string.encode()).hexdigest()}"

        return {
            'id': incident_id,
            'origin': ORIGIN,
            'issue_id': issue_id,
            'issue_version': issue_version,
            'present': True,
            type_id_key: type_id_value,
        }

    def generate_incident_on_match(self, kcidb_io_object, issue_id, issue_version, issue_pattern_object):
        incidents = []

        if self.validator.match_fields(issue_pattern_object, kcidb_io_object):
            incident = self.create_incident(kcidb_io_object, issue_id, issue_version)
            incidents.append(incident)

        return incidents

    def generate_incidents_from_db(self, kcidb_io_object):
        incidents = []

        for row in self.db.get_all_patterns():
            issue_id, issue_version, pattern_object_json = row
            pattern_object = json.loads(pattern_object_json)
            incidents.extend(self.generate_incident_on_match(kcidb_io_object, issue_id, issue_version, pattern_object))

        return {
            "version": KCIDB_IO_VERSION,
            "incidents": incidents
        }

    def generate_incidents_from_object(self, kcidb_io_object):
        incidents = []

        for issue in kcidb_io_object['issues']:
            pattern_object = issue['misc']['pattern_object'] if 'pattern_object' in issue['misc'] else {}
            if not pattern_object:
                continue  # Nothing to match
            incidents.extend(self.generate_incident_on_match(kcidb_io_object, issue['id'], issue['version'], pattern_object))

        return {
            "version": KCIDB_IO_VERSION,
            "incidents": incidents
        }


def fetch_kcidb_io_object(query_id, db_conn, query_type):
    command = ["kcidb-query", f"-{query_type[0]}", query_id, "-d", db_conn, "--parents"]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"Failed to fetch KCIDB IO object: {result.stderr}")
    return json.loads(result.stdout)


def validate_kcidb_io_object(kcidb_io_object):
    if not (('tests' in kcidb_io_object and kcidb_io_object['tests']) or
            ('builds' in kcidb_io_object and kcidb_io_object['builds'])):
        logger.error("The KCIDB IO object must contain at least one non-empty test or build")
        return False

    try:
        kcidb.io.SCHEMA.validate(kcidb_io_object)
    except ValidationError as err:
        logger.error("KCIDB IO object validation error: %s", err)
        return False

    return True


def parse_arguments():
    class CustomHelpFormatter(argparse.RawTextHelpFormatter):
        pass

    parser = argparse.ArgumentParser(
        description='KCIDB Match Tool',
        formatter_class=CustomHelpFormatter,
        epilog='''\
Usage examples:

export -x DB_OPTS="postgresql:host=127.0.0.1 port=5432 sslmode=disable dbname=playground_kcidb user=helen.koike@collabora.com"

# Update patterns
kcidb-query -i "kernelci_api:70d17807303641a9d6d2a8aeb1aee829221cefcf" -d "$DB_OPTS" | ./kcidb-match.py --update-patterns

# Generate incidents
kcidb-query -t "maestro:6690dbfc7488a1b744200e82" -d "$DB_OPTS" --parents | ./kcidb-match.py --generate-incidents

# Check test ID
cat issue.json | ./kcidb-match.py --check_test_id "maestro:6690dbfc7488a1b744200e82" -d "$DB_OPTS"

# Check build ID
cat issue.json | ./kcidb-match.py --check_build_id "maestro:6690dbfc7488a1b744200e82" -d "$DB_OPTS"
'''
    )

    parser.add_argument('--update-patterns', action='store_true',
                        help='Update patterns from issues. Other arguments are ignored when used. Expects KCIDB-IO object with issues via stdin.')

    parser.add_argument('--generate-incidents', action='store_true',
                        help='Generate incidents for matched issues. Expects KCIDB-IO object with build and/or test via stdin.')

    parser.add_argument('--ignore-db', action='store_true',
                        help='Ignore the database and generate incidents based on the issues field in the KCIDB-IO object via stdin.')

    parser.add_argument('--check_test_id', type=str,
                        help='Test ID to check. Requires --db_conn. Implies --ignore-db. Expects KCIDB-IO object with issues via stdin.')

    parser.add_argument('--check_build_id', type=str,
                        help='Build ID to check. Requires --db_conn. Implies --ignore-db. Expects KCIDB-IO object with issues via stdin.')

    parser.add_argument('-d', '--db_conn', type=str,
                        help='Database connection string for kcidb-query. Required with --check_test_id or --check_build_id.')

    args = parser.parse_args()

    if args.check_test_id and args.check_build_id:
        parser.error("Cannot use both --check_test_id and --check_build_id")

    if (args.check_test_id or args.check_build_id) and not args.db_conn:
        parser.error("--db_conn is required when using --check_test_id or --check_build_id")

    if args.check_test_id or args.check_build_id:
        args.ignore_db = True

    return args


def main():
    args = parse_arguments()

    if args.update_patterns:
        issue_objects = json.load(sys.stdin)
        IncidentGenerator().db.update_patterns(issue_objects)
        return

    if args.check_test_id or args.check_build_id:
        id_type = 'test' if args.check_test_id else 'build'
        kcidb_id = args.check_test_id if args.check_test_id else args.check_build_id
        kcidb_io_object = fetch_kcidb_io_object(kcidb_id, args.db_conn, id_type)
        issue_objects = json.load(sys.stdin)
        kcidb_io_object["issues"] = issue_objects["issues"]
    else:
        kcidb_io_object = json.load(sys.stdin)

    if not validate_kcidb_io_object(kcidb_io_object):
        return

    incident_generator = IncidentGenerator()

    if args.ignore_db:
        results = incident_generator.generate_incidents_from_object(kcidb_io_object)
    else:
        results = incident_generator.generate_incidents_from_db(kcidb_io_object)

    if args.generate_incidents:
        print(json.dumps(results, indent=2))
        return

    for incident in results['incidents']:
        print("Matched issue ID:", incident['issue_id'], "Version:", incident['issue_version'])


if __name__ == "__main__":
    main()
