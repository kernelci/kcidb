#!/usr/bin/env python3

"""KCIDB auto-matching tool"""


import json
import sys
import sqlite3
import hashlib
import logging
import argparse
from .pattern_validator import match_fields, validate_pattern_object


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


class PatternDatabase:
    """Class to handle DB table 'patterns'"""
    def __init__(self, db_name=DB_NAME):
        self.db_name = db_name
        self.setup_database()

    def setup_database(self):
        """Connect to DB and create 'patterns' table if doesn't exist"""
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
        """Add pattern object to DB"""
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
        """Remove pattern object from DB"""
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM patterns WHERE issue_id = ?',
                           (issue_id,))
            conn.commit()

    def get_all_patterns(self):
        """Retrieve all patterns objects from DB"""
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT issue_id, issue_version, pattern_object '
                           'FROM patterns')
            while True:
                row = cursor.fetchone()
                if row is None:
                    break
                yield row

    def update_patterns(self, issue):
        """Update patterns for existing issue"""
        if not issue.misc:
            return

        if not issue.misc.get("pattern_object"):
            self.remove_pattern(issue.id)
            return

        pattern_object = issue.misc.get("pattern_object")
        if not validate_pattern_object(pattern_object):
            logger.error("Pattern object validation failed for issue id: %s",
                         issue.id)
            return
        self.add_pattern(issue.id, issue.version, pattern_object)


class IncidentGenerator:
    """Class to generate incidents"""
    def __init__(self, db_name=DB_NAME):
        self.db = PatternDatabase(db_name)

    def create_incident(self, kcidb_io_object, issue_id, issue_version):
        """Create and return an incident object"""
        if tests := kcidb_io_object.get('tests'):
            type_id_key = "test_id"
            type_id_value = tests[0]['id']
        elif builds := kcidb_io_object.get('builds'):
            type_id_key = "build_id"
            type_id_value = builds[0]['id']
        else:
            raise ValueError("The KCIDB IO object must contain at least "
                             "one non-empty test or build")

        unique_string = f"{issue_id}{issue_version}{type_id_value}"
        incident_id = f"{ORIGIN}:" \
                      f"{hashlib.sha256(unique_string.encode()).hexdigest()}"

        return {
            'id': incident_id,
            'origin': ORIGIN,
            'issue_id': issue_id,
            'issue_version': issue_version,
            'present': True,
            type_id_key: type_id_value,
        }

    def generate_incident_on_match(self, kcidb_io_object, issue_id,
                                   issue_version, issue_pattern_object):
        """Generate incident if issue pattern is found in a build/test
        object"""
        incident = {}

        if match_fields(issue_pattern_object, kcidb_io_object):
            incident = self.create_incident(kcidb_io_object, issue_id,
                                            issue_version)

        return incident

    def generate_incidents_from_db(self, kcidb_io_object):
        """
        Generate incidents by trying to match the kcidb_io_object
        against the patterns saved in the database
        """
        incidents = []

        for row in self.db.get_all_patterns():
            issue_id, issue_version, pattern_object_json = row
            pattern_object = json.loads(pattern_object_json)
            incident = self.generate_incident_on_match(
                kcidb_io_object, issue_id, issue_version, pattern_object)
            if incident:
                incidents.append(incident)

        return {
            "version": KCIDB_IO_VERSION,
            "incidents": incidents
        }

    def generate_incidents_from_test(self, test):
        """Generate incident from test object"""
        kcidb_io_object = {"tests": [test._data],
                           "builds": [test.build._data],
                           "checkouts": [test.build.checkout._data]}
        return self.generate_incidents_from_db(kcidb_io_object)

    def generate_incidents_from_build(self, build):
        """Generate incident from build object"""
        kcidb_io_object = {"builds": [build._data],
                           "checkouts": [build.checkout._data]}
        return self.generate_incidents_from_db(kcidb_io_object)


def parse_arguments():
    """Parse command-line arguments"""
    class CustomHelpFormatter(argparse.RawTextHelpFormatter):
        """Help string formatter for command-line tools"""

    parser = argparse.ArgumentParser(
        description='KCIDB Match Tool',
        formatter_class=CustomHelpFormatter,
        epilog='''\
Usage examples:

export -x DB_OPTS="postgresql:host=127.0.0.1 port=5432 sslmode=disable
dbname=playground_kcidb user=helen.koike@collabora.com"

# Update patterns
kcidb-query -i "kernelci_api:70d17807303641a9d6d2a8aeb1aee829221cefcf"
-d "$DB_OPTS" | ./kcidb-match.py --update-patterns

# Generate incidents
kcidb-query -t "maestro:6690dbfc7488a1b744200e82" -d "$DB_OPTS"
--parents | ./kcidb-match.py --generate-incidents

# Check test ID
cat issue.json | ./kcidb-match.py --check_test_id
"maestro:6690dbfc7488a1b744200e82" -d "$DB_OPTS"

# Check build ID
cat issue.json | ./kcidb-match.py --check_build_id
"maestro:6690dbfc7488a1b744200e82" -d "$DB_OPTS"
'''
    )

    parser.add_argument('--update-patterns', action='store_true',
                        help='Update patterns from issues. Other '
                             'arguments are ignored when used. Expects '
                             'KCIDB-IO object with issues via stdin.')

    parser.add_argument('--generate-incidents', action='store_true',
                        help='Generate incidents for matched issues. '
                             'Expects KCIDB-IO object with build and/or '
                             'test via stdin.')

    parser.add_argument('--ignore-db', action='store_true',
                        help='Ignore the database and generate incidents '
                             'based on the issues field in the KCIDB-IO '
                             'object via stdin.')

    parser.add_argument('--check_test_id', type=str,
                        help='Test ID to check. Requires --db_conn. '
                             'Implies --ignore-db. Expects KCIDB-IO '
                             'object with issues via stdin.')

    parser.add_argument('--check_build_id', type=str,
                        help='Build ID to check. Requires --db_conn. '
                             'Implies --ignore-db. '
                             'Expects KCIDB-IO object with issues via stdin.')

    parser.add_argument('-d', '--db_conn', type=str,
                        help='Database connection string for kcidb-query.'
                             'Required with --check_test_id or '
                             '--check_build_id.')

    args = parser.parse_args()

    if args.check_test_id and args.check_build_id:
        parser.error("Cannot use both --check_test_id and --check_build_id")

    if (args.check_test_id or args.check_build_id) and not args.db_conn:
        parser.error("--db_conn is required when using --check_test_id or "
                     "--check_build_id")

    if args.check_test_id or args.check_build_id:
        args.ignore_db = True

    return args


def main():
    """Main function"""
    args = parse_arguments()

    if args.update_patterns:
        issue_objects = json.load(sys.stdin)
        IncidentGenerator().db.update_patterns(issue_objects)
        return

    kcidb_io_object = json.load(sys.stdin)

    incident_generator = IncidentGenerator()

    results = incident_generator.generate_incidents_from_db(
        kcidb_io_object)

    if args.generate_incidents:
        print(json.dumps(results, indent=2))
        return

    for incident in results['incidents']:
        print("Matched issue ID:", incident['issue_id'], "Version:",
              incident['issue_version'])


if __name__ == "__main__":
    main()
