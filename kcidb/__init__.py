"""Kernel CI reporting"""

import json
import sys
import email
import logging
import jsonschema
from kcidb import db, io, mq, oo, spool, subscriptions, tests, misc

__all__ = [
    "db", "io", "mq", "oo", "spool", "subscriptions", "tests",
    "io_schema",
    "Client",
    "submit_main",
    "query_main",
    "schema_main",
    "validate_main",
    "upgrade_main",
    "summarize_main",
    "describe_main",
    "notify_main",
]

# pylint: disable=invalid-name,fixme
# TODO Remove once users switched to kcidb.io.schema
# Compatibility alias
io_schema = io.schema

# Module's logger
LOGGER = logging.getLogger(__name__)


class Client:
    """Kernel CI reporting client"""

    def __init__(self, project_id=None, dataset_name=None, topic_name=None):
        """
        Initialize a reporting client

        Args:
            project_id:     ID of the Google Cloud project hosting the report
                            database infrastructure. Can be None to have
                            submitting disabled.
            dataset_name:   The name of the Kernel CI dataset to query reports
                            from. The dataset should be located within the
                            specified Google Cloud project. Can be None,
                            to have querying disabled.
            topic_name:     Name of the message queue topic to publish
                            submissions to. The message queue should be
                            located within the specified Google Cloud project.
                            Can be None, to have submitting disabled.
        """
        assert project_id is None or \
            isinstance(project_id, str) and project_id
        assert dataset_name is None or \
            isinstance(dataset_name, str) and dataset_name
        assert topic_name is None or \
            isinstance(topic_name, str) and topic_name
        self.db_client = \
            db.Client(dataset_name, project_id=project_id) if dataset_name \
            else None
        self.mq_publisher = \
            mq.Publisher(project_id, topic_name) if project_id and topic_name \
            else None

    def submit(self, data):
        """
        Submit reports.

        Args:
            data:   The JSON report data to submit.
                    Must adhere to a version of I/O schema.

        Raises:
            `NotImplementedError`, if not supplied with a project ID or an MQ
            topic name at initialization time.
        """
        assert io.schema.is_valid(data)
        if self.mq_publisher:
            self.mq_publisher.publish(data)
        else:
            raise NotImplementedError

    def query(self, ids=None, patterns=None, children=False, parents=False):
        """
        Match and fetch report objects.

        Args:
            ids:        A dictionary of object list names, and lists of IDs of
                        objects to match. None means empty dictionary.
            patterns:   A dictionary of object list names, and lists of LIKE
                        patterns, for IDs of objects to match. None means
                        empty dictionary.
            children:   True if children of matched objects should be matched
                        as well.
            parents:    True if parents of matched objects should be matched
                        as well.

        Returns:
            The fetched JSON data adhering to the latest I/O schema version.

        Raises:
            `NotImplementedError`, if not supplied with a dataset name at
            initialization time;
            `IncompatibleSchema` if the dataset schema is incompatible with
            the latest I/O schema.
        """
        assert ids is None or isinstance(ids, dict)
        if ids is None:
            ids = dict()
        assert all(isinstance(k, str) and isinstance(v, list) and
                   all(isinstance(e, str) for e in v)
                   for k, v in ids.items())

        assert patterns is None or isinstance(patterns, dict)
        if patterns is None:
            patterns = dict()
        assert all(isinstance(k, str) and isinstance(v, list) and
                   all(isinstance(e, str) for e in v)
                   for k, v in patterns.items())

        if self.db_client:
            data = self.db_client.query(ids, patterns, children, parents)
        else:
            raise NotImplementedError

        assert io.schema.is_valid_latest(data)
        return data


def submit_main():
    """Execute the kcidb-submit command-line tool"""
    description = \
        'kcidb-submit - Submit Kernel CI reports'
    parser = misc.ArgumentParser(description=description)
    parser.add_argument(
        '-p', '--project',
        help='ID of the Google Cloud project containing the message queue',
        required=True
    )
    parser.add_argument(
        '-t', '--topic',
        help='Name of the message queue topic to publish to',
        required=True
    )
    args = parser.parse_args()
    data = json.load(sys.stdin)
    data = io.schema.upgrade(data, copy=False)
    client = Client(project_id=args.project, topic_name=args.topic)
    client.submit(data)


def query_main():
    """Execute the kcidb-query command-line tool"""
    description = \
        "kcidb-query - Query Kernel CI reports"
    parser = db.QueryArgumentParser(description=description)
    args = parser.parse_args()
    client = Client(project_id=args.project, dataset_name=args.dataset)
    data = client.query(ids=dict(revisions=args.revision_ids,
                                 builds=args.build_ids,
                                 tests=args.test_ids),
                        patterns=dict(revisions=args.revision_id_patterns,
                                      builds=args.build_id_patterns,
                                      tests=args.test_id_patterns),
                        parents=args.parents,
                        children=args.children)
    json.dump(data, sys.stdout, indent=4, sort_keys=True)


def schema_main():
    """Execute the kcidb-schema command-line tool"""
    description = 'kcidb-schema - Output latest I/O JSON schema'
    parser = misc.ArgumentParser(description=description)
    parser.parse_args()
    json.dump(io.schema.LATEST.json, sys.stdout, indent=4, sort_keys=True)


def validate_main():
    """Execute the kcidb-validate command-line tool"""
    description = 'kcidb-validate - Validate I/O JSON data'
    parser = misc.ArgumentParser(description=description)
    parser.parse_args()

    try:
        data = json.load(sys.stdin)
    except json.decoder.JSONDecodeError as err:
        print(misc.format_exception_stack(err), file=sys.stderr)
        return 1

    try:
        io.schema.validate(data)
    except jsonschema.exceptions.ValidationError as err:
        print(misc.format_exception_stack(err), file=sys.stderr)
        return 2
    return 0


def upgrade_main():
    """Execute the kcidb-upgrade command-line tool"""
    description = 'kcidb-upgrade - Upgrade I/O JSON data to latest schema'
    parser = misc.ArgumentParser(description=description)
    parser.parse_args()

    try:
        data = json.load(sys.stdin)
    except json.decoder.JSONDecodeError as err:
        print(misc.format_exception_stack(err), file=sys.stderr)
        return 1

    try:
        data = io.schema.upgrade(data, copy=False)
    except jsonschema.exceptions.ValidationError as err:
        print(misc.format_exception_stack(err), file=sys.stderr)
        return 2

    json.dump(data, sys.stdout, indent=4, sort_keys=True)
    return 0


def count_main():
    """Execute the kcidb-count command-line tool"""
    description = 'kcidb-count - Count number of objects in I/O JSON data'
    parser = misc.ArgumentParser(description=description)
    parser.parse_args()

    try:
        data = json.load(sys.stdin)
    except json.decoder.JSONDecodeError as err:
        print(misc.format_exception_stack(err), file=sys.stderr)
        return 1

    try:
        io.schema.validate(data)
    except jsonschema.exceptions.ValidationError as err:
        print(misc.format_exception_stack(err), file=sys.stderr)
        return 2

    print(io.get_obj_num(data))
    return 0


def summarize_main():
    """Execute the kcidb-summarize command-line tool"""
    description = 'kcidb-summarize - Output summaries of report objects'
    parser = misc.ArgumentParser(description=description)
    parser.add_argument(
        'obj_list_name',
        metavar='LIST',
        choices={n for n in io.schema.LATEST.tree if n},
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
    parser = misc.ArgumentParser(description=description)
    parser.add_argument(
        'obj_list_name',
        metavar='LIST',
        choices={n for n in io.schema.LATEST.tree if n},
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
            sys.stdout.write(obj_map[obj_id].describe())
            sys.stdout.write("\x00")
    return 0


def merge_main():
    """Execute the kcidb-merge command-line tool"""
    description = 'kcidb-merge - Upgrade and merge I/O data sets'
    parser = misc.ArgumentParser(description=description)
    parser.add_argument(
        'paths',
        metavar='JSON_FILE',
        nargs='*',
        default=[],
        help='Path to a JSON file with I/O data to merge'
    )
    args = parser.parse_args()

    sources = []
    for path in args.paths:
        try:
            with open(path, "r") as json_file:
                sources.append(io.schema.validate(json.load(json_file)))
        except json.decoder.JSONDecodeError as err:
            print(misc.format_exception_stack(err), file=sys.stderr)
            return 1
        except jsonschema.exceptions.ValidationError as err:
            print(misc.format_exception_stack(err), file=sys.stderr)
            return 2

    merged_data = io.merge(io.new(), sources,
                           copy_target=False, copy_sources=False)
    json.dump(merged_data, sys.stdout, indent=4, sort_keys=True)
    return 0


def notify_main():
    """Execute the kcidb-notify command-line tool"""
    description = 'kcidb-notify - Generate notifications for new I/O data'
    parser = misc.ArgumentParser(description=description)
    parser.add_argument(
        'new',
        metavar='NEW_FILE',
        help='Path to a JSON file with new I/O data'
    )
    parser.add_argument(
        'base',
        metavar='BASE_FILE',
        nargs='?',
        help='Path to a JSON file with base I/O data'
    )
    args = parser.parse_args()

    if args.base is None:
        base = io.new()
    else:
        try:
            with open(args.base, "r") as json_file:
                base = io.schema.validate(json.load(json_file))
        except (json.decoder.JSONDecodeError,
                jsonschema.exceptions.ValidationError) as err:
            print("Failed reading base file:", file=sys.stderr)
            print(misc.format_exception_stack(err), file=sys.stderr)
            return 1

    try:
        with open(args.new, "r") as json_file:
            new = io.schema.validate(json.load(json_file))
    except (json.decoder.JSONDecodeError,
            jsonschema.exceptions.ValidationError) as err:
        print("Failed reading new file:", file=sys.stderr)
        print(misc.format_exception_stack(err), file=sys.stderr)
        return 1

    for notification in subscriptions.match_new_io(base, new):
        sys.stdout.write(
            notification.render().as_string(policy=email.policy.SMTPUTF8)
        )
        sys.stdout.write("\x00")
    return 0
