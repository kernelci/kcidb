"""Kernel CI reporting"""

import json
import sys
import email
import logging
import jsonschema
import jq
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
    "merge_main",
    "count_main",
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

    # We can live with this for now, pylint: disable=too-many-arguments
    def query_iter(self, ids=None, patterns=None,
                   children=False, parents=False,
                   objects_per_report=0):
        """
        Match and fetch reports, in object number-limited chunks.

        Args:
            ids:                A dictionary of object list names, and lists
                                of IDs of objects to match. None means empty
                                dictionary.
            patterns:           A dictionary of object list names, and lists
                                of LIKE patterns, for IDs of objects to match.
                                None means empty dictionary.
            children:           True if children of matched objects should be
                                matched as well.
            parents:            True if parents of matched objects should be
                                matched as well.
            objects_per_report: A positive integer number of objects per each
                                returned report, or zero for no limit.

        Returns:
            An iterator returning report JSON data adhering to the latest I/O
            schema version, each containing at most the specified number of
            objects.

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

        assert isinstance(objects_per_report, int)
        assert objects_per_report >= 0

        if not self.db_client:
            raise NotImplementedError

        return self.db_client.query_iter(ids, patterns, children, parents,
                                         objects_per_report)

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
    sys.excepthook = misc.log_and_print_excepthook
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
    client = Client(project_id=args.project, topic_name=args.topic)
    for data in misc.json_load_stream_fd(sys.stdin.fileno()):
        data = io.schema.upgrade(data, copy=False)
        client.submit(data)


def query_main():
    """Execute the kcidb-query command-line tool"""
    sys.excepthook = misc.log_and_print_excepthook
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
    json.dump(data, sys.stdout, indent=4)


def schema_main():
    """Execute the kcidb-schema command-line tool"""
    sys.excepthook = misc.log_and_print_excepthook
    description = 'kcidb-schema - Output latest I/O JSON schema'
    parser = misc.ArgumentParser(description=description)
    parser.parse_args()
    json.dump(io.schema.LATEST.json, sys.stdout, indent=4)


def validate_main():
    """Execute the kcidb-validate command-line tool"""
    sys.excepthook = misc.log_and_print_excepthook
    description = 'kcidb-validate - Validate I/O JSON data'
    parser = misc.ArgumentParser(description=description)
    parser.parse_args()

    for data in misc.json_load_stream_fd(sys.stdin.fileno()):
        io.schema.validate(data)


def upgrade_main():
    """Execute the kcidb-upgrade command-line tool"""
    sys.excepthook = misc.log_and_print_excepthook
    description = 'kcidb-upgrade - Upgrade I/O JSON data to latest schema'
    parser = misc.ArgumentParser(description=description)
    parser.parse_args()

    for data in misc.json_load_stream_fd(sys.stdin.fileno()):
        data = io.schema.upgrade(data, copy=False)
        json.dump(data, sys.stdout, indent=4)


def count_main():
    """Execute the kcidb-count command-line tool"""
    sys.excepthook = misc.log_and_print_excepthook
    description = 'kcidb-count - Count number of objects in I/O JSON data'
    parser = misc.ArgumentParser(description=description)
    parser.parse_args()

    for data in misc.json_load_stream_fd(sys.stdin.fileno()):
        io.schema.validate(data)
        print(io.get_obj_num(data))


def summarize_main():
    """Execute the kcidb-summarize command-line tool"""
    sys.excepthook = misc.log_and_print_excepthook
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
    for io_data in misc.json_load_stream_fd(sys.stdin.fileno()):
        oo_data = oo.from_io(io.schema.upgrade(io_data, copy=False))
        obj_map = oo_data.get(args.obj_list_name, {})
        for obj_id in args.ids or obj_map:
            if obj_id in obj_map:
                print(obj_map[obj_id].summarize())


def describe_main():
    """Execute the kcidb-describe command-line tool"""
    sys.excepthook = misc.log_and_print_excepthook
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
    for io_data in misc.json_load_stream_fd(sys.stdin.fileno()):
        oo_data = oo.from_io(io.schema.upgrade(io_data, copy=False))
        obj_map = oo_data.get(args.obj_list_name, {})
        for obj_id in args.ids or obj_map:
            if obj_id in obj_map:
                sys.stdout.write(obj_map[obj_id].describe())
                sys.stdout.write("\x00")


def merge_main():
    """Execute the kcidb-merge command-line tool"""
    sys.excepthook = misc.log_and_print_excepthook
    description = 'kcidb-merge - Upgrade and merge I/O data sets'
    parser = misc.ArgumentParser(description=description)
    parser.parse_args()

    sources = [
        io.schema.validate(data)
        for data in misc.json_load_stream_fd(sys.stdin.fileno())
    ]
    merged_data = io.merge(io.new(), sources,
                           copy_target=False, copy_sources=False)
    json.dump(merged_data, sys.stdout, indent=4)


def notify_main():
    """Execute the kcidb-notify command-line tool"""
    sys.excepthook = misc.log_and_print_excepthook
    description = 'kcidb-notify - Generate notifications for new I/O data'
    parser = misc.ArgumentParser(description=description)
    parser.add_argument(
        'base',
        metavar='BASE_FILE',
        nargs='?',
        help='Path to a JSON file with base I/O data'
    )
    args = parser.parse_args()

    base = io.new()
    if args.base is not None:
        try:
            with open(args.base, "r") as json_file:
                base_reports = [
                    io.schema.validate(data)
                    for data in misc.json_load_stream_fd(json_file.fileno())
                ]
                base = io.merge(base, base_reports,
                                copy_target=False, copy_sources=False)
        except (jq.JSONParseError,
                jsonschema.exceptions.ValidationError) as err:
            raise Exception("Failed reading base file") from err

    try:
        for new in misc.json_load_stream_fd(sys.stdin.fileno()):
            new = io.schema.validate(new)
            for notification in subscriptions.match_new_io(base, new):
                sys.stdout.write(
                    notification.render().
                    as_string(policy=email.policy.SMTPUTF8)
                )
                sys.stdout.write("\x00")
            base = io.merge(base, [new],
                            copy_target=False, copy_sources=False)
    except (jq.JSONParseError,
            jsonschema.exceptions.ValidationError) as err:
        raise Exception("Failed reading new I/O data") from err
