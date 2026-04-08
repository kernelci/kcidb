"""Kernel CI reporting"""

import sys
import email
import logging
import os
import re
import concurrent.futures
import time

import requests
from kcidb.misc import LIGHT_ASSERTS
# Silence flake8 "imported but unused" warning
from kcidb import io, db, orm, oo, monitor, tests, unittest, misc # noqa


# Module's logger
LOGGER = logging.getLogger(__name__)


class DatabaseNotInitialized(Exception):
    """Database is not initialized exception"""
    def __init__(self):
        super().__init__("Database is not initialized")


class Client:
    """Kernel CI reporting client
    Regex for validating REST URI format
    http(s)://[token@]host[:port][/path]
    Token may be omitted for local endpoints (e.g. localhost).
    """
    REST_REGEX = re.compile(
        r'^(?:(?P<scheme>https?)://)?'
        r'(?:(?P<token>[^@]+)@)?'
        r'(?P<host>[^:/]+)'
        r'(?::(?P<port>\d+))?'
        r'(/.*)?$',
    )
    LOCAL_REST_HOSTS = {"localhost", "127.0.0.1", "0.0.0.0"}

    def __init__(self, database=None, rest_uri=None, max_workers=10):
        """
        Initialize a reporting client

        Args:
            database:       The database specification string to use for
                            accessing the report database.
                            Can be None to have querying disabled.
            rest_uri:       REST URI to use for submissions. If not provided,
                            the KCIDB_REST environment variable is used.
            max_workers:    Maximum number of worker threads for concurrent
                            submissions. Defaults to 10.

        Raises:
            `kcidb.DatabaseNotInitialized` if the database is not
            initialized.
            `kcidb.db.IncompatibleSchema` if the database schema
            is incompatible with the current I/O schema.
        """
        # Store max_workers for executor configuration
        assert isinstance(max_workers, int) and max_workers > 0
        self._max_workers = max_workers

        # verify if environment have KCIDB_REST variable
        rest = rest_uri or os.environ.get("KCIDB_REST")
        if rest:
            if not isinstance(rest, str) or not rest:
                raise ValueError("KCIDB_REST must be a non-empty string")
            if not self.validate_rest_uri(rest):
                raise ValueError("KCIDB_REST must be a valid URI")
            self._resturi = rest
        else:
            self._resturi = None
        assert database is None or isinstance(database, str) and database
        if database is None:
            self.db_client = None
        else:
            self.db_client = db.Client(database)
            if not self.db_client.is_initialized():
                raise DatabaseNotInitialized()
        self._executor = None

    @property
    def executor(self):
        """Get or create the thread pool executor for REST submissions."""
        if self._executor is None:
            self._executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=self._max_workers
            )
        return self._executor

    def __del__(self):
        """Cleanup executor on object destruction."""
        if hasattr(self, '_executor') and self._executor is not None:
            self._executor.shutdown(wait=True)

    def validate_rest_uri(self, uri):
        """
        Validate the REST URI format.
        http(s)://[token@]host[:port][/path]

        Args:
            uri: The URI to validate.

        Returns:
            True if the URI is valid, False otherwise.
        """
        rest_regex = self.REST_REGEX
        match = rest_regex.match(uri)
        if match:
            host = match.group('host')
            port = match.group('port')
            scheme = match.group('scheme')
            token = match.group('token')
            if scheme and scheme not in ['http', 'https']:
                return False
            if not token and host not in self.LOCAL_REST_HOSTS:
                return False
            if port and not re.match(r'^\d+$', port):
                return False
            if host and not re.match(r'^[a-zA-Z0-9.-]+$', host):
                return False
            return True
        return False

    def _rest_submit_with_retry(self, data, max_retries=3):
        """
        Submit data with retry logic for transient errors.

        Args:
            data: JSON data to submit
            max_retries: Maximum number of retry attempts

        Returns:
            Submission ID string
        """
        last_error = None

        for attempt in range(max_retries):
            try:
                return self.rest_submit(data)
            except requests.exceptions.Timeout as e:
                last_error = e
                if attempt < max_retries - 1:
                    LOGGER.warning(
                        "Request timeout, retrying (%s/%s)...",
                        attempt + 1,
                        max_retries,
                    )
                    time.sleep(2 ** attempt)  # Exponential backoff
                    continue
            except requests.exceptions.ConnectionError as e:
                last_error = e
                if attempt < max_retries - 1:
                    LOGGER.warning(
                        "Connection error, retrying (%s/%s)...",
                        attempt + 1,
                        max_retries,
                    )
                    time.sleep(2 ** attempt)
                    continue
            except requests.exceptions.RequestException as e:
                # Check if it's a server error (5xx)
                if hasattr(e, 'response') and e.response is not None:
                    is_server_error = e.response.status_code >= 500
                    should_retry = attempt < max_retries - 1
                    if is_server_error and should_retry:
                        last_error = e
                        LOGGER.warning(
                            "Server error %s, retrying (%s/%s)...",
                            e.response.status_code,
                            attempt + 1,
                            max_retries,
                        )
                        time.sleep(2 ** attempt)
                        continue
                # For client errors or other issues, don't retry
                raise e

        # If we've exhausted all retries, raise the last error
        if last_error:
            raise last_error
        raise RuntimeError("Retry attempts exhausted")

    def rest_submit(self, data):
        """Submit reports over REST API.
        Args:
            data:   A JSON object with the report data to submit.
                    Must adhere to the current, or an earlier version of I/O
                    schema. Note that this function will not validate the
                    submitted data.
        Returns:
            Submission ID string.

        Uses the environment variable KCIDB_REST to get the REST URI
        and credentials.
        """
        LOGGER.debug("Submitting report over REST API")
        vre = self.REST_REGEX
        match = vre.match(self._resturi)
        if match:
            host = match.group('host')
            port = match.group('port')
            scheme = match.group('scheme') or 'https'
            token = match.group('token')
            url = None
            if scheme not in ['http', 'https']:
                raise ValueError("Invalid scheme in REST URI")
            if not port:
                url = f"{scheme}://{host}/submit"
            else:
                url = f"{scheme}://{host}:{port}/submit"
            headers = {'Content-Type': 'application/json'}
            # add token
            if token:
                headers['Authorization'] = f"Bearer {token}"
            try:
                response = requests.post(url, json=data,
                                         headers=headers,
                                         timeout=60)
            except requests.exceptions.RequestException as e:
                raise e

            if response.status_code == 200:
                try:
                    submission_id = response.json().get('id')
                    if not submission_id:
                        raise ValueError("No submission ID in response")
                    return submission_id
                except ValueError as ve:
                    raise requests.exceptions.RequestException(
                        f"Error parsing response: {ve}"
                    )
                except Exception as e:
                    raise requests.exceptions.RequestException(
                        f"Error parsing response: {e}"
                    )
            raise requests.exceptions.RequestException(
                f"Error submitting report: {response.status_code} "
                f"{response.text}"
            )
        raise ValueError("Invalid REST URI format")

    def submit(self, data):
        """
        Submit reports.

        Args:
            data:   A JSON object with the report data to submit.
                    Must adhere to the current, or an earlier version of I/O
                    schema. Note that this function will not validate the
                    submitted data.

        Returns:
            Submission ID string.

        Raises:
            `NotImplementedError`, if REST submission is not configured.
        """
        assert io.SCHEMA.is_compatible(data)
        assert LIGHT_ASSERTS or io.SCHEMA.is_valid(data)
        if not self._resturi:
            raise NotImplementedError
        return self.rest_submit(data)

    def future_submit(self, data):
        """
        Submit reports without blocking for the interaction with the
        database.

        Args:
            data:   A JSON object with the report data to submit.
                    Must adhere to the current, or an earlier version of I/O
                    schema. Note that this function will not validate the
                    submitted data.

        Returns:
            A future which will return the Submission ID string.

        Raises:
            `NotImplementedError`, if REST submission is not configured.
        """
        assert io.SCHEMA.is_compatible(data)
        assert LIGHT_ASSERTS or io.SCHEMA.is_valid(data)
        if not self._resturi:
            raise NotImplementedError
        # Submit the REST request with retry using shared executor
        future = self.executor.submit(self._rest_submit_with_retry, data)
        return future

    def submit_iter(self, data_iter, done_cb=None):
        """
        Submit reports returned by an iterator using parallel execution.

        Args:
            data_iter:  An iterator returning the JSON report data to submit.
                        Each must adhere to the current, or an earlier version
                        of I/O schema.
            done_cb:    A function to call when a report is successfully
                        submitted. Will be called with the submission ID of
                        each report returned by the iterator, in order.

        Raises:
            `NotImplementedError`, if REST submission is not configured.
        """
        if not self._resturi:
            raise NotImplementedError
        # Convert iterator to list to preserve order for done_cb
        data_list = list(data_iter)
        submission_results = []

        # Submit all tasks with retry logic using shared executor
        future_to_data = {
            self.executor.submit(
                self._rest_submit_with_retry,
                data,
            ): (idx, data)
            for idx, data in enumerate(data_list)
        }

        # Process completed futures in order
        errors = []
        for future in concurrent.futures.as_completed(future_to_data):
            idx, data = future_to_data[future]
            try:
                submission_id = future.result()
                submission_results.append((idx, submission_id, None))
            except Exception as e:  # pylint: disable=broad-exception-caught
                LOGGER.error("Error submitting report: %s", e)
                submission_results.append((idx, None, e))
                errors.append(e)

        # Call done_cb in original order if provided
        if done_cb:
            submission_results.sort(key=lambda x: x[0])
            for idx, submission_id, error in submission_results:
                if submission_id and not error:
                    done_cb(submission_id)
        if errors:
            if len(errors) == 1:
                raise errors[0]
            raise RuntimeError(
                f"{len(errors)} submissions failed"
            ) from errors[0]

    # We can live with this for now, pylint: disable=too-many-arguments
    # Or if you prefer, pylint: disable=too-many-positional-arguments
    def query_iter(self, ids=None,
                   children=False, parents=False,
                   objects_per_report=0, with_metadata=False):
        """
        Match and fetch reports, in object number-limited chunks.

        Args:
            ids:                A dictionary of object list names, and lists
                                of IDs of objects to match. None means empty
                                dictionary. Each ID is either a tuple of
                                values or a single value (equivalent to a
                                single-value tuple). The values should match
                                the types, the order, and the number of the
                                object's ID fields as described by the
                                database's I/O schema (the "id_fields"
                                attribute).
            children:           True if children of matched objects should be
                                matched as well.
            parents:            True if parents of matched objects should be
                                matched as well.
            objects_per_report: A positive integer number of objects per each
                                returned report, or zero for no limit.
            with_metadata:      True, if metadata fields should be fetched as
                                well. False, if not.

        Returns:
            An iterator returning report JSON data adhering to the current I/O
            schema version, each containing at most the specified number of
            objects.

        Raises:
            `NotImplementedError`, if not supplied with a dataset name at
            initialization time;
        """
        if not self.db_client:
            raise NotImplementedError

        assert self.db_client.query_ids_are_valid(ids)
        assert isinstance(objects_per_report, int)
        assert objects_per_report >= 0
        assert isinstance(with_metadata, bool)

        return self.db_client.query_iter(ids=ids,
                                         children=children, parents=parents,
                                         objects_per_report=objects_per_report,
                                         with_metadata=with_metadata)

    def query(self, ids=None, children=False, parents=False,
              with_metadata=False):
        """
        Match and fetch report objects.

        Args:
            ids:            A dictionary of object list names, and lists of
                            IDs of objects to match. None means empty
                            dictionary. Each ID is either a tuple of values or
                            a single value (equivalent to a single-value
                            tuple). The values should match the types, the
                            order, and the number of the object's ID fields as
                            described by the database's I/O schema (the
                            "id_fields" attribute).
            children:       True if children of matched objects should be
                            matched as well.
            parents:        True if parents of matched objects should be
                            matched as well.
            with_metadata:  True, if metadata fields should be fetched as
                            well. False, if not.

        Returns:
            The fetched JSON data adhering to the database I/O schema
            (current, or an earlier I/O schema).

        Raises:
            `NotImplementedError`, if not supplied with a dataset name at
            initialization time;
        """
        if not self.db_client:
            raise NotImplementedError
        assert self.db_client.query_ids_are_valid(ids)
        assert isinstance(with_metadata, bool)

        return self.db_client.query(ids=ids,
                                    children=children, parents=parents,
                                    with_metadata=with_metadata)


def submit_main():
    """Execute the kcidb-submit command-line tool"""
    sys.excepthook = misc.log_and_print_excepthook
    description = \
        'kcidb-submit - Submit Kernel CI reports over REST, print IDs'
    parser = misc.InputArgumentParser(description=description)
    args = parser.parse_args()
    if not os.environ.get("KCIDB_REST"):
        parser.error("KCIDB_REST environment variable is required")
    client = Client()

    def print_submission_id(submission_id):
        print(submission_id, file=sys.stdout)
        sys.stdout.flush()

    client.submit_iter(
        (io.SCHEMA.validate(data)
         for data in
         misc.json_load_stream_fd(sys.stdin.fileno(), seq=args.seq_in)),
        done_cb=print_submission_id
    )


def query_main():
    """Execute the kcidb-query command-line tool"""
    sys.excepthook = misc.log_and_print_excepthook
    description = \
        "kcidb-query - Query Kernel CI reports"
    parser = db.QueryArgumentParser(description=description)
    args = parser.parse_args()
    client = Client(database=args.database)
    query_iter = client.query_iter(
        ids=dict(checkouts=args.checkout_ids,
                 builds=args.build_ids,
                 tests=args.test_ids,
                 issues=list(zip(args.issue_ids, args.issue_versions)),
                 incidents=args.incident_ids),
        parents=args.parents,
        children=args.children,
        objects_per_report=args.objects_per_report,
        with_metadata=args.with_metadata
    )
    misc.json_dump_stream(
        query_iter, sys.stdout, indent=args.indent, seq=args.seq_out
    )


def schema_main():
    """Execute the kcidb-schema command-line tool"""
    sys.excepthook = misc.log_and_print_excepthook
    description = 'kcidb-schema - Output current or older I/O JSON schema'
    parser = misc.OutputArgumentParser(description=description)
    misc.argparse_schema_add_args(parser, "output")
    args = parser.parse_args()
    misc.json_dump(args.schema_version.json, sys.stdout, indent=args.indent,
                   seq=args.seq_out)


def validate_main():
    """Execute the kcidb-validate command-line tool"""
    sys.excepthook = misc.log_and_print_excepthook
    description = 'kcidb-validate - Validate I/O JSON data'
    parser = misc.InputOutputArgumentParser(description=description)
    misc.argparse_schema_add_args(parser, "validate against")
    args = parser.parse_args()
    misc.json_dump_stream(
        (
            args.schema_version.validate(data)
            for data in misc.json_load_stream_fd(sys.stdin.fileno(),
                                                 seq=args.seq_in)
        ),
        sys.stdout, indent=args.indent, seq=args.seq_out
    )


def upgrade_main():
    """Execute the kcidb-upgrade command-line tool"""
    sys.excepthook = misc.log_and_print_excepthook
    description = 'kcidb-upgrade - Upgrade I/O JSON data to current schema'
    parser = misc.InputOutputArgumentParser(description=description)
    misc.argparse_schema_add_args(parser, "upgrade")
    args = parser.parse_args()
    misc.json_dump_stream(
        (
            args.schema_version.upgrade(io.SCHEMA.validate(data), copy=False)
            for data in
            misc.json_load_stream_fd(sys.stdin.fileno(), seq=args.seq_in)
        ),
        sys.stdout, indent=args.indent, seq=args.seq_out
    )


def count_main():
    """Execute the kcidb-count command-line tool"""
    sys.excepthook = misc.log_and_print_excepthook
    description = 'kcidb-count - Count number of objects in I/O JSON data'
    parser = misc.InputArgumentParser(description=description)
    args = parser.parse_args()

    for data in misc.json_load_stream_fd(sys.stdin.fileno(), seq=args.seq_in):
        print(io.SCHEMA.count(io.SCHEMA.validate(data)), file=sys.stdout)
        sys.stdout.flush()


def merge_main():
    """Execute the kcidb-merge command-line tool"""
    sys.excepthook = misc.log_and_print_excepthook
    description = 'kcidb-merge - Upgrade and merge I/O data sets'
    parser = misc.InputOutputArgumentParser(description=description)
    args = parser.parse_args()

    sources = [
        io.SCHEMA.validate(data)
        for data in
        misc.json_load_stream_fd(sys.stdin.fileno(), seq=args.seq_in)
    ]
    target_schema = max(
        (io.SCHEMA.get_exactly_compatible(s) for s in sources),
        default=io.SCHEMA
    )
    merged_data = target_schema.merge(target_schema.new(), sources,
                                      copy_target=False, copy_sources=False)
    misc.json_dump(merged_data, sys.stdout, indent=args.indent,
                   seq=args.seq_out)


def notify_main():
    """Execute the kcidb-notify command-line tool"""
    sys.excepthook = misc.log_and_print_excepthook
    description = 'kcidb-notify - Generate notifications for specified objects'
    parser = oo.ArgumentParser(database="json", description=description)
    args = parser.parse_args()
    oo_client = oo.Client(db.Client(args.database))
    pattern_set = set()
    for pattern_string in args.pattern_strings:
        pattern_set |= orm.query.Pattern.parse(pattern_string)
    for notification in monitor.match(oo_client.query(pattern_set)):
        sys.stdout.write(
            notification.render().
            as_string(policy=email.policy.SMTPUTF8)
        )
        sys.stdout.write("\x00")
        sys.stdout.flush()


def ingest_main():
    """Execute the kcidb-ingest command-line tool"""
    sys.excepthook = misc.log_and_print_excepthook
    description = 'kcidb-ingest - Load data into a (new) database and ' \
        'generate notifications for new and modified objects'
    parser = db.InputArgumentParser(database="sqlite::memory:",
                                    description=description)
    args = parser.parse_args()

    db_client = db.Client(args.database)
    if not db_client.is_initialized():
        db_client.init()
    oo_client = oo.Client(db_client, sort=True)
    io_schema = db_client.get_schema()[1]

    # For each JSON object in stdin
    for data in misc.json_load_stream_fd(sys.stdin.fileno(), seq=args.seq_in):
        # Validate and upgrade the data to the database's I/O schema
        data = io_schema.upgrade(io_schema.validate(data), copy=False)
        # Load into the database
        db_client.load(data)
        # Possibly upgrade the data further, to be compatible with ORM
        data = io.SCHEMA.upgrade(data, copy=False)
        # Record patterns matching the loaded objects and all their parents
        pattern_set = set()
        for pattern in orm.query.Pattern.from_io(data):
            # TODO Avoid formatting and parsing
            pattern_set |= orm.query.Pattern.parse(repr(pattern) + "<*#")
        LOGGER.debug("Notification patterns: %r", pattern_set)
        # Reset the OO cache
        oo_client.reset_cache()
        # Generate notifications for objects matching the patterns
        for notification in monitor.match(oo_client.query(pattern_set)):
            sys.stdout.write(
                notification.render().
                as_string(policy=email.policy.SMTPUTF8)
            )
            sys.stdout.write("\x00")
            sys.stdout.flush()
