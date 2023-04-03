"""Kernel CI reporting - misc definitions"""

import os
import atexit
import tempfile
import sys
import traceback
import itertools
import logging
import json
from textwrap import indent
from google.cloud import secretmanager
import jq

# Module's logger
LOGGER = logging.getLogger(__name__)

# A dictionary of names of logging levels and their values
LOGGING_LEVEL_MAP = {
    name: value
    for name, value in logging.__dict__.items()
    if name.isalpha() and name.isupper() and isinstance(value, int) and value
}
# Logging level disabling all logging
LOGGING_LEVEL_MAP["NONE"] = max(LOGGING_LEVEL_MAP.values()) + 1
# Sort levels highest->lowest
# I don't see it, pylint: disable=unnecessary-comprehension
LOGGING_LEVEL_MAP = {
    k: v
    for k, v in sorted(LOGGING_LEVEL_MAP.items(),
                       key=lambda i: i[1], reverse=True)
}

# Check light assertions only, if True
LIGHT_ASSERTS = not os.environ.get("KCIDB_HEAVY_ASSERTS", "")


def logging_setup(level):
    """
    Setup logging: set root logger log level and disable irrelevant logging.

    Args:
        level:  Logging level for the root logger.
    """
    assert isinstance(level, int)
    logging.getLogger().setLevel(level)
    # TODO Consider separate arguments for controlling the below
    logging.getLogger("urllib3").setLevel(LOGGING_LEVEL_MAP["NONE"])
    logging.getLogger("google").setLevel(LOGGING_LEVEL_MAP["NONE"])


def format_exception_stack(exc):
    """
    Format an exception's context stack as a series of indented messages.

    Args:
        exc:    The exception to format the stack of.

    Returns:
        The formatted exception stack.
    """
    assert isinstance(exc, Exception)
    string = ""
    prefix = ""
    while True:
        summary = ": ".join(s for s in (type(exc).__name__, str(exc)) if s)
        string += indent(summary, prefix)
        if getattr(exc, "__suppress_context__", False):
            exc = exc.__cause__
        else:
            exc = exc.__context__
        if exc:
            string += ":\n"
            prefix += "  "
        else:
            break
    return string


def log_and_print_excepthook(type, value, tb):
    """
    Log an exception with DEBUG level and print its summary to stderr.
    Adheres to sys.excepthook interface.

    Args:
        type:   Exception class.
        value:  Exception instance.
        tb:     Exception traceback object.
    """
    # "tb" is OK, pylint: disable=invalid-name
    lines = traceback.format_exception(type, value, tb)
    LOGGER.debug("%s", "".join(lines).rstrip())
    print(format_exception_stack(value), file=sys.stderr)


def json_load_stream_fd(stream_fd, chunk_size=4*1024*1024):
    """
    Load a series of JSON values from a stream file descriptor.

    Args:
        stream_fd:  The file descriptor for the stream to read.
        chunk_size: Maximum size of chunks to read from the file, bytes.

    Returns:
        An iterator returning loaded JSON values.
    """
    def read_chunk():
        while True:
            chunk = os.read(stream_fd, chunk_size)
            if chunk:
                yield chunk
            else:
                break

    return jq.parse_json(text_iter=read_chunk())


# It's OK, pylint: disable=redefined-outer-name
def json_dump(value, fp, indent=0, seq=False):
    """
    Dump a JSON value to a file, followed by a newline.

    Args:
        value:  The JSON value to dump.
        fp:     The file-like object to output to.
        indent: Number of indent spaces for pretty-printing, or zero to
                disable pretty-printing and dump the value single-line.
        seq:    Prefix the value with an RS character, to make output comply
                with RFC 7464 and the "application/json-seq" media type.
    """
    # "fp" is OK, pylint: disable=invalid-name
    if seq:
        fp.write("\x1e")
    json.dump(value, fp, indent=indent or None)
    fp.write("\n")


def json_dump_stream(value_iter, fp, indent=0, seq=False):
    """
    Dump a series of JSON values to a file, each followed by a newline.

    Args:
        value_iter:     An iterator returning the JSON values to dump.
        fp:             The file-like object to output to.
        indent:         Number of indent spaces for pretty-printing,
                        or zero to disable pretty-printing and dump values
                        single-line.
        seq:            Prefix each value with an RS character, to make output
                        comply with RFC 7464 and the "application/json-seq"
                        media type.
    """
    # "fp" is OK, pylint: disable=invalid-name
    for value in value_iter:
        json_dump(value, fp, indent=indent, seq=seq)
        fp.flush()


def get_secret(project_id, secret_id):
    """
    Get the latest version of a secret from Google Secret Manager.

    Args:
        project_id: The ID of the Google Cloud project to fetch secrets from.
        secret_id:  The ID of the secret to fetch latest version of.

    Returns:
        The latest version of the secret.
    """
    assert isinstance(project_id, str) and project_id
    assert isinstance(secret_id, str) and secret_id
    client = secretmanager.SecretManagerServiceClient()
    path = client.secret_version_path(project_id, secret_id, "latest")
    return client.access_secret_version(
        request={"name": path}
    ).payload.data.decode()


def get_secret_pgpass(project_id, secret_id):
    """
    Get the latest version of a secret containing a PostgreSQL's .pgpass file
    from Google Secret Manager. Store that secret in a temporary file and
    expose it via the PGPASSFILE environment variable for use by libpq-based
    clients.

    Args:
        project_id: The ID of the Google Cloud project to fetch secrets from.
        secret_id:  The ID of the secret to fetch latest version of.
    """
    assert isinstance(project_id, str) and project_id
    assert isinstance(secret_id, str) and secret_id
    pgpass = get_secret(project_id, secret_id)
    (pgpass_fd, pgpass_filename) = tempfile.mkstemp(suffix=".pgpass")
    with os.fdopen(pgpass_fd, mode="w", encoding="utf-8") as pgpass_file:
        pgpass_file.write(pgpass)
    os.environ["PGPASSFILE"] = pgpass_filename
    atexit.register(os.remove, pgpass_filename)


def merge_dicts(*args, **kwargs):
    """
    Merge dictionaries together.

    Args:
        args:   The list of dictionaries to merge together.
        kwargs: The last dictionary to merge on top of the result.

    Returns:
        The merged dictionary.
    """
    assert all(isinstance(arg, dict) for arg in args)
    args = [*args, kwargs]
    result = {}
    for arg in args:
        result.update(arg)
    return result


def isliced(iterable, size):
    """
    Create a generator yielding iterables of specified maximum number of
    elements from an iterable.

    Args:
        iterable:   The iterable to return elements from.
        size:       Maximum number of elements in each tuple (a positive
                    integer), or zero to have the original iterable yielded.
    """
    assert isinstance(size, int) and size >= 0
    if size == 0:
        yield iterable
        return
    iterator = iter(iterable)
    while True:
        iterator_slice = tuple(itertools.islice(iterator, size))
        if not iterator_slice:
            break
        yield iterator_slice
