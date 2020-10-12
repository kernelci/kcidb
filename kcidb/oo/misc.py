"""
Kernel CI report OO data - miscellaneous definitions
"""

import enum
import re
import os
import jinja2


@enum.unique
class Status(enum.IntEnum):
    """Report object status values, in order of severity"""
    # Unknown/missing status
    NONE = 1
    # The test wasn't executed,
    # the status of the tested code is unknown
    SKIP = 2
    # The test has finished successfully,
    # the status of the tested code is unknown
    DONE = 3
    # The test has passed,
    # the tested code is correct
    PASS = 4
    # The test has failed,
    # the tested code is faulty
    FAIL = 5
    # The test is faulty,
    # the status of the tested code is unknown
    ERROR = 6


@enum.unique
class Waived(enum.IntEnum):
    """Report object waived values, in order of severity"""
    # Unknown/missing waived
    UNKNOWN = 1
    # Waived
    TRUE = 2
    # Unwaived
    FALSE = 3


# Jinja2 environment for node summary and description templates
# Loads templates from the directory of kcidb.oo package.
TEMPLATE_ENV = jinja2.Environment(
    trim_blocks=True,
    keep_trailing_newline=True,
    lstrip_blocks=True,
    undefined=jinja2.StrictUndefined,
    loader=jinja2.FileSystemLoader(
        os.path.dirname(os.path.realpath(__file__))
    )
)

# A regular expression matching valid node summaries
SUMMARY_RE = re.compile(r"[^\x00-\x1f\x7f]*")


# pylint: disable=too-few-public-methods
class Node:
    """Object-oriented data node"""

    # The name of the variable to use to expose "self" to Jinja2 templates
    # None or empty string means templates cannot be rendered.
    _NAME = None

    # A Jinja2 template object generating a single-line string summarizing the
    # node. Will be provided the node in variable with _NAME name. Can be
    # None, signifying missing template.
    _SUMMARY_TEMPLATE = None

    # A Jinja2 template object generating a string with detailed description
    # of the node. Will be provided the node in variable with _NAME name. Can
    # be None, signifying missing template.
    _DESCRIPTION_TEMPLATE = None

    @staticmethod
    def _data_from_io(data):
        """
        Convert an I/O data node to OO representation.

        Args:
            data:   The I/O data node to convert

        Returns:
            The OO representation of the data.
        """
        if isinstance(data, dict):
            return Node(data, {})
        if isinstance(data, list):
            return [Node._data_from_io(v) for v in data]
        return data

    def __init__(self, data, attrs=None, converter_map=None):
        """
        Initialize an abstract report node object.

        Args:
            data:                   The I/O data for the report object.
            attrs:                  A dictionary of extra attributes to
                                    assign, or None for none.
            converter_map:          A dictionary of I/O data property names
                                    and converter callables to use for turning
                                    their values to OO representation. If a
                                    property has no entry in the dictionary,
                                    the Node class is used for dictionaries
                                    not named "misc", and everything else is
                                    left as is.
                                    The converter must accept the I/O property
                                    value as its sole argument and return its
                                    OO representation.
                                    The default is an empty dictionary.
        """
        assert isinstance(data, dict)
        assert attrs is None or isinstance(attrs, dict)
        if attrs is None:
            attrs = {}
        assert isinstance(converter_map, (type(None), dict))
        if converter_map is None:
            converter_map = {}
        assert all(isinstance(k, str) and callable(v)
                   for k, v in converter_map.items())
        # pylint: disable=invalid-name # Really pylint?
        for k, v in data.items():
            if k in converter_map:
                v = converter_map[k](v)
            elif k != "misc":
                v = Node._data_from_io(v)
            setattr(self, k, v)
        for k, v in attrs.items():
            setattr(self, k, v)

    def __getattr__(self, _):
        # Emulate missing JSON properties
        # pylint: disable=fixme
        # TODO: Instead, make all known properties attributes, and assign None
        #       to missing ones. Otherwise typos will be uncaught.
        #       Requires I/O schema introspection.
        return None

    def __repr__(self):
        return str(self.__dict__)

    def __eq__(self, other):
        """
        Check if the node is equal to another value.

        Args:
            other:  The value to compare to.

        Returns:
            True if the node is equal to the other value. False otherwise.
        """
        if not isinstance(other, type(self)):
            return False

        if set(self.__dict__.keys()) != set(self.__dict__.keys()):
            return False

        for k in self.__dict__:
            self_v = getattr(self, k)
            other_v = getattr(other, k)
            if k.endswith("_"):
                # Yes, we want to be simple and strict here
                # pylint: disable=unidiomatic-typecheck
                if type(self_v) != type(other_v):
                    return False
                if isinstance(self_v, list):
                    return \
                        len(self_v) == len(other_v) and \
                        all(
                            id(self_v[i]) == id(other_v[i])
                            for i in range(0, len(self_v))
                        )
                return id(self_v) == id(other_v)
            return self_v == other_v

    @staticmethod
    def _data_to_io(data):
        """
        Convert an OO data node to I/O representation.

        Args:
            data:   The OO data node to convert

        Returns:
            The I/O representation of the data.
        """
        if isinstance(data, Node):
            return data.to_io()
        if isinstance(data, list):
            return [Node._data_to_io(v) for v in data]
        return data

    def to_io(self):
        """
        Convert the OO data node to I/O data, without any of the linked
        parents or children.

        Returns:
            The I/O data for the node.
        """
        io_data = {}
        # pylint: disable=invalid-name
        for k, v in self.__dict__.items():
            if k[0] != "_" and k[-1] != "_":
                io_data[k] = Node._data_to_io(v)
        return io_data

    def summarize(self):
        """
        Generate a text summary of the data node.

        Returns:
            A single-line string summary of the data node.
        """
        if not self._NAME or self._SUMMARY_TEMPLATE is None:
            raise NotImplementedError
        summary = self._SUMMARY_TEMPLATE.render({
            self._NAME: self,
        })
        assert SUMMARY_RE.fullmatch(summary), \
            f"Summary is invalid: {repr(summary)}"
        return summary

    def describe(self):
        """
        Generate a detailed text description of the data node.

        Returns:
            A (multiline) string describing the data node in detail.
        """
        if not self._NAME or self._DESCRIPTION_TEMPLATE is None:
            raise NotImplementedError
        return self._DESCRIPTION_TEMPLATE.render({
            self._NAME: self,
        })
