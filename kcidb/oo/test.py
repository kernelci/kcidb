"""
Kernel CI report OO data - test run definitions
"""

import json
from kcidb.oo import misc
from kcidb.templates import ENV as TEMPLATE_ENV


class NodeEnvironment(misc.Node):
    """Object-oriented representation of a test environment"""

    def __init__(self, data, attrs=None):
        """
        Initialize a test environment object.

        Args:
            data:   The I/O data for the report object.
            attrs:  A dictionary of extra attributes to assign,
                    or None for none.
        """
        assert isinstance(data, dict)
        assert attrs is None or isinstance(attrs, dict)
        super().__init__(data, attrs)
        self.id_ = json.dumps(data, sort_keys=True)
        self._hash = hash(self.id_)

    def __hash__(self):
        return self._hash


class Node(misc.Node):
    """Object-oriented representation of a test run"""

    _NAME = "test"

    _SUMMARY_TEMPLATE = \
        TEMPLATE_ENV.get_template("test_summary.txt.j2")

    _DESCRIPTION_TEMPLATE = \
        TEMPLATE_ENV.get_template("test_description.txt.j2")

    def __init__(self, data, attrs=None):
        """
        Initialize a test run object.

        Args:
            data:   The I/O data for the report object.
            attrs:  A dictionary of extra attributes to assign,
                    or None for none.
        """
        assert isinstance(data, dict)
        assert attrs is None or isinstance(attrs, dict)
        super().__init__(data, attrs,
                         converter_map=dict(environment=NodeEnvironment))
        # Status value which can be compared by severity
        self.status_ = misc.Status[
            "NONE" if self.status is None else self.status
        ]
        # Waived value which can be compared by severity
        self.waived_ = misc.Waived[
            "FALSE" if self.waived is False else
            "TRUE" if self.waived is True else
            "UNKNOWN"
        ]
