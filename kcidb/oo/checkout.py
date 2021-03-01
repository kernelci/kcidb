"""
Kernel CI report OO data - checkout definitions
"""

from kcidb.oo import misc
from kcidb.templates import ENV as TEMPLATE_ENV


class Node(misc.Node):
    """Object-oriented representation of a checkout"""

    _NAME = "checkout"

    _SUMMARY_TEMPLATE = \
        TEMPLATE_ENV.get_template("checkout_summary.txt.j2")

    _DESCRIPTION_TEMPLATE = \
        TEMPLATE_ENV.get_template("checkout_description.txt.j2")

    def __init__(self, data, attrs=None):
        """
        Initialize a checkout object.

        Args:
            data:   The I/O data for the report object.
            attrs:  A dictionary of extra attributes to assign,
                    or None for none.
        """
        assert isinstance(data, dict)
        assert attrs is None or isinstance(attrs, dict)
        super().__init__(data, attrs)
