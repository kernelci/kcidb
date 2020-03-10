"""
Kernel CI report OO data - revision definitions
"""

from kcidb.oo import misc


class Node(misc.Node):
    """Object-oriented representation of a revision"""

    _NAME = "revision"

    _SUMMARY_TEMPLATE = \
        misc.TEMPLATE_ENV.get_template("revision_summary.txt.j2")

    _DESCRIPTION_TEMPLATE = \
        misc.TEMPLATE_ENV.get_template("revision_description.txt.j2")

    def __init__(self, data, attrs=None):
        """
        Initialize a revision object.

        Args:
            data:   The I/O data for the report object.
            attrs:  A dictionary of extra attributes to assign,
                    or None for none.
        """
        assert isinstance(data, dict)
        assert attrs is None or isinstance(attrs, dict)
        super().__init__(data, attrs)
