"""
Kernel CI report database - Jinja2 templates
"""

import os
import jinja2


# Jinja2 template environment.
# Loads templates from the directory of this package.
ENV = jinja2.Environment(
    trim_blocks=True,
    keep_trailing_newline=True,
    lstrip_blocks=True,
    undefined=jinja2.StrictUndefined,
    loader=jinja2.FileSystemLoader(
        os.path.dirname(os.path.realpath(__file__))
    )
)
