"""Kernel CI reporting test catalog schema"""

import jsonschema

# JSON schema for a test catalog
JSON = {
    "description":
        "A catalog of tests recognized by KCIDB",
    "type": "object",
    "patternProperties": {
        "^[a-zA-Z0-9_-]+$": {
            "type": "object",
            "description": "A test: a test program or a collection thereof",
            "properties": {
                "title": {
                    "type": "string",
                    # Enforce single-line strings
                    "pattern": "^[^\x00-\x1f]*$",
                    "description":
                        "A human-oriented summary of the test"
                },
                "description": {
                    "type": "string",
                    "description":
                        "An optional longer description of the test"
                },
                "home": {
                    "type": "string",
                    "format": "uri",
                    "description":
                        "A URL pointing to the test's home page"
                }
            },
            "additionalProperties": False,
            "required": [
                "title",
                "home"
            ]
        }
    },
    "additionalProperties": False
}


def validate(catalog):
    """
    Validate a catalog with its schema.

    Args:
        catalog:    The catalog validate.

    Return:
        The validated catalog.

    Raises:
        `jsonschema.exceptions.ValidationError` if the catalog
            is invalid
    """
    try:
        format_checker = jsonschema.Draft7Validator.FORMAT_CHECKER
    except AttributeError:
        # Nevermind, pylint: disable=fixme
        # TODO Remove once we stop supporting Python 3.6
        format_checker = jsonschema.draft7_format_checker

    jsonschema.validate(instance=catalog, schema=JSON,
                        format_checker=format_checker)
    return catalog
