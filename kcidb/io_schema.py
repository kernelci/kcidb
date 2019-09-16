"""I/O schema"""

import jsonschema

# JSON schema for I/O data
JSON = {
    "title": "test_case_list",
    "description": "List of test cases",
    "type": "array",
    "items": {
        "title": "test_case",
        "description": "A test case - an execution of a test",
        "type": "object",
        "properties": {
            "name": {
                "type": "string",
                "description": "Test name",
            },
            "result": {
                "type": "string",
                "description": "Test result",
            },
        },
        "required": [
            "name",
            "result",
        ],
    },
}


def validate(io_data):
    """Validate I/O data with its schema"""
    jsonschema.validate(instance=io_data, schema=JSON)
