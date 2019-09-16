"""I/O schema"""

import jsonschema

# JSON schema for a test execution
JSON_TEST = {
    "title": "test",
    "description": "A test execution",
    "type": "object",
    "properties": {
        "build_path": {
            "type": "string",
            "description":
                "Dot-separated build tree path of the build being tested",
            "pattern": "^[.a-zA-Z0-9_]*$"
        },
        "path": {
            "type": "string",
            "description":
                "Dot-separated test tree path of the test being executed",
            "pattern": "^[.a-zA-Z0-9_]*$"
        },
        "status": {
            "type": "string",
            "description": "Execution status",
            "enum": ["ERROR", "FAIL", "PASS", "DONE"],
        },
        "waived": {
            "type": "boolean",
            "description": "Waived flag",
            "default": False,
        }
    },
    "required": [
        "build_path",
        "path",
    ],
}

# JSON schema for I/O data
JSON = {
    "title": "kcidb",
    "description": "Kernelci.org test data",
    "type": "object",
    "properties": {
        "version": {
            "type": "string",
            "description": "Version of the schema the data complies to",
            "const": "1"
        },
        "test_list": {
            "description": "List of test executions",
            "type": "array",
            "items": JSON_TEST,
        },
    },
    "required": [
        "version",
    ]
}


def validate(io_data):
    """Validate I/O data with its schema"""
    jsonschema.validate(instance=io_data, schema=JSON)
