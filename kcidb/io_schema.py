"""I/O schema"""

import jsonschema

# JSON schema for a code revision
JSON_REVISION = {
    "title": "revision",
    "description": "A revision of the tested code",
    "type": "object",
    "properties": {
        "origin": {
            "type": "string",
            "description":
                "The name of the CI system which submitted the revision",
            "pattern": "^[a-z0-9_]*$"
        },
        "origin_id": {
            "type": "string",
            "description": "Origin-unique revision ID",
        },
    },
    "required": [
        "origin",
        "origin_id",
    ],
}

# JSON schema for a build of a revision
JSON_BUILD = {
    "title": "build",
    "description": "A build of a revision",
    "type": "object",
    "properties": {
        "revision_origin": {
            "type": "string",
            "description":
                "The name of the CI system which submitted the built revision",
            "pattern": "^[a-z0-9_]*$"
        },
        "revision_origin_id": {
            "type": "string",
            "description": "Origin-unique ID of the built revision",
        },
        "origin": {
            "type": "string",
            "description":
                "The name of the CI system which submitted the build",
            "pattern": "^[a-z0-9_]*$"
        },
        "origin_id": {
            "type": "string",
            "description": "Origin-unique build ID",
        },
    },
    "required": [
        "revision_origin",
        "revision_origin_id",
        "origin",
        "origin_id",
    ],
}

# JSON schema for a test run on a build
JSON_TEST = {
    "title": "test",
    "description": "A test run against a build",
    "type": "object",
    "properties": {
        "build_origin": {
            "type": "string",
            "description":
                "The name of the CI system which submitted the tested build",
            "pattern": "^[a-z0-9_]*$"
        },
        "build_origin_id": {
            "type": "string",
            "description": "Origin-unique ID of the tested build",
        },
        "origin": {
            "type": "string",
            "description":
                "The name of the CI system which submitted the test run",
            "pattern": "^[a-z0-9_]*$"
        },
        "origin_id": {
            "type": "string",
            "description": "Origin-unique ID of the tested build",
        },
        "path": {
            "type": "string",
            "description":
                "Dot-separated canonical test tree path "
                "of the test being executed",
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
        },
    },
    "required": [
        "build_origin",
        "build_origin_id",
        "origin",
        "origin_id",
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
        "revision_list": {
            "description": "List of code revisions",
            "type": "array",
            "items": JSON_REVISION,
        },
        "build_list": {
            "description": "List of builds",
            "type": "array",
            "items": JSON_BUILD,
        },
        "test_list": {
            "description": "List of test runs",
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
