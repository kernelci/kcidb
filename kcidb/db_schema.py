"""Database schema"""
from google.cloud.bigquery.schema import SchemaField as Field

# A map of table names to their BigQuery schemas
TABLE_MAP = dict(
    revisions=[
        Field(
            "id", "INT64",
            description="Unique revision ID"
        ),
        Field(
            "origin", "STRING",
            description="The name of the CI system which submitted "
                        "the revision"
        ),
        Field(
            "origin_id", "STRING",
            description="Origin-unique revision ID"
        ),
    ],
    builds=[
        Field(
            "id", "INT64",
            description="Unique build ID"
        ),
        Field(
            "revision_origin", "STRING",
            description="The name of the CI system which submitted "
                        "the revision"
        ),
        Field(
            "revision_origin_id", "STRING",
            description="Origin-unique revision ID"
        ),
        Field(
            "origin", "STRING",
            description="The name of the CI system which submitted "
                        "the build"
        ),
        Field(
            "origin_id", "STRING",
            description="Origin-unique build ID"
        ),
    ],
    tests=[
        Field(
            "id", "INT64",
            description="Unique test run ID"
        ),
        Field(
            "build_origin", "STRING",
            description="The name of the CI system which submitted "
                        "the tested build"
        ),
        Field(
            "build_origin_id", "STRING",
            description="Origin-unique ID of the tested build"
        ),
        Field(
            "origin", "STRING",
            description="The name of the CI system which submitted "
                        "the test run"
        ),
        Field(
            "origin_id", "STRING",
            description="Origin-unique test run ID"
        ),
        Field(
            "path", "STRING",
            description="Dot-separated canonical test tree path"
        ),
        Field(
            "status", "STRING",
            description="Test status"
        ),
        Field(
            "waived", "BOOL",
            description="Waived flag"
        ),
    ]
)
