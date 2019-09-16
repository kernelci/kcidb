"""Database schema"""
from google.cloud.bigquery.schema import SchemaField as Field

# A map of table names to their BigQuery schemas
TABLE_MAP = dict(
    tests=[
        Field(
            "build_path", "STRING",
            description="Dot-separated build path (^[.a-zA-Z0-9_]*$)"
        ),
        Field(
            "path", "STRING",
            description="Dot-separated test path (^[.a-zA-Z0-9_]*$)"
        ),
        Field(
            "status", "STRING",
            description="Test status (ERROR/FAIL/PASS/DONE)"
        ),
        Field(
            "waived", "BOOL",
            description="Waived flag"
        ),
    ]
)
