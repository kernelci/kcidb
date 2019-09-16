"""Database schema"""
from google.cloud.bigquery.schema import SchemaField as Field

# A map of table names to their BigQuery schemas
TABLE_MAP = dict(
    tests=[
        Field(
            "name", "STRING",
            description="Test name"
        ),
        Field(
            "result", "STRING",
            description="Test result"
        ),
    ]
)
