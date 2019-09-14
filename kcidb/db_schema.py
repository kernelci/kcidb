"""Database schema"""
from google.cloud import bigquery

# A map of table names to their BigQuery schemas
table_map = dict(
    tests=[
        bigquery.schema.SchemaField(
            "name", "STRING",
            description="Test name"
        ),
        bigquery.schema.SchemaField(
            "result", "STRING",
            description="Test result"
        ),
    ]
)
