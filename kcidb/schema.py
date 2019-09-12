from google.cloud import bigquery

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
