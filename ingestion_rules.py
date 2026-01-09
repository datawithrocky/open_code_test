# ingestion_rules.py

INGESTION_RULES = [
    {
        "source_db": "mongodb",
        "collection": "customers",
        "target_table": "landing_customers",
        "columns": ["customer_id", "name", "region", "status"],
        "filter_condition": "status = 'ACTIVE'"
    }
]
