# ingestion_rules.py

INGESTION_RULES = [
    {
        "source": "mongodb",
        "object": "customers",          # collection
        "columns": ["customer_id", "name", "region"],
        "target_table": "landing_customers"
    },
    {
        "source": "mysql",
        "object": "orders",             # table
        "columns": ["order_id", "amount", "order_date"],
        "target_table": "landing_orders"
    }
]
