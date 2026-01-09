# config.py

# -------------------------
# SOURCE CONNECTION CONFIG
# -------------------------
SOURCE_CONNECTIONS = {
    "mysql": {
        "driver": "com.mysql.cj.jdbc.Driver",
        "url": "jdbc:mysql://{host}:{port}/{database}",
        "port": 3306
    },
    "sqlserver": {
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "url": "jdbc:sqlserver://{host}:{port};databaseName={database}",
        "port": 1433
    },
    # Future ready
    "snowflake": {
        "driver": "net.snowflake.client.jdbc.SnowflakeDriver",
        "url": "jdbc:snowflake://{account}.snowflakecomputing.com"
    }
}

# -------------------------
# TARGET (POSTGRES) CONFIG
# -------------------------
TARGET_POSTGRES = {
    "driver": "org.postgresql.Driver",
    "url": "jdbc:postgresql://{host}:{port}/{database}",
    "port": 5432
}

# -------------------------
# INGESTION RULES (IDENT)
# -------------------------
INGESTION_RULES = [
    {
        "source_type": "mysql",
        "source_table": "customer",
        "target_table": "landing_customer",
        "columns": ["cust_id", "cust_name", "region", "status"],
        "filter_condition": "status = 'ACTIVE'"
    },
    {
        "source_type": "sqlserver",
        "source_table": "orders",
        "target_table": "landing_orders",
        "columns": ["order_id", "order_date", "amount"],
        "filter_condition": "amount > 1000"
    }
]
