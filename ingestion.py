from pyspark.sql import SparkSession
from config import SOURCE_CONNECTIONS, TARGET_POSTGRES, INGESTION_RULES

spark = SparkSession.builder \
    .appName("IDENT-Selective-Ingestion") \
    .getOrCreate()

# -------------------------
# JDBC READ FUNCTION
# -------------------------
def read_source_table(rule, credentials):
    source_conf = SOURCE_CONNECTIONS[rule["source_type"]]

    jdbc_url = source_conf["url"].format(
        host=credentials["host"],
        port=source_conf.get("port"),
        database=credentials["database"]
    )

    df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", rule["source_table"]) \
        .option("user", credentials["user"]) \
        .option("password", credentials["password"]) \
        .option("driver", source_conf["driver"]) \
        .load()

    return df

# -------------------------
# JDBC WRITE FUNCTION
# -------------------------
def write_to_postgres(df, target_table, target_credentials):
    jdbc_url = TARGET_POSTGRES["url"].format(
        host=target_credentials["host"],
        port=TARGET_POSTGRES["port"],
        database=target_credentials["database"]
    )

    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", target_table) \
        .option("user", target_credentials["user"]) \
        .option("password", target_credentials["password"]) \
        .option("driver", TARGET_POSTGRES["driver"]) \
        .mode("append") \
        .save()

# -------------------------
# MAIN IDENT INGESTION FLOW
# -------------------------
def run_ident_ingestion(source_credentials, target_credentials):
    for rule in INGESTION_RULES:

        # Step 1: Read Source
        df = read_source_table(rule, source_credentials)

        # Step 2: Select Required Columns
        df = df.select(*rule["columns"])

        # Step 3: Apply Ingestion Rule (Subset)
        if rule.get("filter_condition"):
            df = df.filter(rule["filter_condition"])

        # Step 4: Write to Landing (Postgres)
        write_to_postgres(
            df,
            rule["target_table"],
            target_credentials
        )

# -------------------------
# DRIVER CODE
# -------------------------
if __name__ == "__main__":

    source_credentials = {
        "host": "source-host",
        "database": "source-db",
        "user": "source-user",
        "password": "source-password"
    }

    target_credentials = {
        "host": "pg-host",
        "database": "landing-db",
        "user": "pg-user",
        "password": "pg-password"
    }

    run_ident_ingestion(source_credentials, target_credentials)
