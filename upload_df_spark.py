from pyspark.sql import SparkSession
from db_config import db_config
from ingestion_rules import INGESTION_RULES

# -------------------------
# SPARK SESSION
# -------------------------
spark = SparkSession.builder \
    .appName("IDENT-Mongo-Selective-Ingestion") \
    .getOrCreate()

# -------------------------
# READ FROM MONGODB
# -------------------------
def read_mongodb(rule):
    mongo_conf = db_config["mongodb"]

    mongo_uri = (
        f"mongodb://{mongo_conf['username']}:{mongo_conf['password']}"
        f"@{mongo_conf['host']}:{mongo_conf['port']}/"
        f"{mongo_conf['database']}.{rule['collection']}"
    )

    df = spark.read \
        .format("mongodb") \
        .option("uri", mongo_uri) \
        .load()

    return df

# -------------------------
# WRITE TO POSTGRES
# -------------------------
def write_postgres(df, target_table):
    pg_conf = db_config["postgresql"]

    pg_url = (
        f"jdbc:postgresql://{pg_conf['host']}:{pg_conf['port']}/"
        f"{pg_conf['database']}"
    )

    df.write \
        .format("jdbc") \
        .option("url", pg_url) \
        .option("dbtable", f"{pg_conf['landschm']}.{target_table}") \
        .option("user", pg_conf["username"]) \
        .option("password", pg_conf["password"]) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

# -------------------------
# MAIN IDENT INGESTION FLOW
# -------------------------
def run_ident_ingestion():
    for rule in INGESTION_RULES:

        # Step 1: Read MongoDB
        df = read_mongodb(rule)

        # Step 2: Select Required Columns
        df = df.select(*rule["columns"])

        # Step 3: Apply Filter Rule
        if rule.get("filter_condition"):
            df = df.filter(rule["filter_condition"])

        # Step 4: Write to Postgres
        write_postgres(df, rule["target_table"])

# -------------------------
# DRIVER
# -------------------------
if __name__ == "__main__":
    run_ident_ingestion()
