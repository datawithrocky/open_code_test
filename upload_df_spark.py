from pyspark.sql import SparkSession
from db_config import db_config
from ingestion_rules import INGESTION_RULES

spark = SparkSession.builder \
    .appName("IDENT-Generic-Ingestion") \
    .getOrCreate()

# -------------------------
# GENERIC READ FUNCTION
# -------------------------
def read_source(rule):
    source_conf = db_config[rule["source"]]

    # MongoDB
    if source_conf["type"] == "mongodb":
        uri = (
            f"mongodb://{source_conf['username']}:{source_conf['password']}"
            f"@{source_conf['host']}:{source_conf['port']}/"
            f"{source_conf['database']}.{rule['object']}"
        )
        return spark.read.format("mongodb").option("uri", uri).load()

    # JDBC Sources (MySQL, SQL Server, etc.)
    jdbc_url = source_conf["url"].format(
        host=source_conf["host"],
        port=source_conf["port"],
        database=source_conf["database"]
    )

    return spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", rule["object"]) \
        .option("user", source_conf["username"]) \
        .option("password", source_conf["password"]) \
        .option("driver", source_conf["driver"]) \
        .load()

# -------------------------
# WRITE TO POSTGRES
# -------------------------
def write_to_postgres(df, target_table):
    pg = db_config["postgresql"]

    url = f"jdbc:postgresql://{pg['host']}:{pg['port']}/{pg['database']}"

    df.write \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", f"{pg['schema']}.{target_table}") \
        .option("user", pg["username"]) \
        .option("password", pg["password"]) \
        .option("driver", pg["driver"]) \
        .mode("append") \
        .save()

# -------------------------
# MAIN FLOW
# -------------------------
for rule in INGESTION_RULES:
    df = read_source(rule)
    df = df.select(*rule["columns"])   # 🔥 SELECTIVE COLUMN TRANSFER
    write_to_postgres(df, rule["target_table"])
