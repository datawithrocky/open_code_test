import json
import pandas as pd
from sqlalchemy import create_engine
from data_std import ds


def file_map(file_dict, schema, target_table):

    engine = create_engine(
        "postgresql+psycopg2://admin:admin123@40.81.136.92/Data_Migration_Pipeline"
    )

    # Read existing target SDM
    try:
        target_df = pd.read_sql_table(target_table, engine, schema=schema)
    except:
        target_df = pd.DataFrame()

    # Loop through source tables
    for source_table in file_dict.keys():

        config = file_dict[source_table]

        if config["relational"] == "True":
            target_df = ds(
                file_dict[source_table]["mapping"],
                target_df,
                file_dict,
                source_table.replace(".xlsx", "")
            )

    # Write final SDM
    target_df.to_sql(
        target_table,
        engine,
        schema=schema,
        if_exists="replace",
        index=False
    )

    return {"status": "success"}
