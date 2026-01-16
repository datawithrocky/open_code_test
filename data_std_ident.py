import pandas as pd
import numpy as np
from sqlalchemy import create_engine


def merge_dicts(dict_list):
    merged = {}
    for d in dict_list:
        merged.update(d)
    return merged


def ds(file_mapping_list, target_df, policy_dict, source_table):
    """
    Relational selective column extraction & merge
    """

    # PostgreSQL connection
    engine = create_engine(
        "postgresql+psycopg2://admin:admin123@40.81.136.92/Data_Migration_Pipeline"
    )

    # Read source table
    source_df = pd.read_sql_table(source_table, engine)

    # Required source columns
    source_columns = list(
        set(m["source_column"] for m in policy_dict[source_table]["mapping"])
    )

    source_key = policy_dict[source_table]["mapping"][0]["source_key"]
    target_key = policy_dict[source_table]["mapping"][0]["target_key"]

    # Selective extraction
    source_df = source_df[source_columns]

    # Rename source → target
    rename_cols = merge_dicts([
        {m["source_column"]: m["target_column"]}
        for m in policy_dict[source_table]["mapping"]
    ])

    source_df = source_df.rename(columns=rename_cols)

    # Merge with target
    merged_df = target_df.merge(
        source_df,
        left_on=target_key,
        right_on=rename_cols[source_key],
        how="outer",
        suffixes=("", "_new")
    )

    # Column overwrite logic
    for col in merged_df.columns:
        if col.endswith("_new"):
            base_col = col.replace("_new", "")
            merged_df[base_col] = merged_df[col].combine_first(merged_df[base_col])
            merged_df.drop(columns=[col], inplace=True)

    return merged_df
