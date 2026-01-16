import pandas as pd
import numpy as np
from sqlalchemy import create_engine


def merge_dicts(dict_list):
    merged = {}
    for d in dict_list:
        merged.update(d)
    return merged


def check_duplicate_columns(df):
    return df.columns[df.columns.duplicated()].tolist()


def ds(file_mapping_list, source_df, target_policy, policy_dict, source_f):

    engine = create_engine(
        "postgresql+psycopg2://admin:admin123@40.81.136.92/Data_Migration_Pipeline"
    )

    table_name = source_f.replace(".xlsx", "")
    source_df = pd.read_sql(table_name, engine)

    # 🔹 selective source columns
    source_subset_columns = list(
        set(i["source_column"] for i in policy_dict[source_f]["mapping"])
    )

    source_keys = list(set(i["source_key"] for i in policy_dict[source_f]["mapping"]))
    target_keys = list(set(i["target_key"] for i in policy_dict[source_f]["mapping"]))

    source_df_sb = source_df[source_subset_columns]

    # 🔹 rename source → target
    rename_cols = merge_dicts([
        {i["source_column"]: i["target_column"]}
        for i in policy_dict[source_f]["mapping"]
    ])

    # ✅ FIXED: rename columns (not index)
    source_df_sb = source_df_sb.rename(columns=rename_cols)

    # 🔹 relational merge
    target_policy = target_policy.merge(
        source_df_sb,
        left_on=target_keys,
        right_on=[rename_cols[k] for k in source_keys],
        how="outer",
        suffixes=('', '_df2')
    )

    # 🔹 overwrite only when new value exists
    for col in target_policy.columns:
        col_df2 = f"{col}_df2"
        if col_df2 in target_policy.columns:
            target_policy[col] = (
                target_policy[col_df2]
                .replace('', np.nan)
                .combine_first(target_policy[col])
            )
            target_policy.drop(columns=[col_df2], inplace=True)

    # 🔹 handle NaT safely
    target_policy = target_policy.apply(
        lambda x: x.replace(pd.NaT, np.nan)
        if x.dtype == "datetime64[ns]" else x
    )

    return target_policy
