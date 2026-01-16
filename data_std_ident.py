import numpy as np
import pandas as pd
from sqlalchemy import create_engine

def merge_dicts(dict_list):
    merged_dict = {}
    for d in dict_list:
        merged_dict.update(d)
    return merged_dict

def check_duplicate_columns(df):
    duplicates = df.index[df.index.duplicated()].tolist()
    return duplicates


def ds(file_mapping_list, source_df, target_policy, policy_dict, source_f):

    input_conn_string = 'postgresql+psycopg2://admin:admin123@40.81.136.92/Data_Migration_Pipeline'
    input_db = create_engine(input_conn_string)
    input_conn = input_db.connect()

    table_name = source_f.replace(".xlsx", "")
    source_df = pd.read_sql(table_name, input_conn)

    source_subset_columns = list(
        set([i["source_column"] for i in policy_dict[source_f]["mapping"]])
    )

    source_key = list(set([i["source_key"] for i in policy_dict[source_f]["mapping"]]))
    target_key = list(set([i["target_key"] for i in policy_dict[source_f]["mapping"]]))

    source_df_sb = source_df[source_subset_columns]

    rename_cols = [
        {i['source_column']: i['target_column']}
        for i in policy_dict[source_f]["mapping"]
    ]

    rename_cols_n = merge_dicts(rename_cols)

    # ❌ BUG HERE (index instead of columns)
    source_df_sb = source_df_sb.rename(index=rename_cols_n)

    target_policy = target_policy.merge(
        source_df_sb,
        on=target_key,
        how="outer",
        suffixes=('', '_df2')
    )

    for col in target_policy:
        col_df2 = col + '_df2'
        if col_df2 in target_policy.columns:
            target_policy[col] = (
                target_policy[col_df2]
                .replace('', np.nan)
                .combine_first(target_policy[col])
            )
            target_policy.drop(columns=[col_df2], inplace=True)

    for col in target_policy.columns:
        if "_df2" in col:
            target_policy[col.split('_df2')[0]] = (
                target_policy[col]
                .replace('', np.nan)
                .combine_first(target_policy[col.split('_df2')[0]])
            )
            target_policy.drop(columns=[col], axis="columns")

    target_policy = target_policy.apply(
        lambda x: x.replace(pd.NaT, np.nan)
        if x.dtype == 'datetime64[ns]' else x
    )

    return target_policy
