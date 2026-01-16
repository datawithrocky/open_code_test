import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from data_std import ds


def file_map(file_dict, schema, tabl):
    """
    Cleaned version of failmapping.py
    SAME functionality as old code
    """

    pd.set_option("display.max_columns", None)

    # -------------------------------------------------
    # TARGET DB (SDM)
    # -------------------------------------------------
    tgt_engine = create_engine(
        "postgresql+psycopg2://postgres@15.165.117.226:5433/rakesh_tgt_test",
        connect_args={"options": f"-csearch_path={schema}"}
    )

    # Read existing SDM (first run safe)
    try:
        csat = pd.read_sql_table(tabl, tgt_engine)
    except Exception:
        csat = pd.DataFrame()

    # Used for non-relational flow
    sdm_df = pd.DataFrame(columns=csat.columns)

    # Audit containers (kept – optional use later)
    missing_columns = {}
    missing_target = {}

    # -------------------------------------------------
    # NON-RELATIONAL COLUMN MAPPING (UNCHANGED LOGIC)
    # -------------------------------------------------
    def file_mapping(file_mapping_list, df, sdm_df, df_type, sch, df_columns, filename):

        dict_flag = dict.fromkeys(list(csat.columns), 0)
        temp_df = pd.DataFrame(columns=list(csat.columns))
        temp_df["Index"] = df_type

        for mp in file_mapping_list:

            if mp["source_column"] in df_columns:

                if mp["target_column"] in temp_df.columns:

                    if dict_flag[mp["target_column"]] == 0:
                        temp_df[mp["target_column"]] = df[mp["source_column"]]
                        dict_flag[mp["target_column"]] = 1
                    else:
                        temp_df2 = pd.DataFrame(columns=list(csat.columns))
                        temp_df2[mp["target_column"]] = df[mp["source_column"]]
                        temp_df = pd.concat([temp_df, temp_df2], ignore_index=True)

                else:
                    missing_target[filename].append(mp["target_column"])
            else:
                missing_columns[filename].append(mp["source_column"])

        temp_df["filename"] = df["filename"]
        temp_df = temp_df.replace({np.nan: None})

        sdm_df = pd.concat([sdm_df, temp_df], ignore_index=True)
        return sdm_df

    # -------------------------------------------------
    # SOURCE DB
    # -------------------------------------------------
    src_engine = create_engine(
        "postgresql+psycopg2://postgres@15.165.117.226:5433/rak_src",
        connect_args={"options": "-csearch_path=src_schema"}
    )

    # -------------------------------------------------
    # MAIN LOOP
    # -------------------------------------------------
    for filename, config in file_dict.items():

        table_name = filename.replace(".csv", "").replace(".xlsx", "")
        df = pd.read_sql_table(table_name, src_engine)

        # Cleaning & trimming (PRESERVED)
        df = df.replace({np.nan: None})
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        df["filename"] = table_name

        missing_columns[filename] = []
        missing_target[filename] = []

        # Relational flow
        if config["relational"] == "True":
            csat = ds(
                file_mapping_list=config["mapping"],
                target_policy=csat,
                policy_dict=file_dict,
                source_f=table_name
            )

        # Non-relational flow
        else:
            sdm_df = file_mapping(
                config["mapping"],
                df,
                sdm_df,
                config["type"],
                schema,
                list(df.columns),
                filename
            )

    # -------------------------------------------------
    # FINAL OUTPUT
    # -------------------------------------------------
    final_df = csat if not csat.empty else sdm_df
    final_df.dropna(axis=1, how="all", inplace=True)

    final_df.to_sql(
        tabl,
        tgt_engine,
        schema=schema,
        if_exists="replace",
        index=False
    )

    return {
        "target_table": tabl,
        "schema": schema,
        "rows_loaded": len(final_df)
    }
