import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from data_std import ds


def file_map(file_dict, schema, tabl):

    pd.set_option("display.max_columns", None)

    # -------------------------------
    # Target DB
    # -------------------------------
    tgt_engine = create_engine(
        "postgresql+psycopg2://postgres@15.165.117.226:5433/rakesh_tgt_test",
        connect_args={"options": f"-csearch_path={schema}"}
    )

    try:
        csat = pd.read_sql_table(tabl, tgt_engine)
    except Exception:
        csat = pd.DataFrame()

    sdm_df = pd.DataFrame(columns=csat.columns)

    missing_columns = {}
    missing_target = {}

    # --------------------------------------------------
    # NON-RELATIONAL MAPPING (RESTORED)
    # --------------------------------------------------
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

    # -------------------------------
    # Source DB
    # -------------------------------
    src_engine = create_engine(
        "postgresql+psycopg2://postgres@15.165.117.226:5433/rak_src",
        connect_args={"options": "-csearch_path=src_schema"}
    )

    # -------------------------------
    # Main loop
    # -------------------------------
    for filename, config in file_dict.items():

        table_name = filename.replace(".csv", "").replace(".xlsx", "")
        df = pd.read_sql_table(table_name, src_engine)

        # Cleaning & trimming (PRESERVED)
        df = df.replace({np.nan: None})
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        df["filename"] = table_name

        missing_columns[filename] = []
        missing_target[filename] = []

        if config["relational"] == "True":
            csat = ds(
                file_mapping_list=config["mapping"],
                target_policy=csat,
                policy_dict=file_dict,
                source_f=table_name
            )
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

    # -------------------------------
    # Final write
    # -------------------------------
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
