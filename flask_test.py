import json
import warnings

warnings.simplefilter(action="ignore")

from failmapping import file_map


def standardization_process():
    """
    Script-based pipeline runner
    SAME functional behavior as old flask.py
    """

    # -------------------------------
    # Load mapping JSON
    # -------------------------------
    mapping_path = "Standardisation/policy_relational.json"
    with open(mapping_path, "r", encoding="utf-8") as f:
        file_mapping_file_dict = json.load(f)

    # -------------------------------
    # Target details
    # -------------------------------
    schema = "policy_data"
    target_table = "sdm_policy_"

    # -------------------------------
    # Trigger pipeline
    # -------------------------------
    response = file_map(
        file_mapping_file_dict,
        schema,
        target_table
    )

    return response


# -------------------------------
# Entry point
# -------------------------------
if __name__ == "__main__":
    result = standardization_process()
    print("✅ Standardization completed successfully")
    print(result)
