import json
import warnings

warnings.simplefilter(action="ignore")

from flask import Flask, jsonify
from flask_cors import CORS, cross_origin

from failmapping import file_map

# -------------------------------
# Flask App Setup
# -------------------------------
app = Flask(__name__)
CORS(app)


@cross_origin()
@app.route("/standardization_process", methods=["POST"])
def standardization_process():
    """
    Flask API to trigger standardization pipeline
    SAME logic as script version
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

    return jsonify({
        "status": "success",
        "result": response
    })


# -------------------------------
# Run Flask Server
# -------------------------------
if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=30103,
        debug=False
    )
