from flask import Flask, request, jsonify
import pandas as pd
import ipaddress
import re
import os
from datetime import datetime
from werkzeug.utils import secure_filename
from pymongo import MongoClient, UpdateOne

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["clients_db"]
collection = db["clients_data"]

# Create TTL index to auto-delete records after 10 minutes (600 seconds)
collection.create_index("created_at", expireAfterSeconds=600)
collection.create_index("MAC", unique=False)

# Pre-compiled regular expressions
MAC_REGEX = re.compile(r'^([0-9A-Fa-f]{2}:){5}[0-9A-Fa-f]{2}$')
INDEX_REGEX = re.compile(r'^[A-Z]{2}$')

def is_valid_ip(ip_str):
    try:
        ipaddress.ip_address(ip_str)
        return True
    except ValueError:
        return False

def is_valid_mac(mac_str):
    return MAC_REGEX.fullmatch(mac_str) is not None

def is_valid_vlan(num):
    try:
        num = int(num)
        return 100 <= num <= 4092
    except (ValueError, TypeError):
        return False

def is_valid_index(idx):
    if not isinstance(idx, str):
        idx = ""
    return INDEX_REGEX.fullmatch(idx.strip()) is not None

@app.route('/clients', methods=['POST'])
def process_csv():
    # Check if a file is provided in the request
    if 'file' not in request.files:
        return jsonify({"error": "No file provided."}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No file selected."}), 400

    # Save the file with a unique name using timestamp
    timestamp = datetime.now().strftime("%s")
    filename = secure_filename(file.filename)
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], f'{timestamp}_{filename}')
    file.save(filepath)

    try:
        # Read the CSV file with tab delimiter and assign column names
        df = pd.read_csv(filepath, delimiter='\t', header=None)
        df.columns = ['CL_IP', 'MAC', 'VLAN', 'BNG_IP', 'INDEX']
    except Exception as e:
        return jsonify({"error": "Error processing file.", "details": str(e)}), 400

    valid_records = []
    error_rows = []
    bulk_operations = []

    # Process each row of the CSV file
    for idx, row in df.iterrows():
        row_errors = {}

        if not is_valid_ip(row['CL_IP']):
            row_errors['CL_IP'] = "Invalid IP address."
        if not is_valid_mac(row['MAC']):
            row_errors['MAC'] = "Invalid MAC address."
        if not is_valid_vlan(row['VLAN']):
            row_errors['VLAN'] = "Wrong VLAN. Should be 100 - 4092."
        if not is_valid_ip(row['BNG_IP']):
            row_errors['BNG_IP'] = "Invalid IP address."
        if not is_valid_index(str(row['INDEX'])):
            row_errors['INDEX'] = "Wrong Index."

        if row_errors:
            error_rows.append({"record": idx + 1, "data": row.to_dict(), "errors": row_errors})
        else:
            record = row.to_dict()
            current_time = datetime.utcnow()
            record["created_at"] = current_time
            valid_records.append(record)

            bulk_operations.append(
                UpdateOne(
                    {"MAC": record["MAC"]},  # Find document by MAC address
                    {"$set": {
                        "CL_IP": record["CL_IP"],
                        "VLAN": record["VLAN"],
                        "BNG_IP": record["BNG_IP"],
                        "INDEX": record["INDEX"],
                        "created_at": current_time  # Set the timestamp
                    }},
                    upsert=True  # Insert the document if it does not exist
                )
            )

    # Execute bulk operations in MongoDB
    if bulk_operations:
        try:
            collection.bulk_write(bulk_operations)
        except Exception as e:
            return jsonify({"error": "Database update failed.", "details": str(e)}), 500

    # Remove the original file to prevent file accumulation
    try:
        os.remove(filepath)
    except Exception:
        pass  # Optionally log the error

    # Save invalid records to a separate CSV file if there are any errors
    if error_rows:
        invalid_filepath = os.path.join(app.config['UPLOAD_FOLDER'], f"invalid_data_{timestamp}.csv")
        try:
            pd.DataFrame([e["data"] for e in error_rows]).to_csv(invalid_filepath, index=False, sep='\t', header=False)
        except Exception as e:
            return jsonify({"error": "Failed to save invalid records file.", "details": str(e)}), 500

    response_data = {
        "message": "The data is processed.",
        "processed_records": len(df),
        "invalid_records": len(error_rows)
    }
    if error_rows:
        response_data["invalid_info"] = error_rows

    return jsonify(response_data), 200

# To run the application directly:
#if __name__ == '__main__':
#    app.run(host='46.40.123.1', port=15000, debug=True)
