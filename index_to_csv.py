from flask import Flask, request, Response
import pandas as pd
from pymongo import MongoClient
import io

app = Flask(__name__)

client = MongoClient("mongodb://localhost:27017/")
db = client["clients_db"]
collection = db["clients_data"]

@app.route('/export', methods=['GET'])
def export_csv():
    """API endpoint"""
    index_values = request.args.get('index')
    bng_ip_values = request.args.get('bng_ip')

    # If not parameter added return 204
    if index_values is None and bng_ip_values is None:
        return Response("", status=204)

    # If we have for some params all, return all data
    if (index_values and index_values.lower() == "all") or (bng_ip_values and bng_ip_values.lower() == "all"):
        query = {}
    else:
        query = {}
        if index_values:
            index_list = index_values.split(',')
            query["INDEX"] = {"$in": index_list}
        if bng_ip_values:
            bng_ip_list = bng_ip_values.split(',')
            query["BNG_IP"] = {"$in": bng_ip_list}

    # request to MongoDB
    records = list(collection.find(query, {"_id": 0, "created_at": 0}))

    if not records:
        return Response("", status=204)

    # translate records to DataFrame
    df = pd.DataFrame(records)

    # order the colums: CL_IP, MAC, VLAN, INDEX, BNG_IP
#    df = df[['CL_IP', 'MAC', 'VLAN', 'INDEX', 'BNG_IP']]
    df = df[['CL_IP', 'MAC', 'VLAN']]

    output = io.StringIO()
    df.to_csv(output, index=False, sep='\t', header=False)
    output.seek(0)

    filter_desc = f"index_{index_values}" if index_values else f"bng_ip_{bng_ip_values}"
    return Response(output.getvalue(), mimetype="text/csv", headers={"Content-Disposition": f"attachment; filename=data_{filter_desc}.csv"})

# if __name__ == '__main__':
#     app.run(host='0.0.0.0', port=13000, debug=True)
