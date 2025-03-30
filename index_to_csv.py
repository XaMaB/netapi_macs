from flask import Flask, request, Response
import pandas as pd
from pymongo import MongoClient
import io

app = Flask(__name__)

client = MongoClient("mongodb://localhost:27017/")
db = client["clients_db"]
collection = db["clients_data"]

@app.route('/export', methods=['GET'])
def export_data():

    client_param = request.args.get('client')
    export_format = request.args.get('export', 'csv').lower()
    index_values = request.args.get('index')
    bng_ip_values = request.args.get('bng_ip')

    if not client_param:
        expected = "client=admin or client=edge"
        return Response(
            f"Missing required parameter 'client'. Expected parameters: {expected}. Optionally, index, bng_ip and export=json.",
            status=400
        )

    if client_param.lower() not in ['admin', 'edge']:
        return Response("Invalid client parameter. Allowed values are 'admin' or 'edge'.", status=400)

    if index_values is None and bng_ip_values is None:
        query = {}
        filter_desc = "all"
    else:
        if (index_values and index_values.lower() == "all") or (bng_ip_values and bng_ip_values.lower() == "all"):
            query = {}
            filter_desc = "all"
        else:
            query = {}
            filter_desc_parts = []
            if index_values:
                index_list = index_values.split(',')
                query["INDEX"] = {"$in": index_list}
                filter_desc_parts.append(f"index_{index_values}")
            if bng_ip_values:
                bng_ip_list = bng_ip_values.split(',')
                query["BNG_IP"] = {"$in": bng_ip_list}
                filter_desc_parts.append(f"bng_ip_{bng_ip_values}")
            filter_desc = "_".join(filter_desc_parts) if filter_desc_parts else "all"

    records = list(collection.find(query, {"_id": 0, "created_at": 0}))
    if not records:
        return Response("", status=204)

    df = pd.DataFrame(records)

    if client_param.lower() == 'admin':
        columns_admin = ['CL_IP', 'MAC', 'VLAN', 'INDEX', 'BNG_IP']
        df = df[[col for col in columns_admin if col in df.columns]]
    elif client_param.lower() == 'edge':
        columns_edge = ['CL_IP', 'MAC', 'VLAN']
        df = df[[col for col in columns_edge if col in df.columns]]

    if export_format == 'json':
        response_data = df.to_json(orient='records')
        return Response(
            response_data,
            mimetype="application/json",
            headers={"Content-Disposition": f"attachment; filename=data_{filter_desc}.json"}
        )
    else:
        output = io.StringIO()
        df.to_csv(output, index=False, sep='\t', header=False)
        output.seek(0)
        return Response(
            output.getvalue(),
            mimetype="text/csv",
            headers={"Content-Disposition": f"attachment; filename=data_{filter_desc}.csv"}
        )

#if __name__ == '__main__':
#    app.run(host='0.0.0.0', port=19000, debug=True)
