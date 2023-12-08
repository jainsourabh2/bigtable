# write a sample flask app with post method and run it on port 5000
from flask import Flask, request
import json
import time
from apscheduler.schedulers.background import BackgroundScheduler
import argparse  
from datetime import datetime
from google.cloud import bigtable
from google.cloud.bigtable import column_family
from google.cloud.bigtable import row_filters
from google.cloud.bigtable.row_set import RowSet
import atexit
import pandas as pd
import json

# Needs to be populated
project_id = ''
instance_id = ''
table_id = ''

# [START bigtable_hw_connect]
client = bigtable.Client(project=project_id, admin=True)
instance = client.instance(instance_id)
# [END bigtable_hw_connect]

# [START bigtable_hw_create_table]
table = instance.table(table_id)
# [END bigtable_hw_create_table]

max_versions_rule = column_family.MaxVersionsGCRule(3)
column_family_id = "metrics"
column = "money".encode()

data = {'timestamp':[],
        'rowid':[],
        'value':[]}

def test_scheduler():
    end_timestamp = int(time.time())//60*60
    start_timestamp = end_timestamp - 7200
    print(start_timestamp)
    print(end_timestamp)
    print("Scanning for rows based on cell timestamp:")
    st = time.time_ns()
    rows = table.read_rows(
        filter_=row_filters.RowFilterChain(
            filters=[
                row_filters.TimestampRangeFilter(row_filters.TimestampRange(start=datetime.fromtimestamp(int(start_timestamp)), end=datetime.fromtimestamp(int(end_timestamp)))),
                row_filters.CellsColumnLimitFilter(3)
            ]
        )
    )
    et = time.time_ns()
    print(st)
    print(et)
    print((et - st)/1000000)
    global data
    data = {'timestamp':[],
            'rowid':[],
            'value':[]}
    st = time.time_ns()
    for row in rows:
        cells = len(row.cells[column_family_id][column]) 
        if cells == 3:
            minimum_money = min(row.cells[column_family_id][column][0].value.decode("utf-8"),row.cells[column_family_id][column][1].value.decode("utf-8"),row.cells[column_family_id][column][2].value.decode("utf-8"))
        elif cells == 2:
            minimum_money = min(row.cells[column_family_id][column][0].value.decode("utf-8"),row.cells[column_family_id][column][1].value.decode("utf-8"))
        else: 
            minimum_money = min(row.cells[column_family_id][column][0].value.decode("utf-8"))
        print(row.row_key.decode("utf-8").split("#"))   
        data['timestamp'].append(row.row_key.decode("utf-8").split("#")[1])
        data['rowid'].append(row.row_key.decode("utf-8").split("#")[0])
        data['value'].append(minimum_money)
    print(data)
    et = time.time_ns()
    print(st)
    print(et)
    print((et - st)/1000000)

app = Flask(__name__)

@app.route('/post', methods=['POST'])
def post():
    global data
    # data = request.get_json()
    print(data)
    df = pd.DataFrame.from_dict(data)
    # df = pd.DataFrame(data)
    print(df)
    output = df[df['timestamp'].astype(int) > (int(time.time())//60*60)-7200]
    return output.to_json(orient = "records") , 200

if __name__ == '__main__':
    print('Starting flask app')
    sched = BackgroundScheduler()
    sched.add_job(test_scheduler, trigger="interval", seconds=10)
    sched.start()
    app.run(host='0.0.0.0', port=5000)    # run app on port 5000
