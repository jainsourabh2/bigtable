# https://github.com/googleapis/python-bigtable/blob/HEAD/samples/hello/main.py
import argparse
# import datetime
import time
import random
from datetime import datetime
from google.cloud import bigtable
from google.cloud.bigtable import column_family
from google.cloud.bigtable import row_filters
from google.cloud.bigtable.row_set import RowSet
import atexit

from apscheduler.schedulers.background import BackgroundScheduler

def print_date_time():
    print(time.strftime("%A, %d. %B %Y %I:%M:%S %p"))

def main(project_id, instance_id, table_id):
    # [START bigtable_hw_connect]
    # The client must be created with admin=True because it will create a
    # table.
    client = bigtable.Client(project=project_id, admin=True)
    instance = client.instance(instance_id)
    # [END bigtable_hw_connect]

    # [START bigtable_hw_create_table]
    print("Creating the {} table.".format(table_id))
    table = instance.table(table_id)

    print("Creating column family cf1 with Max Version GC rule...")
    # Create a column family with GC policy : most recent N versions
    # Define the GC policy to retain only the most recent 2 versions
    max_versions_rule = column_family.MaxVersionsGCRule(3)
    column_family_id = "metrics"
    column_families = {column_family_id: max_versions_rule}
    if not table.exists():
        table.create(column_families=column_families)
    else:
        print("Table {} already exists.".format(table_id))
    # [END bigtable_hw_create_table]

    # [START bigtable_hw_write_rows]
    print("Writing some money to the table.")
    money = [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]
    rows = []
    column = "money".encode()
    for i, value in enumerate(money):
        timestamp_value = int(time.time())//60*60
        print(timestamp_value)
        row_key = str(i) + "#" + str(timestamp_value)
        timestamp = int(time.time())
        row = table.direct_row(row_key)
        value_money =  int(random.randint(1, 1000))
        row.set_cell(
            column_family_id, column, value_money, timestamp=datetime.utcnow()
        )
        print(row_key)
        print(value_money)
        rows.append(row)
    table.mutate_rows(rows)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("project_id", help="Your Cloud Platform project ID.")
    parser.add_argument(
        "instance_id", help="ID of the Cloud Bigtable instance to connect to."
    )
    parser.add_argument(
        "table_id", help="Table to create and destroy.", default="timestamp-demo"
    )

    args = parser.parse_args()
    main(args.project_id, args.instance_id, args.table_id)
