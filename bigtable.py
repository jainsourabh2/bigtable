# https://github.com/googleapis/python-bigtable/blob/HEAD/samples/hello/main.py
import argparse
# import datetime
import time    
from datetime import datetime
from google.cloud import bigtable
from google.cloud.bigtable import column_family
from google.cloud.bigtable import row_filters
from google.cloud.bigtable.row_set import RowSet


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
    max_versions_rule = column_family.MaxVersionsGCRule(2)
    column_family_id = "metrics"
    column_families = {column_family_id: max_versions_rule}
    if not table.exists():
        table.create(column_families=column_families)
    else:
        print("Table {} already exists.".format(table_id))
    # [END bigtable_hw_create_table]

    # [START bigtable_hw_write_rows]
    print("Writing some greetings to the table.")
    greetings = [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]
    rows = []
    column = "money".encode()
    for i, value in enumerate(greetings):
        # Note: This example uses sequential numeric IDs for simplicity,
        # but this can result in poor performance in a production
        # application.  Since rows are stored in sorted order by key,
        # sequential keys can result in poor distribution of operations
        # across nodes.
        #
        # For more information about how to design a Bigtable schema for
        # the best performance, see the documentation:
        #
        #     https://cloud.google.com/bigtable/docs/schema-design
        timestamp_value = int(time.time())
        print(timestamp_value)
        row_key = str(i) + "#" + str(timestamp_value)
        timestamp = int(time.time())
        row = table.direct_row(row_key)
        row.set_cell(
            column_family_id, column, str(value), timestamp=datetime.utcnow()
        )
        rows.append(row)
    table.mutate_rows(rows)
    # [END bigtable_hw_write_rows]

    start_timestamp = str("1701751646")
    end_timestamp = str("1701755775")

    print("Scanning for rows based on key range:")
    row_set = RowSet()
    row_set.add_row_range_from_keys(
        start_key=bytes(str("2#" + start_timestamp).encode()), end_key=bytes(str("2#" + end_timestamp).encode())
    )
    st = time.time_ns()
    rows = table.read_rows(row_set=row_set)
    et = time.time_ns()
    print(st)
    print(et)
    print((et - st)/1000000)
    for row in rows:
        print(row.row_key.decode("utf-8"))
        print(row.cells[column_family_id][column][0].value.decode("utf-8"))

    print("Scanning for rows based on cell timestamp:")
    st = time.time_ns()
    rows = table.read_rows(
        filter_=row_filters.TimestampRangeFilter(row_filters.TimestampRange(start=datetime.fromtimestamp(int(start_timestamp)), end=datetime.fromtimestamp(int(end_timestamp))))
    )
    et = time.time_ns()
    print(st)
    print(et)
    print((et - st)/1000000)
    for row in rows:
        print(row.row_key.decode("utf-8"))
        print(row.cells[column_family_id][column][0].value.decode("utf-8"))

    # [END bigtable_hw_scan_all]
    # [END bigtable_hw_scan_with_filter]

    # [START bigtable_hw_delete_table]
    # print("Deleting the {} table.".format(table_id))
    # table.delete()
    # [END bigtable_hw_delete_table]


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
