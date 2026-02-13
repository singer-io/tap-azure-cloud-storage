import io
from singer_encodings import csv as singer_csv

# Test data matching your CSV
csv_data = b"""id,created_at,name,active,amount
1101,2026-01-09T10:00:00Z,Older,false,10.0
1102,2026-01-11T08:15:00Z,Equal,true,20.0
1103,2026-01-12T09:00:00Z,Newer,true,30.0
"""

table_spec = {
    'table_name': 'my_table',
    'search_pattern': 'my_table_csv_fresh\\.csv$',
    'key_properties': ['id'],
    'date_overrides': ['created_at'],
    'delimiter': ','
}

buffer = io.BytesIO(csv_data)

print("Creating iterator...")
try:
    iterator = singer_csv.get_row_iterator(buffer, table_spec, None, True)
    print(f"Iterator created: {iterator}")
    print(f"Iterator type: {type(iterator)}")

    if iterator:
        print("Starting iteration...")
        count = 0
        for row in iterator:
            count += 1
            print(f"Row {count}: {row}")
            if count >= 5:
                break
        print(f"Total rows: {count}")
    else:
        print("Iterator is None or False")
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
