"""
Helper script to create jsonl_csv.zip for testing.
This creates a ZIP file containing both CSV and JSONL files.
"""

import zipfile
import os
import tempfile

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

def create_zip_file():
    """Create jsonl_csv.zip with CSV and JSONL files containing 35 total records."""
    zip_path = os.path.join(SCRIPT_DIR, 'jsonl_csv.zip')
    
    # Create CSV content with 20 records
    csv_content = """id,name,category,value
1,Product A,Electronics,100
2,Product B,Electronics,200
3,Product C,Furniture,300
4,Product D,Furniture,400
5,Product E,Electronics,500
6,Product F,Clothing,600
7,Product G,Clothing,700
8,Product H,Electronics,800
9,Product I,Furniture,900
10,Product J,Electronics,1000
11,Product K,Clothing,1100
12,Product L,Electronics,1200
13,Product M,Furniture,1300
14,Product N,Clothing,1400
15,Product O,Electronics,1500
16,Product P,Furniture,1600
17,Product Q,Clothing,1700
18,Product R,Electronics,1800
19,Product S,Furniture,1900
20,Product T,Clothing,2000
"""
    
    # Create JSONL content with 15 records
    jsonl_lines = []
    for i in range(1, 16):
        jsonl_lines.append(f'{{"order_id": {i}, "customer": "Customer {i}", "amount": {i * 50}}}')
    jsonl_content = '\n'.join(jsonl_lines) + '\n'
    
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        # Add CSV file
        zipf.writestr('data.csv', csv_content)
        # Add JSONL file  
        zipf.writestr('orders.jsonl', jsonl_content)
    
    print(f"Created {zip_path}")
    print(f"  - data.csv with 20 records")
    print(f"  - orders.jsonl with 15 records")
    print(f"  - Total: 35 records")

if __name__ == "__main__":
    create_zip_file()
