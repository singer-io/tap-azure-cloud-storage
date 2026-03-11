"""
Helper script to create compressed Excel test file for integration tests.
Generates a .xlsx.gz file with sample employee data.
"""

import gzip
import shutil
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

def compress_excel_file():
    """Compress employees.xlsx to employees_compressed.xlsx.gz"""
    input_path = os.path.join(SCRIPT_DIR, 'employees.xlsx')
    output_path = os.path.join(SCRIPT_DIR, 'employees_compressed.xlsx.gz')

    if not os.path.exists(input_path):
        print(f"Error: {input_path} not found!")
        print("Please run create_excel_file.py first to generate employees.xlsx")
        return False

    # Compress the Excel file with original filename in header
    with open(input_path, 'rb') as f_in:
        with open(output_path, 'wb') as f_out_raw:
            with gzip.GzipFile(filename='employees.xlsx', mode='wb', fileobj=f_out_raw) as f_out:
                shutil.copyfileobj(f_in, f_out)

    # Get file sizes
    input_size = os.path.getsize(input_path)
    output_size = os.path.getsize(output_path)
    compression_ratio = (1 - output_size / input_size) * 100

    print(f"Created {output_path}")
    print(f"  - Original size: {input_size} bytes")
    print(f"  - Compressed size: {output_size} bytes")
    print(f"  - Compression ratio: {compression_ratio:.1f}%")

    return True

if __name__ == "__main__":
    if compress_excel_file():
        print("\nCompressed Excel test file is ready!")
    else:
        print("\nFailed to create compressed Excel file.")
