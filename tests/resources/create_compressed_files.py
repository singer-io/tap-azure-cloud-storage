"""
Helper script to create compressed .gz files and copy required test files.
Run this script to generate the compressed test files and set up the test environment.
"""

import gzip
import shutil
import os

# Get the directory where this script is located
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.join(SCRIPT_DIR, '..', '..')

def compress_file(input_filename, output_filename):
    """Compress a file using gzip with original filename in header."""
    input_path = os.path.join(SCRIPT_DIR, input_filename)
    output_path = os.path.join(SCRIPT_DIR, output_filename)
    
    # Open the output file and create a GzipFile with the original filename in the header
    with open(input_path, 'rb') as f_in:
        with open(output_path, 'wb') as f_out_raw:
            with gzip.GzipFile(filename=input_filename, mode='wb', fileobj=f_out_raw) as f_out:
                shutil.copyfileobj(f_in, f_out)
    
    print(f"Created {output_filename} from {input_filename}")

def copy_zip_file():
    """Copy jsonl_csv.zip from project root to resources directory."""
    source = os.path.join(PROJECT_ROOT, 'jsonl_csv.zip')
    destination = os.path.join(SCRIPT_DIR, 'jsonl_csv.zip')
    
    if os.path.exists(source):
        shutil.copy2(source, destination)
        print(f"Copied jsonl_csv.zip to resources directory")
        return True
    else:
        print(f"Warning: jsonl_csv.zip not found at {source}")
        print("Please manually copy jsonl_csv.zip to tests/resources/ directory")
        return False

if __name__ == "__main__":
    print("Setting up test resources...\n")
    
    # Create compressed CSV file
    compress_file('gz_stored_as_csv.csv', 'sample_compressed_gz_file.gz')
    
    # Create compressed JSONL file
    compress_file('gz_stored_as_jsonl.jsonl', 'sample_compressed_gz_file_with_json_file_2_records.gz')
    
    # Copy ZIP file
    copy_zip_file()
    
    print("\nAll test resources are ready!")
    print("You can now run the tests.")
