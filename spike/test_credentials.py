#!/usr/bin/env python3
"""
Test Azure credentials by uploading a sample CSV file and reading it back
"""

import json
import io
import csv
from datetime import datetime
import adlfs

def create_sample_csv():
    """Create a sample CSV file in memory"""
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['id', 'name', 'email', 'created_at'])
    writer.writerow(['1', 'John Doe', 'john@example.com', '2024-01-01'])
    writer.writerow(['2', 'Jane Smith', 'jane@example.com', '2024-01-02'])
    writer.writerow(['3', 'Bob Wilson', 'bob@example.com', '2024-01-03'])
    return output.getvalue()

def test_credentials():
    print("=" * 60)
    print("Testing Azure Storage Credentials")
    print("=" * 60)

    # Load config
    try:
        with open('config.json', 'r') as f:
            config = json.load(f)
        print("✓ Config loaded successfully")
        print(f"  Storage Account: {config.get('storage_account_name')}")
        print(f"  Container: {config.get('container_name')}")
    except Exception as e:
        print(f"✗ Failed to load config: {e}")
        return False

    # Create Azure client directly with adlfs
    try:
        print("\n🔗 Creating Azure connection...")

        # Build connection parameters
        kwargs = {}
        if config.get('connection_string'):
            kwargs['connection_string'] = config['connection_string']
        else:
            kwargs['account_name'] = config.get('storage_account_name')
            if config.get('account_key'):
                kwargs['account_key'] = config['account_key']
            if config.get('tenant_id'):
                kwargs['tenant_id'] = config['tenant_id']
            if config.get('client_id'):
                kwargs['client_id'] = config['client_id']
            if config.get('client_secret'):
                kwargs['client_secret'] = config['client_secret']

        fs_client = adlfs.AzureBlobFileSystem(**kwargs)
        print("✓ Azure client created successfully")
    except Exception as e:
        print(f"✗ Failed to create Azure client: {e}")
        return False

    # Test listing files
    try:
        print("\n📂 Listing existing files in container...")
        container_name = config['container_name']
        files = fs_client.ls(container_name, detail=True)

        # Filter only files (not directories)
        file_list = [f for f in files if f['type'] == 'file']
        print(f"✓ Found {len(file_list)} existing file(s) in container")

        for i, file_info in enumerate(file_list[:5], 1):  # Show first 5 files
            name = file_info['name'].replace(f"{container_name}/", "", 1)
            print(f"  {i}. {name}")
        if len(file_list) > 5:
            print(f"  ... and {len(file_list) - 5} more files")
    except Exception as e:
        print(f"✗ Failed to list files: {e}")
        return False

    # Create and upload test CSV
    test_filename = f"test_upload_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    try:
        print(f"\n📤 Uploading test file: {test_filename}")
        csv_content = create_sample_csv()

        with fs_client.open(f'{container_name}/{test_filename}', 'wb') as f:
            f.write(csv_content.encode('utf-8'))

        print(f"✓ Test file uploaded successfully")
    except Exception as e:
        print(f"✗ Failed to upload test file: {e}")
        return False

    # Verify upload by reading it back
    try:
        print(f"\n📥 Reading back test file to verify...")
        with fs_client.open(f'{container_name}/{test_filename}', 'rb') as f:
            content = f.read().decode('utf-8')

        lines = content.strip().split('\n')
        print(f"✓ File read successfully - {len(lines)} lines")
        print(f"  Header: {lines[0]}")
        print(f"  First row: {lines[1]}")
    except Exception as e:
        print(f"✗ Failed to read test file: {e}")
        return False

    # Cleanup - delete test file
    try:
        print(f"\n🗑️  Cleaning up test file...")
        fs_client.rm(f'{container_name}/{test_filename}')
        print(f"✓ Test file deleted successfully")
    except Exception as e:
        print(f"⚠️  Warning: Could not delete test file: {e}")
        print(f"   You may need to manually delete: {test_filename}")

    print("\n" + "=" * 60)
    print("✅ ALL TESTS PASSED - Credentials are working correctly!")
    print("=" * 60)
    return True

if __name__ == '__main__':
    success = test_credentials()
    exit(0 if success else 1)
