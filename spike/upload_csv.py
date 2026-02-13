#!/usr/bin/env python3
"""
Upload a CSV file to Azure Storage
"""

import json
import csv
import io
import adlfs

def create_sample_csv():
    """Create a sample CSV file content"""
    output = io.StringIO()
    writer = csv.writer(output)

    # Write header
    writer.writerow(['id', 'name', 'email', 'age', 'city', 'created_at'])

    # Write sample data
    writer.writerow(['1', 'John Doe', 'john.doe@example.com', '30', 'New York', '2024-01-15'])
    writer.writerow(['2', 'Jane Smith', 'jane.smith@example.com', '25', 'Los Angeles', '2024-01-16'])
    writer.writerow(['3', 'Bob Wilson', 'bob.wilson@example.com', '35', 'Chicago', '2024-01-17'])
    writer.writerow(['4', 'Alice Brown', 'alice.brown@example.com', '28', 'Houston', '2024-01-18'])
    writer.writerow(['5', 'Charlie Davis', 'charlie.davis@example.com', '42', 'Phoenix', '2024-01-19'])
    writer.writerow(['6', 'Diana Miller', 'diana.miller@example.com', '31', 'Philadelphia', '2024-01-20'])
    writer.writerow(['7', 'Eve Taylor', 'eve.taylor@example.com', '27', 'San Antonio', '2024-01-21'])
    writer.writerow(['8', 'Frank Anderson', 'frank.anderson@example.com', '39', 'San Diego', '2024-01-22'])
    writer.writerow(['9', 'Grace Thomas', 'grace.thomas@example.com', '33', 'Dallas', '2024-01-23'])
    writer.writerow(['10', 'Henry Jackson', 'henry.jackson@example.com', '29', 'San Jose', '2024-01-24'])

    return output.getvalue()

def upload_file():
    print("=" * 60)
    print("Uploading CSV file to Azure Storage")
    print("=" * 60)

    # Load config
    try:
        with open('config.json', 'r') as f:
            config = json.load(f)
        print("✓ Config loaded")
    except Exception as e:
        print(f"✗ Failed to load config: {e}")
        return False

    # Create Azure client
    try:
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
        print("✓ Connected to Azure")
    except Exception as e:
        print(f"✗ Failed to connect: {e}")
        return False

    # Upload file
    filename = "my_table_csv_fresh.csv"
    try:
        print(f"\n📤 Uploading {filename}...")
        csv_content = create_sample_csv()

        container_name = config['container_name']
        file_path = f'{container_name}/{filename}'

        with fs_client.open(file_path, 'wb') as f:
            f.write(csv_content.encode('utf-8'))

        print(f"✓ File uploaded successfully!")
        print(f"   Location: {container_name}/{filename}")
        print(f"   Size: {len(csv_content)} bytes")
        print(f"   Rows: 11 (including header)")
    except Exception as e:
        print(f"✗ Failed to upload: {e}")
        return False

    print("\n" + "=" * 60)
    print("✅ Upload complete!")
    print("=" * 60)
    return True

if __name__ == '__main__':
    success = upload_file()
    exit(0 if success else 1)
