"""
Unified Azure Blob Storage utility for managing container operations.

This module provides a command-line interface for Azure Blob Storage operations
including upload, list, and delete functionality.

Requirements:
    - adlfs Python package (or azure-storage-blob)
    - A valid config.json file with connection credentials and container name

Configuration:
    The config.json file must include:
    - connection_string OR (account_name + account_key) OR (tenant_id, client_id, client_secret)
    - container_name: Name of the Azure container to operate on

Usage:
    python azure_utils.py [--config CONFIG_FILE] COMMAND [OPTIONS]

Commands:
    upload      Upload local files to Azure container
    list        List blobs in Azure container
    delete      Delete blob(s) from Azure container

Examples:
    # Upload files to a specific prefix
    python azure_utils.py upload --prefix exports/my_table/ file1.csv file2.csv

    # Upload with custom config file
    python azure_utils.py --config my_custom_config.json upload --prefix exports/ file.csv

    # List all blobs under a prefix
    python azure_utils.py list --prefix exports/my_table/

    # List all blobs in container
    python azure_utils.py list

    # Delete a specific blob
    python azure_utils.py delete --blob exports/my_table/file1.csv

    # Delete all blobs under a prefix
    python azure_utils.py delete --prefix exports/my_table/

Notes:
    - Prefixes are automatically normalized (leading slashes removed)
    - Upload command preserves original filenames in destination
    - Delete with --prefix removes all matching blobs (use with caution)
    - All operations require valid Azure credentials in config file
"""
import argparse
import json
import os
from typing import List
import adlfs


def load_config(path: str) -> dict:
    """Load configuration from JSON file."""
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def get_fs_client_from_config(config: dict) -> adlfs.AzureBlobFileSystem:
    """Create Azure filesystem client from config."""
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

    return adlfs.AzureBlobFileSystem(**kwargs)


def cmd_upload(args):
    """Upload local files to Azure container."""
    config = load_config(args.config)
    container_name = config.get('container_name')
    if not container_name:
        raise ValueError('container_name missing in config')

    fs_client = get_fs_client_from_config(config)

    # Normalize prefix (no leading slash, ensure trailing slash if non-empty)
    prefix = args.prefix.lstrip('/')
    if prefix and not prefix.endswith('/'):
        prefix = prefix + '/'

    for local_path in args.files:
        if not os.path.isfile(local_path):
            print(f"Skipping {local_path}: not a file")
            continue
        filename = os.path.basename(local_path)
        blob_name = f"{prefix}{filename}" if prefix else filename
        blob_path = f"{container_name}/{blob_name}"

        print(f"Uploading {local_path} -> {container_name}/{blob_name}")

        with open(local_path, 'rb') as local_file:
            content = local_file.read()
            with fs_client.open(blob_path, 'wb') as remote_file:
                remote_file.write(content)

    print("Upload complete.")


def cmd_list(args):
    """List blobs in Azure container under a prefix."""
    config = load_config(args.config)
    container_name = config.get('container_name')
    if not container_name:
        raise SystemExit('container_name missing in config')

    fs_client = get_fs_client_from_config(config)

    # Normalize: strip leading slash
    prefix = (args.prefix or '').lstrip('/')

    # List files in the container with prefix
    search_path = f"{container_name}/{prefix}" if prefix else f"{container_name}/"

    total = 0
    try:
        files = fs_client.ls(search_path, detail=True)
        for file_info in files:
            # Extract just the blob name (remove container prefix)
            blob_name = file_info['name']
            if blob_name.startswith(f"{container_name}/"):
                blob_name = blob_name[len(container_name)+1:]
            print(blob_name)
            total += 1
    except FileNotFoundError:
        print(f"No files found under prefix: {prefix}")

    print(f"Total: {total}")


def cmd_delete(args):
    """Delete blob(s) from Azure container."""
    config = load_config(args.config)
    container_name = config.get('container_name')
    if not container_name:
        raise SystemExit('container_name missing in config')

    fs_client = get_fs_client_from_config(config)

    if args.blob:
        # Delete single blob
        blob_name = args.blob.lstrip('/')
        blob_path = f"{container_name}/{blob_name}"

        if fs_client.exists(blob_path):
            print(f"Deleting {container_name}/{blob_name}")
            fs_client.rm(blob_path)
            print("Delete complete.")
        else:
            print(f"Not found (skip): {container_name}/{blob_name}")
    else:
        # Delete by prefix
        prefix = (args.prefix or '').lstrip('/')
        search_path = f"{container_name}/{prefix}" if prefix else f"{container_name}/"

        try:
            # Use recursive=True to delete directories and all their contents
            print(f"Deleting all content under: {search_path}")
            fs_client.rm(search_path, recursive=True)

            # Also try to remove the directory marker itself if it exists
            # (needed for hierarchical namespace / ADLS Gen2)
            if prefix and not prefix.endswith('/'):
                dir_marker = f"{container_name}/{prefix}"
                try:
                    if fs_client.exists(dir_marker):
                        print(f"Removing directory marker: {prefix}")
                        fs_client.rm(dir_marker)
                except Exception:
                    pass  # Ignore if marker doesn't exist or can't be deleted

            print("Delete complete.")
        except FileNotFoundError:
            print("No blobs found under prefix.")


def main():
    parser = argparse.ArgumentParser(
        description='Unified Azure Blob Storage utility for container operations',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--config', default='config.json',
                       help='Path to config.json (with Azure credentials and container_name)')

    subparsers = parser.add_subparsers(dest='command', required=True, help='Command to execute')

    # Upload command
    upload_parser = subparsers.add_parser('upload', help='Upload local files to Azure container')
    upload_parser.add_argument('--prefix', default='',
                              help='Destination prefix in container, e.g. exports/my_table/')
    upload_parser.add_argument('files', nargs='+', help='Local file(s) to upload')
    upload_parser.set_defaults(func=cmd_upload)

    # List command
    list_parser = subparsers.add_parser('list', help='List blobs in Azure container')
    list_parser.add_argument('--prefix', default='', help='Filter by prefix')
    list_parser.set_defaults(func=cmd_list)

    # Delete command
    delete_parser = subparsers.add_parser('delete', help='Delete blob(s) from Azure container')
    delete_group = delete_parser.add_mutually_exclusive_group(required=True)
    delete_group.add_argument('--blob', help='Exact blob path to delete')
    delete_group.add_argument('--prefix', help='Delete all blobs under this prefix')
    delete_parser.set_defaults(func=cmd_delete)

    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
