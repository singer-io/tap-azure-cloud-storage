import os
import json
import re
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient


def _get_blob_service_client():
    """
    Create a BlobServiceClient using Service Principal credentials
    from environment variables.

    Required environment variables:
    - TAP_AZURE_TENANT_ID
    - TAP_AZURE_CLIENT_ID
    - TAP_AZURE_CLIENT_SECRET
    - TAP_AZURE_STORAGE_ACCOUNT_NAME
    """
    tenant_id = os.getenv('TAP_AZURE_TENANT_ID')
    client_id = os.getenv('TAP_AZURE_CLIENT_ID')
    client_secret = os.getenv('TAP_AZURE_CLIENT_SECRET')
    account_name = os.getenv('TAP_AZURE_STORAGE_ACCOUNT_NAME')

    if not all([tenant_id, client_id, client_secret, account_name]):
        raise ValueError(
            "Service Principal environment variables are not set. "
            "Required: TAP_AZURE_TENANT_ID, TAP_AZURE_CLIENT_ID, "
            "TAP_AZURE_CLIENT_SECRET, TAP_AZURE_STORAGE_ACCOUNT_NAME"
        )

    credential = ClientSecretCredential(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret,
    )
    account_url = f"https://{account_name}.blob.core.windows.net"
    return BlobServiceClient(account_url=account_url, credential=credential)


def get_resources_path(file_path, folder_path=None):
    """
    Get the full path to a resource file in the tests/resources directory.

    Args:
        file_path (str): Name of the resource file
        folder_path (str, optional): Subfolder within resources directory

    Returns:
        str: Full path to the resource file
    """
    if folder_path:
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), 'resources', folder_path, file_path)
    else:
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), 'resources', file_path)


def delete_and_push_file(properties, resource_names, folder_path=None, search_prefix_index=0):
    """
    Delete the file from Azure Blob Storage first and then upload it again.

    Args:
        properties (dict): config.json
        resource_names (list): List of file names (available in resources directory)
        folder_path (str, optional): Subfolder within resources directory
        search_prefix_index (int): Index of the table in tables config
    """
    # Initialize Azure Blob Service Client using Service Principal
    blob_service_client = _get_blob_service_client()

    # Parse the tables configuration
    tables = json.loads(properties['tables'])
    container_name = properties['container_name']

    search_prefix = (tables[search_prefix_index].get('search_prefix', '') or '').strip('/')
    search_pattern = tables[search_prefix_index].get('search_pattern')
    regex = re.compile(search_pattern) if search_pattern else None

    for resource_name in resource_names:
        # Construct the Azure blob path
        if search_prefix:
            blob_path = search_prefix + '/' + resource_name
        else:
            blob_path = resource_name

        if regex and not regex.search(blob_path):
            raise ValueError(
                f"Constructed blob path '{blob_path}' does not match table search_pattern '{search_pattern}'. "
                "Ensure test search_prefix/search_pattern are aligned with uploaded fixture paths."
            )

        # Attempt to delete the file before we start
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)
        print(f"Attempting to delete Azure blob: {blob_path}")
        try:
            blob_client.delete_blob()
            print(f"Deleted existing file: {blob_path}")
        except Exception as e:
            print(f"Azure blob does not exist or could not be deleted: {e}")

        # Upload file to Azure Blob Storage
        local_file_path = get_resources_path(resource_name, folder_path)
        with open(local_file_path, 'rb') as data:
            blob_client.upload_blob(data, overwrite=True)
        print(f"Uploaded file: {blob_path}")


def download_blob_bytes(config, blob_path):
    """
    Download blob content from Azure Blob Storage as bytes.

    Args:
        config (dict): Configuration with container_name
        blob_path (str): Path to the file in Azure Blob Storage

    Returns:
        bytes: Full blob content as bytes
    """
    container_name = config['container_name']

    blob_service_client = _get_blob_service_client()
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)

    return blob_client.download_blob().readall()


def delete_azure_blob(properties, blob_name, search_prefix_index=0):
    """
    Delete a specific blob from Azure Blob Storage.

    Args:
        properties (dict): Configuration properties
        blob_name (str): Name of the blob to delete
        search_prefix_index (int): Index of the table in tables config
    """
    blob_service_client = _get_blob_service_client()

    # Parse the tables configuration
    tables = json.loads(properties['tables'])
    container_name = properties['container_name']
    search_prefix = (tables[search_prefix_index].get('search_prefix', '') or '').strip('/')

    # Construct the blob path
    if search_prefix:
        blob_path = f"{search_prefix}/{blob_name}"
    else:
        blob_path = blob_name

    # Delete the blob
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)
    try:
        blob_client.delete_blob()
        print(f"Deleted blob: {blob_path}")
    except Exception as e:
        print(f"Blob does not exist or could not be deleted: {e}")
