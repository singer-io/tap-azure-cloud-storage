import os
import json
from azure.storage.blob import BlobServiceClient
from tap_tester import menagerie, connections


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
    # Initialize Azure Blob Service Client from connection string
    connection_string = os.getenv('TAP_AZURE_CONNECTION_STRING')
    if not connection_string:
        raise ValueError("TAP_AZURE_CONNECTION_STRING environment variable is not set")

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Parse the tables configuration
    tables = json.loads(properties['tables'])
    container_name = properties['container_name']
    container_client = blob_service_client.get_container_client(container_name)

    for resource_name in resource_names:
        # Construct the Azure blob path
        search_prefix = tables[search_prefix_index].get('search_prefix', '')
        if search_prefix:
            blob_path = search_prefix + '/' + resource_name
        else:
            blob_path = resource_name

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


def get_file_handle(config, blob_path):
    """
    Get a file handle for reading from Azure Blob Storage.

    Args:
        config (dict): Configuration with connection_string and container_name
        blob_path (str): Path to the file in Azure Blob Storage

    Returns:
        File-like object: Readable stream from Azure Blob Storage
    """
    connection_string = config['connection_string']
    container_name = config['container_name']

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
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
    connection_string = os.getenv('TAP_AZURE_CONNECTION_STRING')
    if not connection_string:
        raise ValueError("TAP_AZURE_CONNECTION_STRING environment variable is not set")

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Parse the tables configuration
    tables = json.loads(properties['tables'])
    container_name = properties['container_name']
    search_prefix = tables[search_prefix_index].get('search_prefix', '')

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
