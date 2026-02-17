import unittest
from unittest.mock import patch, MagicMock
from azure.core.exceptions import ResourceNotFoundError, HttpResponseError


class TestAzureAuthentication(unittest.TestCase):

    @patch('adlfs.AzureBlobFileSystem')
    def test_successful_authentication_with_connection_string(self, mock_fs):
        """Test successful authentication with connection string"""
        from tap_azure_cloud_storage import azure_storage

        # Reset global fs to ensure fresh authentication
        azure_storage.fs = None

        mock_fs.return_value = MagicMock()

        config = {
            'connection_string': 'DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key;EndpointSuffix=core.windows.net',
            'container_name': 'test-container'
        }

        client = azure_storage.setup_azure_client(config)

        self.assertIsNotNone(client)
        mock_fs.assert_called_once()

    @patch('adlfs.AzureBlobFileSystem')
    def test_successful_authentication_with_service_principal(self, mock_fs):
        """Test successful authentication with service principal credentials"""
        from tap_azure_cloud_storage import azure_storage

        mock_fs.return_value = MagicMock()

        config = {
            'storage_account_name': 'testaccount',
            'tenant_id': 'test-tenant-id',
            'client_id': 'test-client-id',
            'client_secret': 'test-client-secret',
            'container_name': 'test-container'
        }

        # Reset global fs
        azure_storage.fs = None

        client = azure_storage.setup_azure_client(config)

        self.assertIsNotNone(client)
        mock_fs.assert_called_once()

    @patch('adlfs.AzureBlobFileSystem')
    def test_authentication_with_account_key(self, mock_fs):
        """Test authentication with account key"""
        from tap_azure_cloud_storage import azure_storage

        mock_fs.return_value = MagicMock()

        config = {
            'storage_account_name': 'testaccount',
            'account_key': 'test-account-key',
            'container_name': 'test-container'
        }

        # Reset global fs
        azure_storage.fs = None

        client = azure_storage.setup_azure_client(config)

        self.assertIsNotNone(client)
        mock_fs.assert_called_once()

    @patch('adlfs.AzureBlobFileSystem')
    def test_authentication_failure_with_invalid_credentials(self, mock_fs):
        """Test authentication fails with invalid credentials"""
        from tap_azure_cloud_storage import azure_storage

        mock_fs.side_effect = ValueError('Invalid credentials')

        config = {
            'connection_string': 'invalid-connection-string',
            'container_name': 'test-container'
        }

        # Reset global fs
        azure_storage.fs = None

        with self.assertRaises(ValueError):
            azure_storage.setup_azure_client(config)


class TestAzureConnection(unittest.TestCase):

    @patch('tap_azure_cloud_storage.azure_storage.setup_azure_client')
    def test_list_files_in_container_success(self, mock_setup_client):
        """Test successfully listing files in an Azure container"""
        from tap_azure_cloud_storage import azure_storage

        mock_client = MagicMock()

        # Create mock file info
        file1 = {
            'name': 'test-container/file1.csv',
            'type': 'file',
            'last_modified': '2026-01-15T10:00:00Z'
        }
        file2 = {
            'name': 'test-container/file2.csv',
            'type': 'file',
            'last_modified': '2026-01-15T11:00:00Z'
        }

        mock_client.ls.return_value = [file1, file2]
        mock_setup_client.return_value = mock_client

        config = {'container_name': 'test-container'}

        files = list(azure_storage.list_files_in_container(config))

        self.assertEqual(len(files), 2)
        self.assertEqual(files[0].name, 'file1.csv')
        self.assertEqual(files[1].name, 'file2.csv')

    @patch('tap_azure_cloud_storage.azure_storage.setup_azure_client')
    def test_list_files_with_prefix(self, mock_setup_client):
        """Test listing files with a specific prefix via root_path config"""
        from tap_azure_cloud_storage import azure_storage

        mock_client = MagicMock()

        file1 = {
            'name': 'test-container/exports/data.csv',
            'type': 'file',
            'last_modified': '2026-01-15T10:00:00Z'
        }

        mock_client.ls.return_value = [file1]
        mock_setup_client.return_value = mock_client

        config = {'container_name': 'test-container', 'root_path': 'exports/'}

        files = list(azure_storage.list_files_in_container(config))

        self.assertEqual(len(files), 1)
        mock_client.ls.assert_called_once_with('test-container/exports/', detail=True)

    @patch('tap_azure_cloud_storage.azure_storage.setup_azure_client')
    def test_connection_fails_with_invalid_container(self, mock_setup_client):
        """Test connection fails when container doesn't exist"""
        from tap_azure_cloud_storage import azure_storage

        mock_client = MagicMock()
        mock_client.ls.side_effect = ResourceNotFoundError('Container not found')
        mock_setup_client.return_value = mock_client

        config = {'container_name': 'nonexistent-container'}

        with self.assertRaises(ResourceNotFoundError):
            list(azure_storage.list_files_in_container(config))

    @patch('tap_azure_cloud_storage.azure_storage.setup_azure_client')
    def test_connection_fails_with_no_permissions(self, mock_setup_client):
        """Test connection fails when service principal lacks permissions"""
        from tap_azure_cloud_storage import azure_storage

        mock_client = MagicMock()
        mock_client.ls.side_effect = HttpResponseError('Access denied', response=MagicMock(status_code=403))
        mock_setup_client.return_value = mock_client

        config = {'container_name': 'forbidden-container'}

        with self.assertRaises(HttpResponseError):
            list(azure_storage.list_files_in_container(config))


class TestAzureFileOperations(unittest.TestCase):

    @patch('tap_azure_cloud_storage.azure_storage.setup_azure_client')
    def test_get_file_handle_success(self, mock_setup_client):
        """Test successfully getting a file handle from Azure"""
        from tap_azure_cloud_storage import azure_storage

        mock_client = MagicMock()
        mock_file_handle = MagicMock()
        mock_client.open.return_value = mock_file_handle
        mock_setup_client.return_value = mock_client

        config = {'container_name': 'test-container'}
        blob_path = 'exports/data.csv'

        file_handle = azure_storage.get_file_handle(config, blob_path)

        self.assertIsNotNone(file_handle)
        mock_client.open.assert_called_once_with('test-container/exports/data.csv', 'rb')

    @patch('tap_azure_cloud_storage.azure_storage.setup_azure_client')
    def test_get_file_handle_failure(self, mock_setup_client):
        """Test handling failure when file doesn't exist"""
        from tap_azure_cloud_storage import azure_storage

        mock_client = MagicMock()
        mock_client.open.side_effect = Exception('File not found')
        mock_setup_client.return_value = mock_client

        config = {'container_name': 'test-container'}
        blob_path = 'nonexistent.csv'

        file_handle = azure_storage.get_file_handle(config, blob_path)

        self.assertIsNone(file_handle)


if __name__ == '__main__':
    unittest.main()
