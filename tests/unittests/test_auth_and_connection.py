import unittest
from unittest.mock import patch, MagicMock
from azure.core.exceptions import (
    ResourceNotFoundError,
    HttpResponseError,
    ServiceRequestError,
)
from tap_azure_cloud_storage.exceptions import AzureServiceUnavailableError


def _patch_backoff_sleep():
    """Patch backoff's sleep so retries run instantly in tests."""
    return patch('time.sleep', return_value=None)


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
            'storage_account_name': 'testaccount',
            'tenant_id': 'invalid-tenant-id',
            'client_id': 'invalid-client-id',
            'client_secret': 'invalid-client-secret',
            'container_name': 'test-container'
        }

        # Reset global fs
        azure_storage.fs = None

        with self.assertRaises(Exception) as context:
            azure_storage.setup_azure_client(config)
        self.assertIn("Failed to create Azure filesystem client", str(context.exception))


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

        mock_client.find.return_value = {file1['name']: file1, file2['name']: file2}
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

        mock_client.find.return_value = {file1['name']: file1}
        mock_setup_client.return_value = mock_client

        config = {'container_name': 'test-container', 'root_path': 'exports/'}

        files = list(azure_storage.list_files_in_container(config))

        self.assertEqual(len(files), 1)
        mock_client.find.assert_called_once_with('test-container/exports/', detail=True)

    @patch('tap_azure_cloud_storage.azure_storage.setup_azure_client')
    def test_connection_fails_with_invalid_container(self, mock_setup_client):
        """Test connection fails when container doesn't exist"""
        from tap_azure_cloud_storage import azure_storage

        mock_client = MagicMock()
        mock_client.find.side_effect = ResourceNotFoundError('Container not found')
        mock_setup_client.return_value = mock_client

        config = {'container_name': 'nonexistent-container'}

        # 404 errors are not retried; they propagate directly via raise_for_error
        with self.assertRaises(ResourceNotFoundError):
            list(azure_storage.list_files_in_container(config))

    @patch('tap_azure_cloud_storage.azure_storage.setup_azure_client')
    def test_connection_fails_with_no_permissions(self, mock_setup_client):
        """Test connection fails when service principal lacks permissions"""
        from tap_azure_cloud_storage import azure_storage

        mock_client = MagicMock()
        mock_client.find.side_effect = HttpResponseError('Access denied', response=MagicMock(status_code=403))
        mock_setup_client.return_value = mock_client

        config = {'container_name': 'forbidden-container'}

        # 403 errors are not retried; they propagate directly via raise_for_error
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

        with self.assertRaises(Exception) as context:
            azure_storage.get_file_handle(config, blob_path)
        self.assertIn("Failed to open streaming handle for nonexistent.csv", str(context.exception))


class TestConnectionRetry(unittest.TestCase):

    @staticmethod
    def _make_http_error(status_code, message="error"):
        """Helper to create an HttpResponseError with a given status code."""
        mock_response = MagicMock()
        mock_response.status_code = status_code
        mock_response.reason = message
        return HttpResponseError(message=message, response=mock_response)

    @_patch_backoff_sleep()
    @patch('tap_azure_cloud_storage.azure_storage.setup_azure_client')
    def test_list_files_retries_on_internal_server_error(self, mock_setup_client, mock_sleep):
        """Test that list_files_in_container retries on 500 InternalServerError."""
        from tap_azure_cloud_storage import azure_storage

        mock_client = MagicMock()
        mock_blob = {'name': 'test-container/file.csv', 'type': 'file', 'last_modified': '2026-01-01T00:00:00Z'}

        # First call raises 500, backoff retries and second call succeeds
        mock_client.find.side_effect = [
            self._make_http_error(500, 'Internal Server Error'),
            {mock_blob['name']: mock_blob},
        ]
        mock_setup_client.return_value = mock_client

        config = {'container_name': 'test-container'}

        files = list(azure_storage.list_files_in_container(config))

        self.assertEqual(len(files), 1)
        # find called twice: first fails, backoff retries, second succeeds
        self.assertEqual(mock_client.find.call_count, 2)

    @_patch_backoff_sleep()
    @patch('tap_azure_cloud_storage.azure_storage.setup_azure_client')
    def test_list_files_retries_on_service_unavailable(self, mock_setup_client, mock_sleep):
        """Test that list_files_in_container retries on 503 ServiceUnavailable."""
        from tap_azure_cloud_storage import azure_storage

        mock_client = MagicMock()
        mock_blob = {'name': 'test-container/data.csv', 'type': 'file', 'last_modified': '2026-01-01T00:00:00Z'}

        # First call raises 503, backoff retries and second call succeeds
        mock_client.find.side_effect = [
            self._make_http_error(503, 'Service Unavailable'),
            {mock_blob['name']: mock_blob},
        ]
        mock_setup_client.return_value = mock_client

        config = {'container_name': 'test-container'}

        files = list(azure_storage.list_files_in_container(config))

        self.assertEqual(len(files), 1)
        # find called twice: first fails, backoff retries, second succeeds
        self.assertEqual(mock_client.find.call_count, 2)

    @_patch_backoff_sleep()
    @patch('tap_azure_cloud_storage.azure_storage.setup_azure_client')
    def test_get_file_handle_retries_on_5xx(self, mock_setup_client, mock_sleep):
        """Test that get_file_handle retries on transient server errors."""
        from tap_azure_cloud_storage import azure_storage

        mock_client = MagicMock()
        mock_file_handle = MagicMock()

        # First open raises 503, second succeeds
        mock_client.open.side_effect = [
            self._make_http_error(503, 'Service Unavailable'),
            mock_file_handle,
        ]
        mock_setup_client.return_value = mock_client

        config = {'container_name': 'test-container'}

        result = azure_storage.get_file_handle(config, 'exports/data.csv')

        self.assertIsNotNone(result)
        self.assertEqual(mock_client.open.call_count, 2)

    @_patch_backoff_sleep()
    @patch('tap_azure_cloud_storage.azure_storage.setup_azure_client')
    def test_get_file_handle_raises_after_max_tries_exhausted(self, mock_setup_client, mock_sleep):
        """Test that get_file_handle raises after all tries are exhausted."""
        from tap_azure_cloud_storage import azure_storage

        mock_client = MagicMock()

        # Always fails with 503
        mock_client.open.side_effect = self._make_http_error(503, 'Service Unavailable')
        mock_setup_client.return_value = mock_client

        config = {'container_name': 'test-container'}

        with self.assertRaises(AzureServiceUnavailableError):
            azure_storage.get_file_handle(config, 'exports/data.csv')

        # Should have been called MAX_TRIES times (initial + retries)
        self.assertEqual(mock_client.open.call_count, azure_storage.MAX_TRIES)

    @_patch_backoff_sleep()
    @patch('tap_azure_cloud_storage.azure_storage.setup_azure_client')
    def test_get_file_handle_returns_non_retryable_error(self, mock_setup_client, mock_sleep):
        """Test that get_file_handle raises immediately on non-retryable 4xx errors."""
        from tap_azure_cloud_storage import azure_storage

        mock_client = MagicMock()
        mock_client.open.side_effect = ResourceNotFoundError('File not found')
        mock_setup_client.return_value = mock_client

        config = {'container_name': 'test-container'}

        with self.assertRaises(Exception):
            azure_storage.get_file_handle(config, 'exports/missing.csv')

        # Non-retryable errors: only called once
        self.assertEqual(mock_client.open.call_count, 1)

    @_patch_backoff_sleep()
    @patch('tap_azure_cloud_storage.azure_storage.setup_azure_client')
    def test_download_blob_retries_on_bad_gateway(self, mock_setup_client, mock_sleep):
        """Test that _download_blob_with_retry retries on 502 BadGateway."""
        from tap_azure_cloud_storage import azure_storage

        mock_client = MagicMock()
        mock_file = MagicMock()
        mock_file.read.return_value = b'file content'
        mock_file.__enter__ = lambda s: mock_file
        mock_file.__exit__ = MagicMock(return_value=False)

        # First open raises 502, second succeeds
        mock_client.open.side_effect = [
            self._make_http_error(502, 'Bad Gateway'),
            mock_file,
        ]
        mock_setup_client.return_value = mock_client

        result = azure_storage._download_blob_with_retry(mock_client, 'test-container', 'data.csv')

        self.assertEqual(result, b'file content')
        self.assertEqual(mock_client.open.call_count, 2)

    @_patch_backoff_sleep()
    @patch('tap_azure_cloud_storage.azure_storage.setup_azure_client')
    def test_download_blob_retries_on_rate_limit(self, mock_setup_client, mock_sleep):
        """Test that _download_blob_with_retry retries on 429 TooManyRequests."""
        from tap_azure_cloud_storage import azure_storage

        mock_client = MagicMock()
        mock_file = MagicMock()
        mock_file.read.return_value = b'data'
        mock_file.__enter__ = lambda s: mock_file
        mock_file.__exit__ = MagicMock(return_value=False)

        # First open raises 429, second succeeds
        mock_client.open.side_effect = [
            self._make_http_error(429, 'Too Many Requests'),
            mock_file,
        ]
        mock_setup_client.return_value = mock_client

        result = azure_storage._download_blob_with_retry(mock_client, 'test-container', 'data.csv')

        self.assertEqual(result, b'data')
        self.assertEqual(mock_client.open.call_count, 2)

    @_patch_backoff_sleep()
    @patch('tap_azure_cloud_storage.azure_storage.setup_azure_client')
    def test_download_blob_retries_on_gateway_timeout(self, mock_setup_client, mock_sleep):
        """Test that _download_blob_with_retry retries on 504 GatewayTimeout."""
        from tap_azure_cloud_storage import azure_storage

        mock_client = MagicMock()
        mock_file = MagicMock()
        mock_file.read.return_value = b'data'
        mock_file.__enter__ = lambda s: mock_file
        mock_file.__exit__ = MagicMock(return_value=False)

        # First two attempts raise 504, third succeeds
        mock_client.open.side_effect = [
            self._make_http_error(504, 'Gateway Timeout'),
            self._make_http_error(504, 'Gateway Timeout'),
            mock_file,
        ]
        mock_setup_client.return_value = mock_client

        result = azure_storage._download_blob_with_retry(mock_client, 'test-container', 'data.csv')

        self.assertEqual(result, b'data')
        self.assertEqual(mock_client.open.call_count, 3)

    @_patch_backoff_sleep()
    @patch('tap_azure_cloud_storage.azure_storage.setup_azure_client')
    def test_download_blob_non_retryable_error_propagates_immediately(self, mock_setup_client, mock_sleep):
        """Test that non-retryable errors (4xx) are not retried."""
        from tap_azure_cloud_storage import azure_storage

        mock_client = MagicMock()
        mock_client.open.side_effect = ResourceNotFoundError('Not found')

        with self.assertRaises(ResourceNotFoundError):
            azure_storage._download_blob_with_retry(mock_client, 'test-container', 'missing.csv')

        # Should only be called once - no retries for non-retryable errors
        self.assertEqual(mock_client.open.call_count, 1)

    @_patch_backoff_sleep()
    @patch('tap_azure_cloud_storage.azure_storage.setup_azure_client')
    def test_list_files_retries_on_connection_error(self, mock_setup_client, mock_sleep):
        """Test that list_files_in_container retries on transient ServiceRequestError."""
        from tap_azure_cloud_storage import azure_storage

        mock_client = MagicMock()
        mock_blob = {'name': 'test-container/file.csv', 'type': 'file', 'last_modified': '2026-01-01T00:00:00Z'}

        mock_client.find.side_effect = [
            ServiceRequestError('Connection error'),
            {mock_blob['name']: mock_blob},
        ]
        mock_setup_client.return_value = mock_client

        config = {'container_name': 'test-container'}

        files = list(azure_storage.list_files_in_container(config))

        self.assertEqual(len(files), 1)
        self.assertEqual(mock_client.find.call_count, 2)


if __name__ == '__main__':
    unittest.main()
