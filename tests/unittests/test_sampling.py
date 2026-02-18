"""
Unit tests for sampling functionality in tap-azure-cloud-storage.
Tests sample_rate, max_records, and max_files parameters with actual tap functions.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import io


class TestSampleFileFunction(unittest.TestCase):
    """Test the sample_file function with actual tap implementation"""

    def test_sample_rate_on_csv_file(self):
        """Test that sample_rate=5 samples every 5th record from CSV"""
        from tap_azure_cloud_storage import azure_storage

        # Create test CSV data with 20 rows
        csv_data = "id,name\n"
        for i in range(20):
            csv_data += f"{i},Item{i}\n"

        table_spec = {'delimiter': ','}
        sample_rate = 5
        max_records = 1000

        # Test the actual sample_file function
        records = list(azure_storage.sample_file(
            table_spec, 
            'test.csv', 
            csv_data.encode('utf-8'), 
            sample_rate, 
            'csv',
            max_records
        ))

        # Should get rows 0, 5, 10, 15 (4 records)
        self.assertEqual(len(records), 4)
        self.assertEqual(records[0]['id'], '0')
        self.assertEqual(records[1]['id'], '5')
        self.assertEqual(records[2]['id'], '10')
        self.assertEqual(records[3]['id'], '15')

    def test_sample_rate_on_jsonl_file(self):
        """Test that sample_rate=3 samples every 3rd record from JSONL"""
        from tap_azure_cloud_storage import azure_storage

        # Create test JSONL data with 10 rows
        jsonl_data = ""
        for i in range(10):
            jsonl_data += f'{{"id": {i}, "name": "Item{i}"}}\n'

        table_spec = {}
        sample_rate = 3
        max_records = 1000

        # Test the actual sample_file function
        records = list(azure_storage.sample_file(
            table_spec,
            'test.jsonl',
            jsonl_data.encode('utf-8'),
            sample_rate,
            'jsonl',
            max_records
        ))

        # Should get rows 0, 3, 6, 9 (4 records)
        self.assertEqual(len(records), 4)
        self.assertEqual(records[0]['id'], 0)
        self.assertEqual(records[1]['id'], 3)
        self.assertEqual(records[2]['id'], 6)
        self.assertEqual(records[3]['id'], 9)

    def test_max_records_limits_csv_sampling(self):
        """Test that max_records limits the number of sampled CSV records"""
        from tap_azure_cloud_storage import azure_storage

        # Create test CSV data with 100 rows
        csv_data = "id,value\n"
        for i in range(100):
            csv_data += f"{i},{i*10}\n"

        table_spec = {'delimiter': ','}
        sample_rate = 5
        max_records = 3  # Limit to 3 records

        # Test the actual sample_file function
        records = list(azure_storage.sample_file(
            table_spec,
            'test.csv',
            csv_data.encode('utf-8'),
            sample_rate,
            'csv',
            max_records
        ))

        # Should stop at 3 records
        self.assertEqual(len(records), 3)
        self.assertEqual(records[0]['id'], '0')
        self.assertEqual(records[1]['id'], '5')
        self.assertEqual(records[2]['id'], '10')


class TestGetFilesToSampleFunction(unittest.TestCase):
    """Test the get_files_to_sample function"""

    @patch('tap_azure_cloud_storage.azure_storage.setup_azure_client')
    def test_max_files_limits_file_processing(self, mock_setup_client):
        """Test that max_files limits the number of files processed"""
        from tap_azure_cloud_storage import azure_storage

        mock_client = MagicMock()
        
        # Mock returning a simple CSV content
        mock_client.open.return_value.__enter__.return_value.read.return_value = b"id,name\n1,test\n"
        mock_setup_client.return_value = mock_client

        # Create 10 mock Azure files
        azure_files = []
        for i in range(10):
            mock_file = MagicMock()
            mock_file.name = f'file{i}.csv'
            mock_file.last_modified = datetime(2026, 1, 10 + i)
            azure_files.append(mock_file)

        config = {'container_name': 'test-container'}
        max_files = 3

        # Test the actual get_files_to_sample function
        sampled_files = azure_storage.get_files_to_sample(config, azure_files, max_files)

        # Should only process 3 files
        self.assertEqual(len(sampled_files), 3)

    @patch('tap_azure_cloud_storage.azure_storage.setup_azure_client')
    def test_get_files_to_sample_processes_all_when_max_none(self, mock_setup_client):
        """Test that all files are processed when max_files is large"""
        from tap_azure_cloud_storage import azure_storage

        mock_client = MagicMock()
        mock_client.open.return_value.__enter__.return_value.read.return_value = b"id,name\n1,test\n"
        mock_setup_client.return_value = mock_client

        # Create 5 mock Azure files
        azure_files = []
        for i in range(5):
            mock_file = MagicMock()
            mock_file.name = f'file{i}.csv'
            mock_file.last_modified = datetime(2026, 1, 10 + i)
            azure_files.append(mock_file)

        config = {'container_name': 'test-container'}
        max_files = 100  # Large enough to process all

        # Test the actual get_files_to_sample function
        sampled_files = azure_storage.get_files_to_sample(config, azure_files, max_files)

        # Should process all 5 files
        self.assertEqual(len(sampled_files), 5)


class TestAzureFileOperations(unittest.TestCase):
    """Test Azure file operations for sampling"""

    @patch('tap_azure_cloud_storage.azure_storage.setup_azure_client')
    def test_get_file_handle_for_sampling(self, mock_setup_client):
        """Test getting a file handle from Azure for sampling"""
        from tap_azure_cloud_storage import azure_storage

        mock_client = MagicMock()
        mock_file_handle = MagicMock()
        mock_client.open.return_value = mock_file_handle
        mock_setup_client.return_value = mock_client

        config = {'container_name': 'test-container'}
        blob_path = 'test.csv'

        file_handle = azure_storage.get_file_handle(config, blob_path)

        self.assertIsNotNone(file_handle)
        mock_client.open.assert_called_once_with('test-container/test.csv', 'rb')


class TestSamplingEdgeCases(unittest.TestCase):
    """Test edge cases in sampling"""

    def test_empty_csv_returns_no_records(self):
        """Test that empty CSV files return no records"""
        from tap_azure_cloud_storage import azure_storage

        # Empty CSV with just headers
        csv_data = "id,name\n"

        table_spec = {'delimiter': ','}
        sample_rate = 1
        max_records = 1000

        records = list(azure_storage.sample_file(
            table_spec,
            'empty.csv',
            csv_data.encode('utf-8'),
            sample_rate,
            'csv',
            max_records
        ))

        self.assertEqual(len(records), 0)

    def test_single_record_csv(self):
        """Test sampling a CSV file with a single record"""
        from tap_azure_cloud_storage import azure_storage

        csv_data = "id,name\n1,Alice\n"

        table_spec = {'delimiter': ','}
        sample_rate = 1
        max_records = 1000

        records = list(azure_storage.sample_file(
            table_spec,
            'single.csv',
            csv_data.encode('utf-8'),
            sample_rate,
            'csv',
            max_records
        ))

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]['id'], '1')
        self.assertEqual(records[0]['name'], 'Alice')

    def test_sample_rate_larger_than_file_size(self):
        """Test when sample_rate is larger than the number of records"""
        from tap_azure_cloud_storage import azure_storage

        # CSV with only 3 records
        csv_data = "id,value\n1,100\n2,200\n3,300\n"

        table_spec = {'delimiter': ','}
        sample_rate = 10  # Larger than number of records
        max_records = 1000

        records = list(azure_storage.sample_file(
            table_spec,
            'small.csv',
            csv_data.encode('utf-8'),
            sample_rate,
            'csv',
            max_records
        ))

        # Should get only the first record (index 0)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]['id'], '1')


if __name__ == '__main__':
    unittest.main()
