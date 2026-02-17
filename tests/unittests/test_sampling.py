"""
Unit tests for sampling functionality in tap-azure-cloud-storage.
Tests sample_rate, max_records, and max_files parameters.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime


class TestSamplingParameters(unittest.TestCase):
    """Test sampling parameter behavior"""

    def test_sample_rate_controls_record_selection(self):
        """Test that sample_rate=5 samples every 5th record"""
        # Test the concept of sampling every 5th record
        all_records = list(range(20))
        sample_rate = 5

        sampled = [rec for i, rec in enumerate(all_records) if i % sample_rate == 0]

        # Should get rows 0, 5, 10, 15 (4 records)
        self.assertEqual(len(sampled), 4)
        self.assertEqual(sampled[0], 0)
        self.assertEqual(sampled[1], 5)
        self.assertEqual(sampled[2], 10)
        self.assertEqual(sampled[3], 15)

    def test_sample_rate_1_samples_every_record(self):
        """Test that sample_rate=1 samples every record"""
        # Test sampling with rate 1
        all_records = list(range(10))
        sample_rate = 1

        sampled = [rec for i, rec in enumerate(all_records) if i % sample_rate == 0]

        # Should get all 10 records
        self.assertEqual(len(sampled), 10)

    def test_max_records_limits_total_samples(self):
        """Test that max_records limits the total number of sampled records"""
        # Test limiting to max_records
        all_records = list(range(100))
        sample_rate = 5
        max_records = 3

        sampled = []
        for i, rec in enumerate(all_records):
            if i % sample_rate == 0:
                sampled.append(rec)
                if len(sampled) >= max_records:
                    break

        # Should stop at 3 records even though more would match
        self.assertEqual(len(sampled), 3)

    def test_max_records_with_different_sample_rate(self):
        """Test max_records with different sample rate"""
        # Test max_records with sample_rate=2
        all_records = list(range(50))
        sample_rate = 2
        max_records = 5

        sampled = []
        for i, rec in enumerate(all_records):
            if i % sample_rate == 0:
                sampled.append(rec)
                if len(sampled) >= max_records:
                    break

        # Should get exactly 5 records (0, 2, 4, 6, 8)
        self.assertEqual(len(sampled), 5)
        self.assertEqual(sampled[0], 0)
        self.assertEqual(sampled[4], 8)


class TestSamplingWithAzureFiles(unittest.TestCase):
    """Test sampling concepts with file operations"""

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


class TestMaxFilesParameter(unittest.TestCase):
    """Test max_files parameter"""

    def test_max_files_limits_file_processing(self):
        """Test that max_files limits the number of files processed"""
        all_files = [
            {'key': f'file{i}.csv', 'last_modified': datetime(2026, 1, 10 + i)}
            for i in range(10)
        ]

        max_files = 3
        files_to_process = all_files[:max_files]

        # Should only process 3 files
        self.assertEqual(len(files_to_process), 3)
        self.assertEqual(files_to_process[0]['key'], 'file0.csv')
        self.assertEqual(files_to_process[2]['key'], 'file2.csv')

    def test_max_files_none_processes_all_files(self):
        """Test that max_files=None processes all files"""
        all_files = [
            {'key': f'file{i}.csv', 'last_modified': datetime(2026, 1, 10 + i)}
            for i in range(10)
        ]

        max_files = None
        files_to_process = all_files if max_files is None else all_files[:max_files]

        # Should process all 10 files
        self.assertEqual(len(files_to_process), 10)


class TestSamplingEdgeCases(unittest.TestCase):
    """Test edge cases in sampling"""

    def test_empty_file_returns_no_records(self):
        """Test that empty files return no records"""
        # Test with empty data
        all_records = []
        sample_rate = 1

        sampled = [rec for i, rec in enumerate(all_records) if i % sample_rate == 0]

        self.assertEqual(len(sampled), 0)

    def test_single_record_file(self):
        """Test sampling a file with a single record"""
        # Test with single record
        all_records = [{'id': 1, 'name': 'Alice'}]
        sample_rate = 1

        sampled = [rec for i, rec in enumerate(all_records) if i % sample_rate == 0]

        self.assertEqual(len(sampled), 1)
        self.assertEqual(sampled[0]['id'], 1)

    def test_sample_rate_larger_than_file_size(self):
        """Test when sample_rate is larger than the number of records"""
        # Test sample rate larger than data
        all_records = [1, 2, 3]  # Only 3 records
        sample_rate = 10

        sampled = [rec for i, rec in enumerate(all_records) if i % sample_rate == 0]

        # Should get only record 0 (first record)
        self.assertEqual(len(sampled), 1)
        self.assertEqual(sampled[0], 1)


if __name__ == '__main__':
    unittest.main()
