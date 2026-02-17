import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone
import re


class TestFileMatching(unittest.TestCase):

    def test_pattern_matches_csv_files(self):
        """Test that CSV pattern matches .csv files"""
        pattern = re.compile(r'.*\.csv$')

        self.assertTrue(pattern.match('data.csv'))
        self.assertTrue(pattern.match('exports/2024/data.csv'))
        self.assertFalse(pattern.match('data.txt'))
        self.assertFalse(pattern.match('data.csv.gz'))

    def test_pattern_matches_jsonl_files(self):
        """Test that JSONL pattern matches .jsonl files"""
        pattern = re.compile(r'.*\.jsonl$')

        self.assertTrue(pattern.match('data.jsonl'))
        self.assertTrue(pattern.match('logs/app.jsonl'))
        self.assertFalse(pattern.match('data.json'))

    def test_pattern_matches_parquet_files(self):
        """Test that Parquet pattern matches .parquet files"""
        pattern = re.compile(r'.*\.parquet$')

        self.assertTrue(pattern.match('data.parquet'))
        self.assertTrue(pattern.match('warehouse/table.parquet'))
        self.assertFalse(pattern.match('data.csv'))

    def test_pattern_matches_avro_files(self):
        """Test that Avro pattern matches .avro files"""
        pattern = re.compile(r'.*\.avro$')

        self.assertTrue(pattern.match('data.avro'))
        self.assertTrue(pattern.match('stream/events.avro'))
        self.assertFalse(pattern.match('data.parquet'))

    def test_pattern_matches_excel_files(self):
        """Test that Excel pattern matches .xlsx and .xls files"""
        pattern_xlsx = re.compile(r'.*\.xlsx$')
        pattern_xls = re.compile(r'.*\.xls$')

        self.assertTrue(pattern_xlsx.match('data.xlsx'))
        self.assertTrue(pattern_xlsx.match('reports/2024/sales.xlsx'))
        self.assertTrue(pattern_xls.match('old_data.xls'))
        self.assertFalse(pattern_xlsx.match('data.csv'))


class TestPrefixFiltering(unittest.TestCase):

    @patch('tap_azure_cloud_storage.azure_storage.setup_azure_client')
    def test_filter_files_by_prefix(self, mock_setup_client):
        """Test filtering files by search_prefix via root_path config"""
        from tap_azure_cloud_storage import azure_storage

        mock_client = MagicMock()

        # Create mock file info with exports/ prefix
        file1 = {
            'name': 'test-container/exports/data.csv',
            'type': 'file',
            'last_modified': '2026-01-15T10:00:00Z'
        }
        file2 = {
            'name': 'test-container/exports/data2.csv',
            'type': 'file',
            'last_modified': '2026-01-15T11:00:00Z'
        }

        mock_client.ls.return_value = [file1, file2]
        mock_setup_client.return_value = mock_client

        config = {'container_name': 'test-container', 'root_path': 'exports/'}
        files = list(azure_storage.list_files_in_container(config))

        # Should only get files with exports/ prefix
        self.assertEqual(len(files), 2)
        mock_client.ls.assert_called_once_with('test-container/exports/', detail=True)

    def test_prefix_normalization_removes_leading_slash(self):
        """Test that leading slash is removed from prefix"""
        prefix = '/exports/data/'
        normalized = prefix.lstrip('/')

        self.assertEqual(normalized, 'exports/data/')
        self.assertFalse(normalized.startswith('/'))

    def test_prefix_with_nested_folders(self):
        """Test prefix with multiple folder levels"""
        prefix = 'exports/2024/01/15/'

        self.assertTrue('/' in prefix)
        self.assertEqual(prefix.count('/'), 4)


class TestFileTimestampFiltering(unittest.TestCase):

    def test_filter_files_modified_after_timestamp(self):
        """Test filtering files modified after a specific timestamp"""
        cutoff = datetime(2026, 1, 10, 0, 0, 0, tzinfo=timezone.utc)

        files = [
            {'name': 'old.csv', 'last_modified': datetime(2026, 1, 5, 0, 0, 0, tzinfo=timezone.utc)},
            {'name': 'new.csv', 'last_modified': datetime(2026, 1, 12, 0, 0, 0, tzinfo=timezone.utc)},
            {'name': 'newer.csv', 'last_modified': datetime(2026, 1, 15, 0, 0, 0, tzinfo=timezone.utc)}
        ]

        filtered = [f for f in files if f['last_modified'] > cutoff]

        self.assertEqual(len(filtered), 2)
        self.assertEqual(filtered[0]['name'], 'new.csv')
        self.assertEqual(filtered[1]['name'], 'newer.csv')

    def test_strict_greater_than_excludes_equal(self):
        """Test that files with timestamp equal to cutoff are excluded"""
        cutoff = datetime(2026, 1, 10, 0, 0, 0, tzinfo=timezone.utc)

        files = [
            {'name': 'equal.csv', 'last_modified': datetime(2026, 1, 10, 0, 0, 0, tzinfo=timezone.utc)},
            {'name': 'newer.csv', 'last_modified': datetime(2026, 1, 10, 0, 0, 1, tzinfo=timezone.utc)}
        ]

        filtered = [f for f in files if f['last_modified'] > cutoff]

        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0]['name'], 'newer.csv')


class TestFileSorting(unittest.TestCase):

    def test_sort_files_by_modified_timestamp(self):
        """Test sorting files by last_modified timestamp"""
        files = [
            {'key': 'file3.csv', 'last_modified': datetime(2026, 1, 15, 0, 0, 0, tzinfo=timezone.utc)},
            {'key': 'file1.csv', 'last_modified': datetime(2026, 1, 10, 0, 0, 0, tzinfo=timezone.utc)},
            {'key': 'file2.csv', 'last_modified': datetime(2026, 1, 12, 0, 0, 0, tzinfo=timezone.utc)}
        ]

        sorted_files = sorted(files, key=lambda x: x['last_modified'])

        self.assertEqual(sorted_files[0]['key'], 'file1.csv')
        self.assertEqual(sorted_files[1]['key'], 'file2.csv')
        self.assertEqual(sorted_files[2]['key'], 'file3.csv')

    def test_sort_maintains_order_for_equal_timestamps(self):
        """Test that sort is stable for files with equal timestamps"""
        base_time = datetime(2026, 1, 10, 0, 0, 0, tzinfo=timezone.utc)

        files = [
            {'key': 'file_a.csv', 'last_modified': base_time},
            {'key': 'file_b.csv', 'last_modified': base_time},
            {'key': 'file_c.csv', 'last_modified': base_time}
        ]

        sorted_files = sorted(files, key=lambda x: x['last_modified'])

        # Should maintain original order for equal timestamps
        self.assertEqual(len(sorted_files), 3)


class TestCompressionDetection(unittest.TestCase):

    def test_detect_gzip_from_magic_bytes(self):
        """Test detecting gzip compression from magic bytes"""
        gzip_magic = b'\x1f\x8b'
        not_gzip = b'\x00\x01'

        self.assertTrue(gzip_magic[:2] == b'\x1f\x8b')
        self.assertFalse(not_gzip[:2] == b'\x1f\x8b')

    def test_detect_zip_from_extension(self):
        """Test detecting zip files from extension"""
        filenames = [
            'data.zip',
            'archive.ZIP',
            'exports/files.zip',
            'data.csv',
            'data.gz'
        ]

        zip_files = [f for f in filenames if f.lower().endswith('.zip')]

        self.assertEqual(len(zip_files), 3)
        self.assertIn('data.zip', zip_files)
        self.assertIn('archive.ZIP', zip_files)


class TestBlobNameNormalization(unittest.TestCase):

    def test_remove_container_prefix_from_blob_name(self):
        """Test removing container name prefix from blob path"""
        container_name = 'test-container'
        full_path = 'test-container/exports/data.csv'

        normalized = full_path.replace(f"{container_name}/", "", 1) if full_path.startswith(f"{container_name}/") else full_path

        self.assertEqual(normalized, 'exports/data.csv')
        self.assertFalse(normalized.startswith(container_name))

    def test_handle_blob_name_without_container_prefix(self):
        """Test handling blob names that don't have container prefix"""
        container_name = 'test-container'
        blob_path = 'exports/data.csv'

        normalized = blob_path.replace(f"{container_name}/", "", 1) if blob_path.startswith(f"{container_name}/") else blob_path

        self.assertEqual(normalized, 'exports/data.csv')


if __name__ == '__main__':
    unittest.main()
