import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone
import re


class TestFileMatching(unittest.TestCase):

    @patch('tap_azure_cloud_storage.azure_storage.list_files_in_container')
    def test_pattern_matches_csv_files(self, mock_list_files):
        """Test that CSV pattern matches .csv files via _iter_matching_blobs"""
        from tap_azure_cloud_storage import azure_storage

        config = {"container_name": "test-container"}
        table_spec = {"search_pattern": r'.*\.csv$'}

        # Create mock blob objects with name attribute
        mock_blobs = []
        for name in ["data.csv", "exports/2024/data.csv", "data.txt", "data.csv.gz"]:
            mock_blob = MagicMock()
            mock_blob.name = name
            mock_blobs.append(mock_blob)

        mock_list_files.return_value = mock_blobs

        matching_files = list(azure_storage._iter_matching_blobs(config, table_spec))
        matching_names = [f.name for f in matching_files]

        self.assertIn("data.csv", matching_names)
        self.assertIn("exports/2024/data.csv", matching_names)
        self.assertNotIn("data.txt", matching_names)
        self.assertNotIn("data.csv.gz", matching_names)

    @patch('tap_azure_cloud_storage.azure_storage.list_files_in_container')
    def test_pattern_matches_jsonl_files(self, mock_list_files):
        """Test that JSONL pattern matches .jsonl files via _iter_matching_blobs"""
        from tap_azure_cloud_storage import azure_storage

        config = {"container_name": "test-container"}
        table_spec = {"search_pattern": r'.*\.jsonl$'}

        # Create mock blob objects with name attribute
        mock_blobs = []
        for name in ["data.jsonl", "logs/app.jsonl", "data.json"]:
            mock_blob = MagicMock()
            mock_blob.name = name
            mock_blobs.append(mock_blob)

        mock_list_files.return_value = mock_blobs

        matching_files = list(azure_storage._iter_matching_blobs(config, table_spec))
        matching_names = [f.name for f in matching_files]

        self.assertIn("data.jsonl", matching_names)
        self.assertIn("logs/app.jsonl", matching_names)
        self.assertNotIn("data.json", matching_names)

    @patch('tap_azure_cloud_storage.azure_storage.list_files_in_container')
    def test_pattern_matches_parquet_files(self, mock_list_files):
        """Test that Parquet pattern matches .parquet files via _iter_matching_blobs"""
        from tap_azure_cloud_storage import azure_storage

        config = {"container_name": "test-container"}
        table_spec = {"search_pattern": r'.*\.parquet$'}

        # Create mock blob objects with name attribute
        mock_blobs = []
        for name in ["data.parquet", "warehouse/table.parquet", "data.csv"]:
            mock_blob = MagicMock()
            mock_blob.name = name
            mock_blobs.append(mock_blob)

        mock_list_files.return_value = mock_blobs

        matching_files = list(azure_storage._iter_matching_blobs(config, table_spec))
        matching_names = [f.name for f in matching_files]

        self.assertIn("data.parquet", matching_names)
        self.assertIn("warehouse/table.parquet", matching_names)
        self.assertNotIn("data.csv", matching_names)

    @patch('tap_azure_cloud_storage.azure_storage.list_files_in_container')
    def test_pattern_matches_avro_files(self, mock_list_files):
        """Test that Avro pattern matches .avro files via _iter_matching_blobs"""
        from tap_azure_cloud_storage import azure_storage

        config = {"container_name": "test-container"}
        table_spec = {"search_pattern": r'.*\.avro$'}

        # Create mock blob objects with name attribute
        mock_blobs = []
        for name in ["data.avro", "stream/events.avro", "data.parquet"]:
            mock_blob = MagicMock()
            mock_blob.name = name
            mock_blobs.append(mock_blob)

        mock_list_files.return_value = mock_blobs

        matching_files = list(azure_storage._iter_matching_blobs(config, table_spec))
        matching_names = [f.name for f in matching_files]

        self.assertIn("data.avro", matching_names)
        self.assertIn("stream/events.avro", matching_names)
        self.assertNotIn("data.parquet", matching_names)

    @patch('tap_azure_cloud_storage.azure_storage.list_files_in_container')
    def test_pattern_matches_excel_files(self, mock_list_files):
        """Test that Excel pattern matches .xlsx and .xls files via _iter_matching_blobs"""
        from tap_azure_cloud_storage import azure_storage

        config = {"container_name": "test-container"}
        table_spec_xlsx = {"search_pattern": r'.*\.xlsx$'}
        table_spec_xls = {"search_pattern": r'.*\.xls$'}

        # Create mock blob objects with name attribute
        mock_blobs = []
        for name in ["data.xlsx", "reports/2024/sales.xlsx", "old_data.xls", "data.csv"]:
            mock_blob = MagicMock()
            mock_blob.name = name
            mock_blobs.append(mock_blob)

        mock_list_files.return_value = mock_blobs

        matching_xlsx = list(azure_storage._iter_matching_blobs(config, table_spec_xlsx))
        matching_xlsx_names = [f.name for f in matching_xlsx]

        matching_xls = list(azure_storage._iter_matching_blobs(config, table_spec_xls))
        matching_xls_names = [f.name for f in matching_xls]

        self.assertIn("data.xlsx", matching_xlsx_names)
        self.assertIn("reports/2024/sales.xlsx", matching_xlsx_names)
        self.assertNotIn("old_data.xls", matching_xlsx_names)
        self.assertNotIn("data.csv", matching_xlsx_names)

        self.assertIn("old_data.xls", matching_xls_names)
        self.assertNotIn("data.xlsx", matching_xls_names)
        self.assertNotIn("data.csv", matching_xls_names)


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

        mock_client.find.return_value = {file1['name']: file1, file2['name']: file2}
        mock_setup_client.return_value = mock_client

        config = {'container_name': 'test-container', 'root_path': 'exports/'}
        files = list(azure_storage.list_files_in_container(config))

        # Should only get files with exports/ prefix
        self.assertEqual(len(files), 2)
        mock_client.find.assert_called_once_with('test-container/exports/', detail=True)

    @patch('tap_azure_cloud_storage.azure_storage.setup_azure_client')
    def test_prefix_normalization_removes_leading_slash_from_root_path(self, mock_setup_client):
        """Test that leading slash in root_path is removed before listing files"""
        from tap_azure_cloud_storage import azure_storage

        mock_client = MagicMock()
        mock_client.find.return_value = {}
        mock_setup_client.return_value = mock_client

        config = {
            'container_name': 'test-container',
            'root_path': '/exports/data/',
        }

        list(azure_storage.list_files_in_container(config))

        # Leading slash in root_path should be stripped when building the path
        mock_client.find.assert_called_once_with('test-container/exports/data/', detail=True)

    @patch('tap_azure_cloud_storage.azure_storage.setup_azure_client')
    def test_prefix_with_nested_folders_root_path(self, mock_setup_client):
        """Test that nested folder root_path is passed correctly to the client"""
        from tap_azure_cloud_storage import azure_storage

        mock_client = MagicMock()
        mock_client.find.return_value = {}
        mock_setup_client.return_value = mock_client

        config = {
            'container_name': 'test-container',
            'root_path': 'exports/2024/01/15/',
        }

        list(azure_storage.list_files_in_container(config))

        mock_client.find.assert_called_once_with('test-container/exports/2024/01/15/', detail=True)


if __name__ == '__main__':
    unittest.main()
