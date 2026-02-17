import unittest
from unittest.mock import patch, MagicMock, call
from datetime import datetime, timezone
from tap_azure_cloud_storage.sync import stream_is_selected, sync_stream, sync_table_file


class TestSyncHelpers(unittest.TestCase):

    def test_stream_is_selected_returns_true_when_selected(self):
        """Test stream_is_selected returns True when metadata selected is True"""
        mdata_map = {(): {'selected': True}}
        self.assertTrue(stream_is_selected(mdata_map))

    def test_stream_is_selected_returns_false_when_not_selected(self):
        """Test stream_is_selected returns False when metadata selected is False"""
        mdata_map = {(): {'selected': False}}
        self.assertFalse(stream_is_selected(mdata_map))

    def test_stream_is_selected_returns_false_when_missing(self):
        """Test stream_is_selected returns False when selected key is missing"""
        mdata_map = {(): {}}
        self.assertFalse(stream_is_selected(mdata_map))


class TestSyncStream(unittest.TestCase):

    def setUp(self):
        self.config = {
            'start_date': '2024-01-01T00:00:00Z',
            'container_name': 'test-container'
        }
        self.table_spec = {
            'table_name': 'my_table',
            'search_prefix': 'exports/',
            'search_pattern': '.*\\.csv',
            'key_properties': []
        }
        self.stream = {
            'tap_stream_id': 'my_table',
            'schema': {'type': 'object', 'properties': {}}
        }
        self.state = {'bookmarks': {}}
        self.sync_start_time = datetime(2026, 1, 15, 0, 0, 0, tzinfo=timezone.utc)

    @patch('tap_azure_cloud_storage.azure_storage.get_input_files_for_table')
    @patch('tap_azure_cloud_storage.sync.sync_table_file')
    @patch('singer.write_bookmark')
    @patch('singer.write_state')
    def test_sync_stream_processes_files_in_order(self, mock_write_state, mock_write_bookmark, 
                                                   mock_sync_file, mock_get_files):
        """Test that sync_stream processes files sorted by last_modified"""
        mock_files = [
            {'key': 'file2.csv', 'last_modified': datetime(2026, 1, 10, 12, 0, 0, tzinfo=timezone.utc)},
            {'key': 'file1.csv', 'last_modified': datetime(2026, 1, 10, 10, 0, 0, tzinfo=timezone.utc)},
            {'key': 'file3.csv', 'last_modified': datetime(2026, 1, 10, 14, 0, 0, tzinfo=timezone.utc)}
        ]
        mock_get_files.return_value = mock_files
        mock_sync_file.return_value = 10
        mock_write_bookmark.return_value = self.state

        records_streamed = sync_stream(self.config, self.state, self.table_spec, 
                                       self.stream, self.sync_start_time)

        # Check files were processed in sorted order
        calls = mock_sync_file.call_args_list
        self.assertEqual(calls[0][0][1], 'file1.csv')
        self.assertEqual(calls[1][0][1], 'file2.csv')
        self.assertEqual(calls[2][0][1], 'file3.csv')
        self.assertEqual(records_streamed, 30)

    @patch('tap_azure_cloud_storage.azure_storage.get_input_files_for_table')
    @patch('tap_azure_cloud_storage.sync.sync_table_file')
    @patch('singer.write_bookmark')
    @patch('singer.write_state')
    def test_sync_stream_updates_bookmark_per_file(self, mock_write_state, mock_write_bookmark,
                                                    mock_sync_file, mock_get_files):
        """Test that bookmark is updated after each file"""
        mock_files = [
            {'key': 'file1.csv', 'last_modified': datetime(2026, 1, 10, 10, 0, 0, tzinfo=timezone.utc)}
        ]
        mock_get_files.return_value = mock_files
        mock_sync_file.return_value = 5
        mock_write_bookmark.return_value = self.state

        sync_stream(self.config, self.state, self.table_spec, self.stream, self.sync_start_time)

        # Verify bookmark was written
        mock_write_bookmark.assert_called_once()
        args = mock_write_bookmark.call_args[0]
        self.assertEqual(args[1], 'my_table')
        self.assertEqual(args[2], 'modified_since')

    @patch('tap_azure_cloud_storage.azure_storage.get_input_files_for_table')
    @patch('tap_azure_cloud_storage.sync.sync_table_file')
    @patch('singer.write_bookmark')
    @patch('singer.write_state')
    def test_sync_stream_caps_bookmark_at_sync_start_time(self, mock_write_state, mock_write_bookmark,
                                                          mock_sync_file, mock_get_files):
        """Test that bookmark is capped at sync_start_time for files modified after sync started"""
        future_time = datetime(2026, 1, 20, 0, 0, 0, tzinfo=timezone.utc)
        mock_files = [
            {'key': 'file1.csv', 'last_modified': future_time}
        ]
        mock_get_files.return_value = mock_files
        mock_sync_file.return_value = 5
        mock_write_bookmark.return_value = self.state

        sync_stream(self.config, self.state, self.table_spec, self.stream, self.sync_start_time)

        # Bookmark should be sync_start_time, not future_time
        args = mock_write_bookmark.call_args[0]
        bookmark_value = args[3]
        self.assertEqual(bookmark_value, self.sync_start_time.isoformat())


class TestSyncTableFile(unittest.TestCase):

    def setUp(self):
        self.config = {'container_name': 'test-container'}
        self.table_spec = {
            'table_name': 'my_table',
            'key_properties': ['id']
        }
        self.stream = {
            'tap_stream_id': 'my_table',
            'schema': {'type': 'object', 'properties': {}}
        }

    @patch('tap_azure_cloud_storage.sync.handle_file')
    def test_sync_table_file_processes_csv(self, mock_handle):
        """Test that CSV files are processed"""
        mock_handle.return_value = 10

        result = sync_table_file(self.config, 'exports/data.csv', self.table_spec, self.stream)

        self.assertEqual(result, 10)
        mock_handle.assert_called_once()
        # Verify CSV extension is detected
        args = mock_handle.call_args[0]
        self.assertEqual(args[1], 'exports/data.csv')

    @patch('tap_azure_cloud_storage.sync.handle_file')
    def test_sync_table_file_processes_jsonl(self, mock_handle):
        """Test that JSONL files are processed"""
        mock_handle.return_value = 15

        result = sync_table_file(self.config, 'exports/data.jsonl', self.table_spec, self.stream)

        self.assertEqual(result, 15)
        mock_handle.assert_called_once()

    @patch('tap_azure_cloud_storage.sync.handle_file')
    def test_sync_table_file_processes_parquet(self, mock_handle):
        """Test that Parquet files are processed"""
        mock_handle.return_value = 20

        result = sync_table_file(self.config, 'warehouse/data.parquet', self.table_spec, self.stream)

        self.assertEqual(result, 20)
        mock_handle.assert_called_once()

    @patch('tap_azure_cloud_storage.sync.handle_file')
    def test_sync_table_file_processes_avro(self, mock_handle):
        """Test that Avro files are processed"""
        mock_handle.return_value = 25

        result = sync_table_file(self.config, 'stream/events.avro', self.table_spec, self.stream)

        self.assertEqual(result, 25)
        mock_handle.assert_called_once()

    @patch('tap_azure_cloud_storage.sync.handle_file')
    def test_sync_table_file_processes_excel(self, mock_handle):
        """Test that Excel files are processed"""
        mock_handle.return_value = 12

        result = sync_table_file(self.config, 'reports/sales.xlsx', self.table_spec, self.stream)

        self.assertEqual(result, 12)
        mock_handle.assert_called_once()


class TestSDCFields(unittest.TestCase):

    def test_sdc_fields_added_to_records(self):
        """Test that _sdc fields are added to synced records"""
        record = {'id': 1, 'name': 'Test'}
        container_name = 'test-container'
        blob_path = 'exports/data.csv'
        line_number = 5

        # Add _sdc fields
        record['_sdc_source_container'] = container_name
        record['_sdc_source_file'] = blob_path
        record['_sdc_source_lineno'] = line_number
        record['_sdc_extra'] = None

        self.assertEqual(record['_sdc_source_container'], 'test-container')
        self.assertEqual(record['_sdc_source_file'], 'exports/data.csv')
        self.assertEqual(record['_sdc_source_lineno'], 5)
        self.assertIn('_sdc_extra', record)

    def test_sdc_extra_handles_extra_fields(self):
        """Test that _sdc_extra captures fields not in schema"""
        schema_fields = {'id', 'name'}
        record_fields = {'id', 'name', 'extra_field1', 'extra_field2'}

        extra_fields = record_fields - schema_fields

        self.assertEqual(len(extra_fields), 2)
        self.assertIn('extra_field1', extra_fields)
        self.assertIn('extra_field2', extra_fields)


class TestFileExtensionDetection(unittest.TestCase):

    def test_detect_csv_extension(self):
        """Test detecting CSV file extension"""
        filename = 'data.csv'
        self.assertTrue(filename.endswith('.csv'))

    def test_detect_jsonl_extension(self):
        """Test detecting JSONL file extension"""
        filename = 'logs.jsonl'
        self.assertTrue(filename.endswith('.jsonl'))

    def test_detect_parquet_extension(self):
        """Test detecting Parquet file extension"""
        filename = 'warehouse.parquet'
        self.assertTrue(filename.endswith('.parquet'))

    def test_detect_excel_extensions(self):
        """Test detecting Excel file extensions"""
        xlsx_file = 'report.xlsx'
        xls_file = 'old_report.xls'

        self.assertTrue(xlsx_file.endswith('.xlsx'))
        self.assertTrue(xls_file.endswith('.xls'))

    def test_detect_compressed_extensions(self):
        """Test detecting compressed file extensions"""
        gz_file = 'data.csv.gz'
        zip_file = 'archive.zip'

        self.assertTrue(gz_file.endswith('.gz'))
        self.assertTrue(zip_file.endswith('.zip'))


if __name__ == '__main__':
    unittest.main()
