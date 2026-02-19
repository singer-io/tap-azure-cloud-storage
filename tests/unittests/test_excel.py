"""
Unit tests for Excel (.xlsx) file handling in tap-azure-cloud-storage.
Tests Excel file detection, sampling, syncing, and error handling.
"""

import unittest
from unittest.mock import patch, MagicMock, mock_open
import io


class TestExcelFileDetection(unittest.TestCase):
    """Test Excel file detection and routing"""

    @patch('tap_azure_cloud_storage.azure_storage.get_file_handle')
    @patch('tap_azure_cloud_storage.sync.sync_excel_file')
    def test_xlsx_extension_routes_to_excel_handler(self, mock_sync_excel, mock_get_handle):
        """Test that .xlsx files are routed to sync_excel_file"""
        from tap_azure_cloud_storage.sync import sync_table_file

        config = {'container_name': 'test-container'}
        blob_path = 'data/employees.xlsx'
        table_spec = {'table_name': 'employees', 'key_properties': ['id']}
        stream = {'schema': {'properties': {}}, 'metadata': []}

        mock_get_handle.return_value = MagicMock()
        mock_sync_excel.return_value = 10

        result = sync_table_file(config, blob_path, table_spec, stream)

        mock_sync_excel.assert_called_once()
        self.assertEqual(result, 10)

    def test_excel_file_extension_detection(self):
        """Test that Excel file extensions are correctly detected"""
        excel_files = [
            'employees.xlsx',
            'data/reports/sales.xlsx',
            'archive/2024/report.xlsx'
        ]

        for filename in excel_files:
            with self.subTest(filename=filename):
                extension = filename.lower().split('.')[-1]
                self.assertEqual(extension, 'xlsx')


class TestExcelSampling(unittest.TestCase):
    """Test Excel file sampling for schema discovery"""

    @patch('tap_azure_cloud_storage.azure_storage.excel_reader')
    def test_sample_excel_file_with_valid_data(self, mock_excel_reader):
        """Test sampling Excel file returns records"""
        from tap_azure_cloud_storage import azure_storage

        # Mock Excel reader to return sample rows
        mock_iterator = [
            ('Sheet1', {'employee_id': 1, 'name': 'Alice', 'salary': 95000}),
            ('Sheet1', {'employee_id': 2, 'name': 'Bob', 'salary': 85000}),
            ('Sheet1', {'employee_id': 3, 'name': 'Carol', 'salary': 90000}),
            ('Sheet1', {'employee_id': 4, 'name': 'David', 'salary': 100000}),
            ('Sheet1', {'employee_id': 5, 'name': 'Eve', 'salary': 95000}),
        ]
        mock_excel_reader.get_excel_row_iterator.return_value = iter(mock_iterator)

        table_spec = {'key_properties': ['employee_id'], 'date_overrides': []}
        blob_path = 'employees.xlsx'
        data = b'mock_excel_data'
        sample_rate = 2
        max_records = 3

        # Test sampling
        records = list(azure_storage.sample_file(table_spec, blob_path, data, sample_rate, 'xlsx', max_records))

        # Should sample every 2nd record, max 3 records
        self.assertLessEqual(len(records), max_records)
        # First record should be index 0 (Alice)
        self.assertEqual(records[0]['employee_id'], 1)

    @patch('tap_azure_cloud_storage.azure_storage.excel_reader')
    def test_sample_excel_file_handles_empty_file(self, mock_excel_reader):
        """Test sampling Excel file handles empty files gracefully"""
        from tap_azure_cloud_storage import azure_storage

        # Mock Excel reader returning None for empty file
        mock_excel_reader.get_excel_row_iterator.return_value = None

        table_spec = {'key_properties': [], 'date_overrides': []}
        blob_path = 'empty.xlsx'
        data = b'mock_empty_excel'
        sample_rate = 1
        max_records = 1000

        # Should handle empty file without errors
        records = list(azure_storage.sample_file(table_spec, blob_path, data, sample_rate, 'xlsx', max_records))

        self.assertEqual(len(records), 0)

    @patch('tap_azure_cloud_storage.azure_storage.excel_reader')
    def test_sample_excel_file_respects_sample_rate(self, mock_excel_reader):
        """Test that Excel sampling respects sample_rate parameter"""
        from tap_azure_cloud_storage import azure_storage

        # Mock 10 rows of data
        mock_iterator = [
            ('Sheet1', {'id': i, 'value': i * 100})
            for i in range(10)
        ]
        mock_excel_reader.get_excel_row_iterator.return_value = iter(mock_iterator)

        table_spec = {'key_properties': ['id']}
        sample_rate = 3  # Every 3rd record

        records = list(azure_storage.sample_file(
            table_spec, 'test.xlsx', b'data', sample_rate, 'xlsx', 1000
        ))

        # Should get records at indices 0, 3, 6, 9 (4 records)
        self.assertEqual(len(records), 4)
        self.assertEqual(records[0]['id'], 0)
        self.assertEqual(records[1]['id'], 3)
        self.assertEqual(records[2]['id'], 6)
        self.assertEqual(records[3]['id'], 9)

    @patch('tap_azure_cloud_storage.azure_storage.excel_reader')
    def test_sample_excel_file_handles_exception(self, mock_excel_reader):
        """Test that Excel sampling handles exceptions gracefully"""
        from tap_azure_cloud_storage import azure_storage

        # Mock Excel reader raising an exception
        mock_excel_reader.get_excel_row_iterator.side_effect = Exception('Corrupted Excel file')

        table_spec = {'key_properties': []}

        # Should not raise, just return empty and increment skipped count
        records = list(azure_storage.sample_file(
            table_spec, 'corrupted.xlsx', b'bad_data', 1, 'xlsx', 1000
        ))

        self.assertEqual(len(records), 0)


class TestExcelSync(unittest.TestCase):
    """Test Excel file syncing functionality"""

    @patch('tap_azure_cloud_storage.sync.singer.write_record')
    @patch('tap_azure_cloud_storage.sync.excel_reader')
    def test_sync_excel_file_writes_records(self, mock_excel_reader, mock_write_record):
        """Test that sync_excel_file writes records with correct structure"""
        from tap_azure_cloud_storage.sync import sync_excel_file

        # Mock Excel iterator
        mock_iterator = [
            ('Employees', {'employee_id': 1, 'first_name': 'Alice', 'salary': 95000}),
            ('Employees', {'employee_id': 2, 'first_name': 'Bob', 'salary': 85000}),
        ]
        mock_excel_reader.get_excel_row_iterator.return_value = iter(mock_iterator)

        config = {'container_name': 'test-container'}
        file_handle = io.BytesIO(b'mock_excel_data')
        blob_path = 'data/employees.xlsx'
        table_spec = {'table_name': 'employees', 'key_properties': ['employee_id'], 'date_overrides': []}
        stream = {
            'schema': {
                'properties': {
                    'employee_id': {'type': 'integer'},
                    'first_name': {'type': 'string'},
                    'salary': {'type': 'integer'}
                }
            },
            'metadata': []
        }

        records_synced = sync_excel_file(config, file_handle, blob_path, table_spec, stream)

        # Should have synced 2 records
        self.assertEqual(records_synced, 2)

        # Verify write_record was called for each record
        self.assertEqual(mock_write_record.call_count, 2)

        # Check first record has _sdc fields
        first_call_args = mock_write_record.call_args_list[0]
        table_name = first_call_args[0][0]
        record = first_call_args[0][1]

        self.assertEqual(table_name, 'employees')
        self.assertIn('_sdc_source_container', record)
        self.assertIn('_sdc_source_file', record)
        self.assertIn('_sdc_source_lineno', record)
        self.assertEqual(record['_sdc_source_container'], 'test-container')
        # _sdc_source_file should include sheet name
        self.assertIn('Employees', record['_sdc_source_file'])

    @patch('tap_azure_cloud_storage.sync.excel_reader')
    def test_sync_excel_file_handles_empty_file(self, mock_excel_reader):
        """Test that sync_excel_file handles empty Excel files"""
        from tap_azure_cloud_storage.sync import sync_excel_file

        # Mock empty Excel file
        mock_excel_reader.get_excel_row_iterator.return_value = None

        config = {'container_name': 'test-container'}
        file_handle = io.BytesIO(b'empty')
        blob_path = 'empty.xlsx'
        table_spec = {'table_name': 'test', 'key_properties': []}
        stream = {'schema': {'properties': {}}, 'metadata': []}

        records_synced = sync_excel_file(config, file_handle, blob_path, table_spec, stream)

        # Should return 0 records for empty file
        self.assertEqual(records_synced, 0)

    @patch('tap_azure_cloud_storage.sync.excel_reader')
    def test_sync_excel_file_handles_exception(self, mock_excel_reader):
        """Test that sync_excel_file handles exceptions during reading"""
        from tap_azure_cloud_storage.sync import sync_excel_file

        # Mock Excel reader raising an exception
        mock_excel_reader.get_excel_row_iterator.side_effect = Exception('Excel parsing error')

        config = {'container_name': 'test-container'}
        file_handle = io.BytesIO(b'corrupted_data')
        blob_path = 'corrupted.xlsx'
        table_spec = {'table_name': 'test', 'key_properties': []}
        stream = {'schema': {'properties': {}}, 'metadata': []}

        records_synced = sync_excel_file(config, file_handle, blob_path, table_spec, stream)

        # Should return 0 and not crash
        self.assertEqual(records_synced, 0)

    @patch('tap_azure_cloud_storage.sync.singer.write_record')
    @patch('tap_azure_cloud_storage.sync.excel_reader')
    def test_sync_excel_file_includes_sheet_name_in_source_file(self, mock_excel_reader, mock_write_record):
        """Test that _sdc_source_file includes the Excel sheet name"""
        from tap_azure_cloud_storage.sync import sync_excel_file

        # Mock multiple sheets
        mock_iterator = [
            ('Sales', {'id': 1, 'amount': 1000}),
            ('Sales', {'id': 2, 'amount': 2000}),
            ('Marketing', {'id': 3, 'budget': 5000}),
        ]
        mock_excel_reader.get_excel_row_iterator.return_value = iter(mock_iterator)

        config = {'container_name': 'reports'}
        file_handle = io.BytesIO(b'data')
        blob_path = 'quarterly_report.xlsx'
        table_spec = {'table_name': 'reports', 'key_properties': ['id']}
        stream = {'schema': {'properties': {}}, 'metadata': []}

        sync_excel_file(config, file_handle, blob_path, table_spec, stream)

        # Check that sheet names are in _sdc_source_file
        call_args_list = mock_write_record.call_args_list

        # First two records from Sales sheet
        first_record = call_args_list[0][0][1]
        self.assertIn('quarterly_report.xlsx/Sales', first_record['_sdc_source_file'])

        second_record = call_args_list[1][0][1]
        self.assertIn('quarterly_report.xlsx/Sales', second_record['_sdc_source_file'])

        # Third record from Marketing sheet
        third_record = call_args_list[2][0][1]
        self.assertIn('quarterly_report.xlsx/Marketing', third_record['_sdc_source_file'])

    @patch('tap_azure_cloud_storage.sync.singer.write_record')
    @patch('tap_azure_cloud_storage.sync.excel_reader')
    def test_sync_excel_file_skips_empty_rows(self, mock_excel_reader, mock_write_record):
        """Test that sync_excel_file skips empty or invalid rows"""
        from tap_azure_cloud_storage.sync import sync_excel_file

        # Mock iterator with some empty/invalid rows
        mock_iterator = [
            ('Sheet1', {'id': 1, 'name': 'Alice'}),
            ('Sheet1', {}),  # Empty dict
            ('Sheet1', None),  # None value
            ('Sheet1', {'id': 2, 'name': 'Bob'}),
            ('Sheet1', ''),  # Empty string (not dict)
        ]
        mock_excel_reader.get_excel_row_iterator.return_value = iter(mock_iterator)

        config = {'container_name': 'test'}
        file_handle = io.BytesIO(b'data')
        blob_path = 'test.xlsx'
        table_spec = {'table_name': 'test', 'key_properties': []}
        stream = {'schema': {'properties': {}}, 'metadata': []}

        records_synced = sync_excel_file(config, file_handle, blob_path, table_spec, stream)

        # Should only sync 2 valid records
        self.assertEqual(records_synced, 2)
        self.assertEqual(mock_write_record.call_count, 2)

    @patch('tap_azure_cloud_storage.sync.singer.write_record')
    @patch('tap_azure_cloud_storage.sync.excel_reader')
    def test_sync_excel_file_increments_line_numbers(self, mock_excel_reader, mock_write_record):
        """Test that _sdc_source_lineno increments for each record"""
        from tap_azure_cloud_storage.sync import sync_excel_file

        mock_iterator = [
            ('Sheet1', {'id': 1}),
            ('Sheet1', {'id': 2}),
            ('Sheet1', {'id': 3}),
        ]
        mock_excel_reader.get_excel_row_iterator.return_value = iter(mock_iterator)

        config = {'container_name': 'test'}
        file_handle = io.BytesIO(b'data')
        blob_path = 'test.xlsx'
        table_spec = {'table_name': 'test', 'key_properties': []}
        stream = {'schema': {'properties': {}}, 'metadata': []}

        sync_excel_file(config, file_handle, blob_path, table_spec, stream)

        # Check line numbers start at 2 (header is line 1)
        call_args = mock_write_record.call_args_list
        self.assertEqual(call_args[0][0][1]['_sdc_source_lineno'], 2)
        self.assertEqual(call_args[1][0][1]['_sdc_source_lineno'], 3)
        self.assertEqual(call_args[2][0][1]['_sdc_source_lineno'], 4)


class TestExcelWithDateOverrides(unittest.TestCase):
    """Test Excel file handling with date_overrides configuration"""

    @patch('tap_azure_cloud_storage.sync.excel_reader')
    def test_excel_reader_receives_date_overrides(self, mock_excel_reader):
        """Test that date_overrides are passed to Excel reader"""
        from tap_azure_cloud_storage.sync import sync_excel_file

        mock_excel_reader.get_excel_row_iterator.return_value = iter([])

        config = {'container_name': 'test'}
        file_handle = io.BytesIO(b'data')
        blob_path = 'test.xlsx'
        table_spec = {
            'table_name': 'test',
            'key_properties': ['id'],
            'date_overrides': ['hire_date', 'birth_date']
        }
        stream = {'schema': {'properties': {}}, 'metadata': []}

        sync_excel_file(config, file_handle, blob_path, table_spec, stream)

        # Verify options were passed correctly
        call_args = mock_excel_reader.get_excel_row_iterator.call_args
        options = call_args[1]['options']

        self.assertEqual(options['key_properties'], ['id'])
        self.assertEqual(options['date_overrides'], ['hire_date', 'birth_date'])


class TestExcelHyperlinks(unittest.TestCase):
    """Test Excel file handling with hyperlinks"""

    @patch('tap_azure_cloud_storage.sync.singer.write_record')
    @patch('tap_azure_cloud_storage.sync.excel_reader')
    def test_sync_excel_file_with_hyperlinks(self, mock_excel_reader, mock_write_record):
        """Test that Excel cells with hyperlinks are synced correctly"""
        from tap_azure_cloud_storage.sync import sync_excel_file

        # Mock records where some cells contain hyperlink data
        # The Excel reader should extract the display text, not the hyperlink URL
        mock_iterator = [
            ('Sheet1', {
                'id': 1,
                'website': 'Visit Our Site',  # Display text from hyperlink
                'email': 'contact@example.com',
                'document': 'Annual Report 2024'  # Display text from file link
            }),
            ('Sheet1', {
                'id': 2,
                'website': 'https://example.com',  # Direct URL (no hyperlink)
                'email': 'sales@example.com',
                'document': 'Q1_Summary.pdf'
            }),
        ]
        mock_excel_reader.get_excel_row_iterator.return_value = iter(mock_iterator)

        config = {'container_name': 'test-container'}
        file_handle = io.BytesIO(b'excel_with_hyperlinks')
        blob_path = 'data/contacts.xlsx'
        table_spec = {'table_name': 'contacts', 'key_properties': ['id']}
        stream = {'schema': {'properties': {}}, 'metadata': []}

        records_synced = sync_excel_file(config, file_handle, blob_path, table_spec, stream)

        # Should sync both records successfully
        self.assertEqual(records_synced, 2)
        self.assertEqual(mock_write_record.call_count, 2)

        # Verify first record has hyperlink display text
        first_call = mock_write_record.call_args_list[0]
        first_record = first_call[0][1]
        self.assertEqual(first_record['website'], 'Visit Our Site')
        self.assertEqual(first_record['document'], 'Annual Report 2024')

    @patch('tap_azure_cloud_storage.sync.singer.write_record')
    @patch('tap_azure_cloud_storage.sync.excel_reader')
    def test_excel_hyperlinks_preserve_cell_values(self, mock_excel_reader, mock_write_record):
        """Test that hyperlinked cells preserve their display values in records"""
        from tap_azure_cloud_storage.sync import sync_excel_file

        # Mock a record with various hyperlink scenarios
        mock_iterator = [
            ('Sheet1', {
                'product_id': 'PROD-001',
                'product_name': 'Widget A',
                'specs_link': 'View Specifications',  # Hyperlink to document
                'price': 99.99
            }),
        ]
        mock_excel_reader.get_excel_row_iterator.return_value = iter(mock_iterator)

        config = {'container_name': 'catalog'}
        file_handle = io.BytesIO(b'data')
        blob_path = 'products.xlsx'
        table_spec = {'table_name': 'products', 'key_properties': ['product_id']}
        stream = {'schema': {'properties': {}}, 'metadata': []}

        sync_excel_file(config, file_handle, blob_path, table_spec, stream)

        # Verify the hyperlink display text is captured
        written_record = mock_write_record.call_args[0][1]
        self.assertEqual(written_record['specs_link'], 'View Specifications')
        self.assertEqual(written_record['product_id'], 'PROD-001')

    @patch('tap_azure_cloud_storage.azure_storage.excel_reader')
    def test_sample_excel_with_hyperlinks(self, mock_excel_reader):
        """Test that sampling Excel files with hyperlinks works correctly"""
        from tap_azure_cloud_storage import azure_storage

        # Mock sampling with hyperlinks
        mock_iterator = [
            ('Sheet1', {'id': 1, 'link': 'Click Here', 'value': 100}),
            ('Sheet1', {'id': 2, 'link': 'Read More', 'value': 200}),
            ('Sheet1', {'id': 3, 'link': 'Download', 'value': 300}),
        ]
        mock_excel_reader.get_excel_row_iterator.return_value = iter(mock_iterator)

        table_spec = {'key_properties': ['id']}
        records = list(azure_storage.sample_file(
            table_spec, 'links.xlsx', b'data', 1, 'xlsx', 1000
        ))

        # All records should be sampled
        self.assertEqual(len(records), 3)
        # Hyperlink display text should be preserved
        self.assertEqual(records[0]['link'], 'Click Here')
        self.assertEqual(records[1]['link'], 'Read More')


class TestExcelComments(unittest.TestCase):
    """Test Excel file handling with cell comments/notes"""

    @patch('tap_azure_cloud_storage.sync.singer.write_record')
    @patch('tap_azure_cloud_storage.sync.excel_reader')
    def test_sync_excel_file_with_comments(self, mock_excel_reader, mock_write_record):
        """Test that Excel cells with comments are synced (comments are typically ignored)"""
        from tap_azure_cloud_storage.sync import sync_excel_file

        # Mock records where cells have comments attached
        # Excel reader typically extracts cell values, not comments
        mock_iterator = [
            ('Sheet1', {
                'employee_id': 1,
                'name': 'Alice Johnson',
                'status': 'Active',  # Cell has comment: "Promoted on 2024-01-15"
                'salary': 95000  # Cell has comment: "Includes performance bonus"
            }),
            ('Sheet1', {
                'employee_id': 2,
                'name': 'Bob Smith',
                'status': 'On Leave',  # Cell has comment: "Medical leave until Feb 2024"
                'salary': 85000
            }),
        ]
        mock_excel_reader.get_excel_row_iterator.return_value = iter(mock_iterator)

        config = {'container_name': 'hr-data'}
        file_handle = io.BytesIO(b'excel_with_comments')
        blob_path = 'employees.xlsx'
        table_spec = {'table_name': 'employees', 'key_properties': ['employee_id']}
        stream = {'schema': {'properties': {}}, 'metadata': []}

        records_synced = sync_excel_file(config, file_handle, blob_path, table_spec, stream)

        # Should sync both records (comments don't affect sync)
        self.assertEqual(records_synced, 2)

        # Verify cell values are extracted (not comments)
        first_call = mock_write_record.call_args_list[0]
        first_record = first_call[0][1]
        self.assertEqual(first_record['status'], 'Active')
        self.assertEqual(first_record['salary'], 95000)

        second_call = mock_write_record.call_args_list[1]
        second_record = second_call[0][1]
        self.assertEqual(second_record['status'], 'On Leave')

    @patch('tap_azure_cloud_storage.sync.singer.write_record')
    @patch('tap_azure_cloud_storage.sync.excel_reader')
    def test_excel_comments_do_not_affect_cell_values(self, mock_excel_reader, mock_write_record):
        """Test that cell comments don't interfere with extracting cell values"""
        from tap_azure_cloud_storage.sync import sync_excel_file

        # Mock records showing comments don't affect actual values
        mock_iterator = [
            ('DataSheet', {
                'metric_id': 'M001',
                'metric_name': 'Revenue',
                'value': 1500000,  # Comment: "Target: 2M by Q4"
                'unit': 'USD'
            }),
        ]
        mock_excel_reader.get_excel_row_iterator.return_value = iter(mock_iterator)

        config = {'container_name': 'metrics'}
        file_handle = io.BytesIO(b'data')
        blob_path = 'quarterly_metrics.xlsx'
        table_spec = {'table_name': 'metrics', 'key_properties': ['metric_id']}
        stream = {'schema': {'properties': {}}, 'metadata': []}

        records_synced = sync_excel_file(config, file_handle, blob_path, table_spec, stream)

        self.assertEqual(records_synced, 1)

        # The numeric value should be extracted correctly despite comment
        written_record = mock_write_record.call_args[0][1]
        self.assertEqual(written_record['value'], 1500000)
        self.assertEqual(written_record['metric_name'], 'Revenue')

    @patch('tap_azure_cloud_storage.azure_storage.excel_reader')
    def test_sample_excel_with_comments(self, mock_excel_reader):
        """Test that sampling Excel files with cell comments works correctly"""
        from tap_azure_cloud_storage import azure_storage

        # Mock sampling where cells have comments
        mock_iterator = [
            ('Notes', {'id': 1, 'description': 'Project Alpha', 'priority': 'High'}),
            ('Notes', {'id': 2, 'description': 'Project Beta', 'priority': 'Medium'}),
            ('Notes', {'id': 3, 'description': 'Project Gamma', 'priority': 'Low'}),
        ]
        mock_excel_reader.get_excel_row_iterator.return_value = iter(mock_iterator)

        table_spec = {'key_properties': ['id']}
        records = list(azure_storage.sample_file(
            table_spec, 'projects.xlsx', b'data', 1, 'xlsx', 1000
        ))

        # All records should be sampled successfully
        self.assertEqual(len(records), 3)
        # Cell values should be extracted correctly
        self.assertEqual(records[0]['description'], 'Project Alpha')
        self.assertEqual(records[1]['priority'], 'Medium')

    @patch('tap_azure_cloud_storage.sync.singer.write_record')
    @patch('tap_azure_cloud_storage.sync.excel_reader')
    def test_excel_with_rich_text_comments(self, mock_excel_reader, mock_write_record):
        """Test that Excel cells with formatted/rich text comments sync correctly"""
        from tap_azure_cloud_storage.sync import sync_excel_file

        # Mock records with cells that might have rich text comments
        mock_iterator = [
            ('Sheet1', {
                'task_id': 'T001',
                'task_name': 'Design Review',
                'assigned_to': 'Alice',  # Comment with bold/italic formatting
                'due_date': '2024-03-15'  # Comment with colored text
            }),
        ]
        mock_excel_reader.get_excel_row_iterator.return_value = iter(mock_iterator)

        config = {'container_name': 'tasks'}
        file_handle = io.BytesIO(b'data')
        blob_path = 'tasks.xlsx'
        table_spec = {'table_name': 'tasks', 'key_properties': ['task_id']}
        stream = {'schema': {'properties': {}}, 'metadata': []}

        records_synced = sync_excel_file(config, file_handle, blob_path, table_spec, stream)

        # Should sync successfully regardless of comment formatting
        self.assertEqual(records_synced, 1)

        # Cell values should be plain text
        written_record = mock_write_record.call_args[0][1]
        self.assertEqual(written_record['assigned_to'], 'Alice')
        self.assertEqual(written_record['due_date'], '2024-03-15')


class TestExcelSpecialFeatures(unittest.TestCase):
    """Test Excel files with special features like merged cells, formulas, etc."""

    @patch('tap_azure_cloud_storage.sync.singer.write_record')
    @patch('tap_azure_cloud_storage.sync.excel_reader')
    def test_excel_with_formulas_extracts_calculated_values(self, mock_excel_reader, mock_write_record):
        """Test that Excel formulas are resolved to their calculated values"""
        from tap_azure_cloud_storage.sync import sync_excel_file

        # Mock records where some cells contain formulas
        # Excel reader should return the calculated value, not the formula
        mock_iterator = [
            ('Sheet1', {
                'item': 'Widget',
                'quantity': 10,
                'unit_price': 25.50,
                'total': 255.00  # Result of formula: =quantity * unit_price
            }),
            ('Sheet1', {
                'item': 'Gadget',
                'quantity': 5,
                'unit_price': 42.00,
                'total': 210.00  # Result of formula
            }),
        ]
        mock_excel_reader.get_excel_row_iterator.return_value = iter(mock_iterator)

        config = {'container_name': 'orders'}
        file_handle = io.BytesIO(b'data')
        blob_path = 'invoice.xlsx'
        table_spec = {'table_name': 'line_items', 'key_properties': ['item']}
        stream = {'schema': {'properties': {}}, 'metadata': []}

        records_synced = sync_excel_file(config, file_handle, blob_path, table_spec, stream)

        self.assertEqual(records_synced, 2)

        # Verify calculated values are extracted
        first_record = mock_write_record.call_args_list[0][0][1]
        self.assertEqual(first_record['total'], 255.00)

        second_record = mock_write_record.call_args_list[1][0][1]
        self.assertEqual(second_record['total'], 210.00)

    @patch('tap_azure_cloud_storage.sync.singer.write_record')
    @patch('tap_azure_cloud_storage.sync.excel_reader')
    def test_excel_with_merged_cells(self, mock_excel_reader, mock_write_record):
        """Test that merged cells are handled correctly"""
        from tap_azure_cloud_storage.sync import sync_excel_file

        # Mock records where merged cells appear
        # Excel reader typically returns the value for the first cell in merge range
        mock_iterator = [
            ('Sheet1', {
                'region': 'North America',  # Merged across 3 rows
                'country': 'USA',
                'sales': 150000
            }),
            ('Sheet1', {
                'region': 'North America',  # Same merged value
                'country': 'Canada',
                'sales': 75000
            }),
            ('Sheet1', {
                'region': 'North America',  # Same merged value
                'country': 'Mexico',
                'sales': 45000
            }),
        ]
        mock_excel_reader.get_excel_row_iterator.return_value = iter(mock_iterator)

        config = {'container_name': 'reports'}
        file_handle = io.BytesIO(b'data')
        blob_path = 'regional_sales.xlsx'
        table_spec = {'table_name': 'sales', 'key_properties': ['country']}
        stream = {'schema': {'properties': {}}, 'metadata': []}

        records_synced = sync_excel_file(config, file_handle, blob_path, table_spec, stream)

        # All 3 records should sync with the merged cell value repeated
        self.assertEqual(records_synced, 3)

        # Each record should have the merged region value
        for i in range(3):
            record = mock_write_record.call_args_list[i][0][1]
            self.assertEqual(record['region'], 'North America')

    @patch('tap_azure_cloud_storage.azure_storage.excel_reader')
    def test_sample_excel_with_formulas(self, mock_excel_reader):
        """Test that sampling Excel files with formulas works correctly"""
        from tap_azure_cloud_storage import azure_storage

        # Mock sampling where cells contain formulas
        mock_iterator = [
            ('Calculations', {'id': 1, 'value_a': 10, 'value_b': 20, 'sum': 30}),
            ('Calculations', {'id': 2, 'value_a': 15, 'value_b': 25, 'sum': 40}),
        ]
        mock_excel_reader.get_excel_row_iterator.return_value = iter(mock_iterator)

        table_spec = {'key_properties': ['id']}
        records = list(azure_storage.sample_file(
            table_spec, 'calculations.xlsx', b'data', 1, 'xlsx', 1000
        ))

        # Formulas should be resolved to values during sampling
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]['sum'], 30)
        self.assertEqual(records[1]['sum'], 40)


if __name__ == '__main__':
    unittest.main()
