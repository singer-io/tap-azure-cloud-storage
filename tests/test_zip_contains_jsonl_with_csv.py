from tap_tester import connections, menagerie, runner
from base import AzureCloudStorageBaseTest
from utils_for_test import delete_and_push_file


class AzureCloudStorageCompressedZipFileJSONLCSV(AzureCloudStorageBaseTest):
    """
    Test that .zip files containing both JSONL and CSV files can be read correctly.
    The tap should automatically extract and read the files inside the ZIP archive.
    """

    table_entry = [
        {
            'table_name': 'zip_has_jsonl_with_csv',
            'search_prefix': 'tap_azure_tester/zip_files',
            'search_pattern': 'tap_azure_tester/zip_files/.*\\.zip',
            'key_properties': []
        }
    ]

    def setUp(self):
        delete_and_push_file(self.get_properties(), self.resource_name(), None)
        self.conn_id = connections.ensure_connection(self)

    def resource_name(self):
        return ["jsonl_csv.zip"]

    def name(self):
        return "test_zip_file"

    def expected_check_streams(self):
        return {'zip_has_jsonl_with_csv'}

    def expected_sync_streams(self):
        return {'zip_has_jsonl_with_csv'}

    def expected_pks(self):
        return {'zip_has_jsonl_with_csv': {}}

    def test_run(self):
        """
        Test that verifies:
        1. .zip files are properly extracted
        2. Both CSV and JSONL files inside are correctly parsed
        3. All records from both file types are synced
        """

        # Run discovery
        found_catalogs = self.run_and_verify_check_mode(self.conn_id)

        # Clear state before our run
        menagerie.set_state(self.conn_id, {})

        # Select our catalogs
        our_catalogs = [c for c in found_catalogs if c.get('tap_stream_id') in self.expected_sync_streams()]

        self.perform_and_verify_table_and_field_selection(self.conn_id, our_catalogs)

        # Run sync
        self.run_and_verify_sync(self.conn_id)

        expected_records = 35
        # Verify actual rows were synced
        records = runner.get_upserts_from_target_output()

        self.assertEqual(expected_records, len(records),
                        msg=f"Expected {expected_records} records from ZIP file, got {len(records)}")

        print(f"Successfully synced {len(records)} records from .zip file containing JSONL and CSV")
