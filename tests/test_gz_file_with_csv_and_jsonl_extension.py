from tap_tester import connections, menagerie, runner
from base import AzureCloudStorageBaseTest
from utils_for_test import delete_and_push_file


class AzureCloudStoragePlainCSVAndJSONLTest(AzureCloudStorageBaseTest):
    """
    Test that plain CSV and JSONL files can be read correctly.
    The tap should handle both file types.
    """

    table_entry = [
        {
            'table_name': 'gz_csv_jsonl_ext',
            'search_prefix': 'tap_azure_tester/gz_ext',
            'search_pattern': 'tap_azure_tester/gz_ext/.*\\.(csv|jsonl)$',
            'key_properties': []
        }
    ]

    def setUp(self):
        delete_and_push_file(self.get_properties(), self.resource_name(), None)
        self.conn_id = connections.ensure_connection(self)

    def resource_name(self):
        return ["gz_stored_as_csv.csv", "gz_stored_as_jsonl.jsonl"]

    def name(self):
        return "test_gz_file_with_csv_and_jsonl_extension"

    def expected_check_streams(self):
        return {'gz_csv_jsonl_ext'}

    def expected_sync_streams(self):
        return {'gz_csv_jsonl_ext'}

    def expected_pks(self):
        return {'gz_csv_jsonl_ext': {}}

    def test_run(self):
        """
        Test that verifies:
        1. CSV files are properly handled
        2. JSONL files are properly handled
        3. Both file types are detected and parsed correctly
        4. All records from both files are synced
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

        # Note: Expected record count depends on the actual content of the test files
        expected_records = 5  # Based on our actual test files (3 from CSV + 2 from JSONL)

        # Verify actual rows were synced
        records = runner.get_upserts_from_target_output()

        self.assertEqual(expected_records, len(records),
                        msg=f"Expected {expected_records} records from both files, got {len(records)}")

        print(f"Successfully synced {len(records)} records from CSV and JSONL files")
