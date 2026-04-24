from tap_tester import connections, menagerie, runner
from base import AzureCloudStorageBaseTest
from utils_for_test import delete_and_push_file


class AzureCloudStorageGzContainsJsonlTest(AzureCloudStorageBaseTest):
    """
    Test that .gz compressed files containing JSONL data can be read correctly.
    The tap should automatically decompress and read the JSONL inside.
    """

    table_entry = [
        {
            'table_name': 'gz_jsonl_test',
            'search_prefix': 'tap_azure_tester',
            'search_pattern': 'sample_compressed_gz_file_with_json_file_2_records\\.gz$',
            'key_properties': ['id']
        }
    ]

    def setUp(self):
        delete_and_push_file(self.get_properties(), self.resource_name(), None)
        self.conn_id = connections.ensure_connection(self)

    def resource_name(self):
        return ["sample_compressed_gz_file_with_json_file_2_records.gz"]

    def name(self):
        return "test_gz_file_contains_jsonl_file"

    def expected_check_streams(self):
        return {"gz_jsonl_test"}

    def expected_sync_streams(self):
        return {"gz_jsonl_test"}

    def expected_pks(self):
        return {"gz_jsonl_test": {"id"}}

    def test_run(self):
        """
        Test that verifies:
        1. .gz files containing JSONL are properly decompressed
        2. JSONL data inside is correctly parsed
        3. All records are synced
        """

        # Run discovery
        found_catalogs = self.run_and_verify_check_mode(self.conn_id)

        # Select our catalogs
        our_catalogs = [c for c in found_catalogs if c.get('tap_stream_id') in self.expected_sync_streams()]

        self.perform_and_verify_table_and_field_selection(self.conn_id, our_catalogs)

        # Clear state before our run
        menagerie.set_state(self.conn_id, {})

        # Run a sync job
        self.run_and_verify_sync(self.conn_id)

        synced_records = runner.get_records_from_target_output()

        # Verify no unexpected streams were replicated
        synced_stream_names = set(synced_records.keys())
        self.assertSetEqual(self.expected_sync_streams(), synced_stream_names)

        for stream in self.expected_sync_streams():
            with self.subTest(stream=stream):
                messages = synced_records.get(stream)

                # Verify we got records
                self.assertGreater(len(messages['messages']), 0,
                                 msg=f"No messages found for stream {stream}")

                # Extract record data
                upsert_messages = [msg for msg in messages['messages'] if msg['action'] == 'upsert']

                # The file should have 2 records based on filename
                self.assertEqual(len(upsert_messages), 2,
                               msg=f"Expected 2 records from .gz JSONL file, got {len(upsert_messages)}")

                # Verify _sdc fields are present
                for msg in upsert_messages:
                    self.assertIn('_sdc_source_file', msg['data'],
                                msg="Missing _sdc_source_file in record")
                    self.assertIn('_sdc_source_container', msg['data'],
                                msg="Missing _sdc_source_container in record")
                    self.assertIn('_sdc_source_lineno', msg['data'],
                                msg="Missing _sdc_source_lineno in record")

                print(f"Successfully synced {len(upsert_messages)} records from .gz JSONL file")
