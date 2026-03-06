from tap_tester import connections, menagerie, runner
import time
from base import AzureCloudStorageBaseTest
from utils_for_test import delete_and_push_file


class AzureCloudStorageStartDateTest(AzureCloudStorageBaseTest):
    """
    Test that the start_date configuration parameter works correctly.
    Verifies that only files modified on or after start_date are synced.
    """

    table_entry = [
        {
            'table_name': 'start_date_test',
            'search_prefix': 'tap_azure_tester/start_date',
            'search_pattern': 'start_date.*\\.csv$',
            'key_properties': ['id']
        }
    ]

    def setUp(self):
        # Upload first file
        delete_and_push_file(self.get_properties(), ["start_date_1.csv"], None)

        # Wait to ensure files have different timestamps
        time.sleep(2)

        # Record the time between uploads (this will be our start_date)
        self.START_DATE = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())

        # Wait again
        time.sleep(2)

        # Upload second file after start_date
        delete_and_push_file(self.get_properties(), ["start_date_2.csv"], None)

        # Create connection AFTER setting START_DATE
        self.conn_id = connections.ensure_connection(self)

    def get_properties(self, original: bool = True):
        """Override to use START_DATE when it's available."""
        props = super().get_properties(original)
        # If START_DATE has been set and we're not requesting original, use it
        if hasattr(self, 'START_DATE') and self.START_DATE and not original:
            props['start_date'] = self.START_DATE
        # For this test, always use START_DATE if it's been set
        elif hasattr(self, 'START_DATE') and self.START_DATE:
            props['start_date'] = self.START_DATE
        return props

    def resource_name(self):
        return ["start_date_1.csv", "start_date_2.csv"]

    def name(self):
        return "test_start_date"

    def expected_check_streams(self):
        return {"start_date_test"}

    def expected_sync_streams(self):
        return {"start_date_test"}

    def expected_pks(self):
        return {"start_date_test": {"id"}}

    def test_run(self):
        """
        Test that verifies:
        1. Only files modified after start_date are discovered and synced
        2. Files modified before start_date are ignored
        """

        # Run discovery with the START_DATE that falls between the two file uploads
        # This should only discover the second file (start_date_2.csv)
        found_catalogs = self.run_and_verify_check_mode(self.conn_id)

        # Select our catalogs
        our_catalogs = [c for c in found_catalogs if c.get('tap_stream_id') in self.expected_sync_streams()]

        self.perform_and_verify_table_and_field_selection(self.conn_id, our_catalogs)

        # Clear state before our run
        menagerie.set_state(self.conn_id, {})

        # Run a sync job
        self.run_and_verify_sync(self.conn_id)

        synced_records = runner.get_records_from_target_output()

        # Verify the stream was synced
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

                # Verify records are from the file uploaded after start_date
                record_count = len(upsert_messages)
                print(f"Synced {record_count} records for {stream}")

                self.assertEqual(
                    record_count,
                    4,
                    msg="Expected exactly 4 records with inclusive start_date filtering, got {}".format(record_count)
                )

                # Verify all records have the required _sdc fields
                for msg in upsert_messages:
                    self.assertIn('_sdc_source_file', msg['data'],
                                msg="Missing _sdc_source_file in record")
                    self.assertIn('_sdc_source_container', msg['data'],
                                msg="Missing _sdc_source_container in record")

                # Log which files were actually synced
                synced_files = set()
                for msg in upsert_messages:
                    if '_sdc_source_file' in msg['data']:
                        synced_files.add(msg['data']['_sdc_source_file'])

                print(f"Files synced: {synced_files}")

                self.assertSetEqual(
                    synced_files,
                    {
                        'tap_azure_tester/start_date/start_date_1.csv',
                        'tap_azure_tester/start_date/start_date_2.csv'
                    },
                    msg="Expected both start_date files to be synced with inclusive filtering, found {}".format(synced_files)
                )
