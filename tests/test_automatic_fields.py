from tap_tester import connections, menagerie, runner
from base import AzureCloudStorageBaseTest
from utils_for_test import delete_and_push_file


class AzureCloudStorageAutomaticFieldsTest(AzureCloudStorageBaseTest):
    """
    Test that automatic fields (primary keys and _sdc fields) are always included
    in the sync, even when not explicitly selected.
    """

    table_entry = [
        {
            'table_name': 'automatic_fields_test',
            'search_prefix': 'tap_azure_tester',
            'search_pattern': 'CSV_with_one_primary_key\\.csv',
            'key_properties': ['id']
        }
    ]

    def setUp(self):
        delete_and_push_file(self.get_properties(), self.resource_name(), None)
        self.conn_id = connections.ensure_connection(self)

    def resource_name(self):
        return ["CSV_with_one_primary_key.csv"]

    def name(self):
        return "test_automatic_fields"

    def expected_check_streams(self):
        return {"automatic_fields_test"}

    def expected_sync_streams(self):
        return {"automatic_fields_test"}

    def expected_pks(self):
        return {"automatic_fields_test": {"id"}}

    def expected_automatic_fields(self):
        """Expected automatic fields: primary keys + _sdc fields"""
        return {
            "automatic_fields_test": {
                "id",  # primary key
                "_sdc_source_container",
                "_sdc_source_file",
                "_sdc_source_lineno",
                "_sdc_extra"
            }
        }

    def test_run(self):
        """
        Verify that:
        1. Automatic fields are marked correctly in metadata
        2. Only automatic fields are synced when no fields are explicitly selected
        3. All automatic fields are present in synced records
        """

        # Run discovery
        found_catalogs = self.run_and_verify_check_mode(self.conn_id)

        # Select our catalogs but DO NOT select any fields (only automatic fields should be synced)
        our_catalogs = [c for c in found_catalogs if c.get('tap_stream_id') in self.expected_sync_streams()]

        # Perform field selection with select_all_fields=False
        # This will only select automatic fields
        self.perform_and_verify_table_and_field_selection(
            self.conn_id,
            our_catalogs,
            select_all_fields=False
        )

        # Get the selected fields from metadata
        stream_to_selected_fields = dict()
        for c in our_catalogs:
            c_annotated = menagerie.get_annotated_schema(self.conn_id, c['stream_id'])

            # Get fields that are selected or automatic
            selected_fields = set()
            for md_entry in c_annotated['metadata']:
                if md_entry['breadcrumb'] != []:
                    field_name = md_entry['breadcrumb'][1]
                    field_metadata = md_entry.get('metadata', {})
                    if (field_metadata.get('selected') is True or 
                        field_metadata.get('inclusion') == 'automatic'):
                        selected_fields.add(field_name)

            stream_to_selected_fields[c['stream_name']] = selected_fields

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

                # Expected values - only automatic fields
                expected_automatic_fields = self.expected_automatic_fields()[stream]
                expected_all_keys = stream_to_selected_fields[stream]

                messages = synced_records.get(stream)

                # Verify we got records
                self.assertGreater(len(messages['messages']), 0,
                                 msg=f"No messages found for stream {stream}")

                # Collect actual keys from synced records
                actual_all_keys = set()
                for message in messages['messages']:
                    if message['action'] == 'upsert':
                        actual_all_keys.update(message['data'].keys())

                # Verify that only automatic fields are present
                self.assertSetEqual(expected_automatic_fields, actual_all_keys,
                                  msg=f"Expected only automatic fields. "
                                      f"Expected: {expected_automatic_fields}, "
                                      f"Actual: {actual_all_keys}")

                # Verify all expected keys are synced
                self.assertTrue(expected_all_keys.issubset(actual_all_keys),
                              msg=f"Not all expected fields were synced. "
                                  f"Expected: {expected_all_keys}, "
                                  f"Actual: {actual_all_keys}")

                # Verify primary key is always present
                for message in messages['messages']:
                    if message['action'] == 'upsert':
                        for pk in self.expected_pks()[stream]:
                            self.assertIn(pk, message['data'].keys(),
                                        msg=f"Primary key {pk} not found in record")
