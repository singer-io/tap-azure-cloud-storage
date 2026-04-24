from tap_tester import connections, menagerie, runner
from base import AzureCloudStorageBaseTest
from utils_for_test import delete_and_push_file


class AzureCloudStorageExcelTest(AzureCloudStorageBaseTest):
    """
    Test that Excel (.xlsx) files can be read and synced correctly.
    The tap should parse Excel files and extract all rows from the sheets.
    """

    table_entry = [
        {
            'table_name': 'excel_employees',
            'search_prefix': 'tap_azure_tester',
            'search_pattern': 'employees\\.xlsx$',
            'key_properties': ['employee_id']
        }
    ]

    def setUp(self):
        delete_and_push_file(self.get_properties(), self.resource_name(), None)
        self.conn_id = connections.ensure_connection(self)

    def resource_name(self):
        return ["employees.xlsx"]

    def name(self):
        return "test_excel_file"

    def expected_check_streams(self):
        return {"excel_employees"}

    def expected_sync_streams(self):
        return {"excel_employees"}

    def expected_pks(self):
        return {"excel_employees": {"employee_id"}}

    def test_run(self):
        """
        Test that verifies:
        1. Excel (.xlsx) files are properly parsed
        2. All rows are correctly extracted
        3. All records are synced with proper data types
        4. _sdc fields are added to each record
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

                # Verify we have actual data records (expecting 10 employees)
                self.assertEqual(len(upsert_messages), 10,
                               msg=f"Expected 10 employee records, got {len(upsert_messages)}")

                # Verify all required fields are present in records
                for msg in upsert_messages:
                    record = msg['data']

                    # Verify primary key is present
                    self.assertIn('employee_id', record,
                                msg="Missing employee_id in record")

                    # Verify other expected fields
                    expected_fields = ['first_name', 'last_name', 'department', 'hire_date', 'salary']
                    for field in expected_fields:
                        self.assertIn(field, record,
                                    msg=f"Missing {field} in record")

                    # Verify _sdc fields are present
                    self.assertIn('_sdc_source_file', record,
                                msg="Missing _sdc_source_file in record")
                    self.assertIn('_sdc_source_container', record,
                                msg="Missing _sdc_source_container in record")
                    self.assertIn('_sdc_source_lineno', record,
                                msg="Missing _sdc_source_lineno in record")

                    # Verify _sdc_source_file includes the sheet context
                    source_file = record['_sdc_source_file']
                    self.assertTrue(
                        source_file.endswith('/Employees') or source_file.endswith('Employees'),
                        msg="_sdc_source_file should include Employees sheet context"
                    )

                # Verify sample employee data
                # The tap unwraps singer-encodings comment wrappers via
                # unwrap_excel_commented_cells(), so values are plain scalars.
                employee_ids = [msg['data']['employee_id'] for msg in upsert_messages]
                self.assertIn(1, employee_ids, msg="Employee ID 1 not found")
                self.assertIn(10, employee_ids, msg="Employee ID 10 not found")

                # Verify departments are present
                departments = [msg['data']['department'] for msg in upsert_messages]
                self.assertIn('Engineering', departments)
                self.assertIn('Marketing', departments)
                self.assertIn('Sales', departments)
                self.assertIn('HR', departments)

                # Verify commented cells were unwrapped to plain values
                first_record = upsert_messages[0]['data']
                self.assertEqual(first_record['employee_id'], 1)
                self.assertEqual(first_record['salary'], 95000)
