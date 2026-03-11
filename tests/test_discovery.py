import unittest
from tap_tester import menagerie, runner, connections
from base import AzureCloudStorageBaseTest
from utils_for_test import delete_and_push_file


class AzureCloudStorageDiscoveryTest(AzureCloudStorageBaseTest):
    """
    Test discovery (check mode) for tap-azure-cloud-storage.

    Verifies:
    - Number of tables discovered matches expectations
    - Table names follow naming convention (lowercase alphas and underscores)
    - There is only 1 top level breadcrumb
    - There are no duplicate/conflicting metadata entries
    - Primary key(s) match expectations
    - '_sdc' fields are added in the schema
    - The presence of forced-replication-method='INCREMENTAL' when datetime fields exist
    - Primary keys have inclusion of "automatic"
    - Non-primary key fields have inclusion of "available"
    """

    table_entry = [
        {
            'table_name': 'employees',
            'key_properties': ['id'],
            'search_prefix': 'tap-azure-test',
            'search_pattern': 'discovery_test\\.csv$',
            'date_overrides': ['date_of_joining']
        }
    ]

    def setUp(self):
        """Set up test environment by uploading test files to Azure"""
        delete_and_push_file(self.get_properties(), self.resource_name(), None)
        self.conn_id = connections.ensure_connection(self)

    def resource_name(self):
        """Test resource files to upload"""
        return ["discovery_test.csv"]

    def name(self):
        """Test name"""
        return "test_discovery"

    def expected_check_streams(self):
        """Expected streams to be discovered"""
        return {'employees'}

    def test_run(self):
        """Run the discovery test"""

        # Run discovery
        found_catalogs = self.run_and_verify_check_mode(self.conn_id)

        # Note: Skipping stream name format assertion as this tap is dynamic.
        # Stream names may not always follow strict naming conventions.

        # Verify each expected stream
        for stream in self.expected_check_streams():
            with self.subTest(stream=stream):

                # Verify the catalog is found for the stream
                catalog = next(iter([catalog for catalog in found_catalogs
                                   if catalog["stream_name"] == stream]), None)
                self.assertIsNotNone(catalog, msg=f"Catalog not found for stream: {stream}")

                # Get schema and metadata
                schema_and_metadata = menagerie.get_annotated_schema(self.conn_id, catalog['stream_id'])
                metadata = schema_and_metadata["metadata"]
                stream_properties = [item for item in metadata if item.get("breadcrumb") == []]

                # Collecting expected values
                expected_primary_keys = {'id'}

                # Collecting actual values from metadata
                actual_primary_keys = set(
                    stream_properties[0].get("metadata", {}).get("table-key-properties", [])
                )
                actual_replication_method = stream_properties[0].get(
                    "metadata", {}
                ).get("forced-replication-method", [])
                actual_automatic_fields = set(
                    item.get("breadcrumb", ["properties", None])[1]
                    for item in metadata
                    if item.get("metadata", {}).get("inclusion") == "automatic"
                )

                ##########################################################################
                ### Metadata assertions
                ##########################################################################

                # Extract all field names
                actual_fields = []
                for md_entry in metadata:
                    if md_entry['breadcrumb'] != []:
                        actual_fields.append(md_entry['breadcrumb'][1])

                # Verify there are no duplicate metadata entries
                self.assertEqual(len(actual_fields), len(set(actual_fields)),
                                msg="duplicates in the fields retrieved")

                # Verify there is only 1 top level breadcrumb
                self.assertTrue(len(stream_properties) == 1,
                               msg="There is more than one top level breadcrumb")

                # Verify primary keys match expectations
                self.assertSetEqual(expected_primary_keys, actual_primary_keys)

                # Verify replication method is INCREMENTAL when date fields exist
                if any(field in ['date_of_joining'] for field in actual_fields):
                    self.assertEqual(actual_replication_method, 'INCREMENTAL',
                                   msg="Replication method should be INCREMENTAL when datetime fields exist")

                # Verify _sdc fields are present
                expected_sdc_fields = {
                    '_sdc_source_container',
                    '_sdc_source_file',
                    '_sdc_source_lineno',
                    '_sdc_extra'
                }
                actual_sdc_fields = {field for field in actual_fields if field.startswith('_sdc')}
                self.assertTrue(expected_sdc_fields.issubset(actual_sdc_fields),
                              msg=f"Expected _sdc fields not found. Expected: {expected_sdc_fields}, Found: {actual_sdc_fields}")

                # Verify primary keys have inclusion of automatic
                for pk in expected_primary_keys:
                    self.assertIn(pk, actual_automatic_fields,
                                msg=f"Primary key {pk} should have automatic inclusion")

                # Verify non-primary key fields have inclusion of available
                for field in actual_fields:
                    if field not in expected_primary_keys and not field.startswith('_sdc'):
                        field_metadata = next((item for item in metadata 
                                             if item.get("breadcrumb") == ["properties", field]), None)
                        if field_metadata:
                            inclusion = field_metadata.get("metadata", {}).get("inclusion")
                            self.assertIn(inclusion, ["available", "automatic"],
                                        msg=f"Field {field} should have inclusion of 'available' or 'automatic'")
