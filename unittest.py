import unittest
from unittest.mock import patch, MagicMock
from main import (
    write_temp_credentials,
    build_credentials,
    build_dimension_filter,
    run_query_for_property_optimized,  # Fixed: was run_query_for_property
    StreamingCSVWriter,  # Added missing import
    discover_all_properties_optimized  # Fixed: was discover_all_properties
)

class TestGA4Connector(unittest.TestCase):

    def test_write_temp_credentials(self):
        creds = {"client_email": "test@test.com"}
        path = write_temp_credentials(creds)
        self.assertTrue(path.startswith("/tmp/"))

    def test_build_dimension_filter_none(self):
        result = build_dimension_filter(None)
        self.assertIsNone(result)

    def test_build_dimension_filter_with_conditions(self):
        filter_config = {
            "and_group": [
                {"field_name": "country", "string_filter": {"value": "Turkey"}},
                {"field_name": "platform", "string_filter": {"value": "Web"}}
            ]
        }
        expr = build_dimension_filter(filter_config)
        self.assertIsNotNone(expr)
        self.assertTrue(hasattr(expr, 'and_group'))

    @patch("main.BetaAnalyticsDataClient")
    def test_run_query_for_property_optimized_mock(self, mock_client):
        mock_instance = MagicMock()
        mock_client.return_value = mock_instance
        mock_instance.run_report.return_value.rows = []

        result = run_query_for_property_optimized(
            data_client=mock_instance,
            query_name="test_query",
            dimensions=[],
            metrics=[],
            date_range=MagicMock(),
            dimension_filter=None,
            prop={"property_id": "123", "account_id": "acc"}
        )
        self.assertEqual(result, [])

    @patch("main.os.makedirs")
    def test_streaming_csv_writer(self, mock_makedirs):
        # Test StreamingCSVWriter functionality
        writer = StreamingCSVWriter("/tmp/test.csv", chunk_size=2)
        test_data = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
        writer.add_rows(test_data)
        # Should flush automatically when chunk_size is reached
        self.assertTrue(mock_makedirs.called)

    @patch("main.create_admin_client")
    def test_discover_all_properties_handles_exception(self, mock_admin):
        mock_admin.side_effect = Exception("API error")
        result = discover_all_properties_optimized({"client_email": "test@test.com"})
        self.assertEqual(result, [])

if __name__ == '__main__':
    unittest.main()
