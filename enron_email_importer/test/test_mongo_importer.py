from unittest import TestCase
from unittest.mock import patch, MagicMock

from enron_email_importer.importers.mongo_importer import MongoImporter


class TestMongoImporter(TestCase):
    @patch('importers.mongo_importer.MongoImporter.convert_to_mongo_insert_format')
    @patch('importers.mongo_importer.MongoImporter.get_collection')
    @patch('importers.mongo_importer.MongoClient')
    @patch('yaml.load')
    @patch('builtins.open')
    def test_import_to_mongo(self,
                             mock_open,
                             mock_yaml_load,
                             mock_mongo_client,
                             mock_get_collection,
                             mock_convert_method):
        mock_collection = MagicMock()
        mock_get_collection.return_value = mock_collection
        mock_convert_method.return_value = {"returned-dict": "some-value"}

        mongo_importer = MongoImporter("some_config_path")
        mongo_importer.import_to_mongo("some_user_name", {"some-key": "some-value"})

        mock_collection.insert_many.assert_called_with({"returned-dict": "some-value"})

    def test_remove_dots_from_keys(self):
        data_dict = {"key.": "value."}
        expected_dict = {"key": "value."}

        self.assertEqual(expected_dict,
                         MongoImporter.remove_dots_from_keys(data_dict))

    def test_remove_dots_from_keys_nested_dicts(self):
        data_dict = {
            "key.": {
                "nested-key.": "nested_value"
            }
        }
        expected_dict = {
            "key": {
                "nested-key": "nested_value"
            }
        }

        self.assertEqual(expected_dict,
                         MongoImporter.remove_dots_from_keys(data_dict))
