from unittest import TestCase
from unittest.mock import patch, MagicMock

from importers.mongo_importer import MongoImporter


class TestMongoImporter(TestCase):
    @patch('importers.mongo_importer.MongoImporter.get_collection')
    @patch('importers.mongo_importer.MongoClient')
    @patch('yaml.load')
    @patch('builtins.open')
    def test_import_to_mongo(self,
                             mock_open,
                             mock_yaml_load,
                             mock_mongo_client,
                             mock_get_collection):
        mock_collection = MagicMock()
        mock_get_collection.return_value = mock_collection

        mongo_importer = MongoImporter("some_config_path")
        mongo_importer.import_to_mongo("some_user_name", [{'some-object': "some-value"}])

        mock_collection.insert_many.assert_called_with([{'some-object': "some-value"}])
