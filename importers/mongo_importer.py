import yaml
from pymongo import MongoClient


class MongoImporter:
    def __init__(self, config_file_path):
        self.db_config = self.__read_yaml_config_file(config_file_path)

        self.db_client = self.get_db_client()
        self.db = self.db_client[self.db_config['dbname']]

    def import_to_mongo(self, user_name, user_data):
        collection = self.get_collection(user_name)

        user_data_mongo = self.convert_to_mongo_insert_format(user_data)
        collection.insert_many(user_data_mongo)

    @staticmethod
    def convert_to_mongo_insert_format(user_data_dict):
        return [{folder_name: messages} for folder_name, messages in
                MongoImporter.remove_dots_from_keys(user_data_dict).items()]

    @staticmethod
    def remove_dots_from_keys(data_dict):
        new_dict = {}
        for key, value in data_dict.items():
            if isinstance(value, dict):
                value = MongoImporter.remove_dots_from_keys(value)
            key_without_dots = key.replace('.', '')
            new_dict[key_without_dots] = value

        return new_dict

    def get_collection(self, collection_name):
        return self.db[collection_name]

    def get_db_client(self):
        return MongoClient(self.db_config['host'], self.db_config['port'])

    @staticmethod
    def __read_yaml_config_file(config_file_path):
        with open(config_file_path, 'r') as f:
            doc = yaml.load(f)

        return doc
