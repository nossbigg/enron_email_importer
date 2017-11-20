import glob
import gzip
import os
import pickle
import sys

from importers.mongo_importer import MongoImporter

mongodb_config_file = "./config/mongo_config.yml"


def main(args):
    if len(args) != 2:
        print("Enron mail data directory not supplied!")
        return False

    DATA_DIR = args[1]

    parse_data_dir(DATA_DIR)

    return True


def parse_data_dir(data_directory_path):
    mongodb_config_file_fullpath = os.path.abspath(mongodb_config_file)
    mongo_importer = MongoImporter(mongodb_config_file_fullpath)

    for gz_file_path in get_gz_files(data_directory_path):
        user_data_dict = get_dict_from_gz(gz_file_path)
        user_name, user_data = user_data_dict.popitem()

        mongo_importer.import_to_mongo(user_name, user_data)


def get_gz_files(data_directory_path):
    return glob.glob(os.path.join(data_directory_path, "*.gz"))


def get_dict_from_gz(gz_file_path):
    with gzip.open(gz_file_path, 'rb') as f:
        data = pickle.load(f)

    return data


if __name__ == '__main__':
    main(sys.argv)
