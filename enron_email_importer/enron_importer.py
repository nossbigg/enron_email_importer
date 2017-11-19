import glob
import gzip
import pickle
import sys

import os


def main(args):
    if len(args) != 2:
        print("Enron mail data directory not supplied!")
        return False

    DATA_DIR = args[1]

    parse_data_dir(DATA_DIR)

    return True


def parse_data_dir(data_directory_path):
    for gz_file_path in get_gz_files(data_directory_path):
        user_data_dict = get_dict_from_gz(gz_file_path)
        user_name, user_data = user_data_dict.popitem()


def get_gz_files(data_directory_path):
    return glob.glob(os.path.join(data_directory_path, "*.gz"))


def get_dict_from_gz(gz_file_path):
    with gzip.open(gz_file_path, 'rb') as f:
        data = pickle.load(f)

    return data


if __name__ == '__main__':
    main(sys.argv)
