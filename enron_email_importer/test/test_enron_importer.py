from unittest import TestCase
from unittest.mock import patch, call

from enron_email_importer import enron_importer


class TestEnronImporter(TestCase):
    def test_enron_importer_fail_missing_data_directory(self):
        self.assertFalse(enron_importer.main([]))

    @patch('pickle.load')
    @patch('gzip.open')
    @patch('glob.glob')
    @patch('os.path.join')
    def test_enron_importer_parses_data(self,
                                        mock_os_path_join,
                                        mock_glob_glob,
                                        mock_gzip_open,
                                        mock_pickle_load):
        mock_os_path_join.return_value = "some-dir"
        mock_glob_glob.return_value = ['file-path-1', 'file-path-2']
        mock_pickle_load.side_effect = lambda x: {'some-key': 'some-value'}

        result = enron_importer.main(["total-garbage", "some-data-dir"])

        self.assertTrue(result)
        mock_os_path_join.assert_called_with('some-data-dir', '*.gz')
        mock_gzip_open.assert_has_calls([call('file-path-1', 'rb'),
                                         call('file-path-2', 'rb')]
                                        , any_order=True)
