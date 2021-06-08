import re
from unittest import TestCase
import script


class TestScript(TestCase):
    file_name = 'test_data.csv'

    def test_encode_delimiter(self):
        delimiter_options = [',', '|', ';', ":", '\t']
        for d in delimiter_options:
            delimiter_regex = script.encode_delimiter(',')
            assert re.split(delimiter_regex, 'A,B,C') == ['A', 'B', 'C']
            assert re.split(delimiter_regex, 'A, B, C') == ['A', 'B', 'C']
            assert re.split(delimiter_regex, 'A ,    B       ,C') == ['A', 'B', 'C']

    def test_encode_query(self):
        assert script.encode_query('select * from thing') == (['*'], 'thing', [])
        assert script.encode_query('select $1 from thing') == ([0], 'thing', [])

    def test_return_relevant(self):
        assert script.return_relevant(['A', 'B', 'C'], parts=[0]) == 'A'
        assert script.return_relevant(['A', 'B', 'C'], parts=[0, 1]) == 'A, B'

    def test_temp_usage(self):
        result_col_names, data_set, statements = script.encode_query(f'select * from {self.file_name}')
        result_rows = script.execute_query(result_col_names, data_set, ',', statements)
        print('\n'.join(result_rows))



