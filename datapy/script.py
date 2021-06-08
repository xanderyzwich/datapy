import re


comparators = {
    '=': lambda x, y: x == y,
    '>': lambda x, y: x > y,
    '>=': lambda x, y: x >= y,
    '<': lambda x, y: x < y,
    '<=': lambda x, y: x <= y,
    '!=': lambda x, y: x != y,
    '<>': lambda x, y: x != y,
}


def encode_delimiter(delimiter):
    return r'[\s]*[' + delimiter + r'][\s]*'


def encode_query(query_string):
    keywords = {
        'select': 0,
        'from': 1,
        'where': 2,
    }
    selected, data_name, where = list(), '', list()
    phase = 0
    for word in query_string.split(' '):
        if word in keywords:
            phase = keywords[word]
        elif phase == 0:
            if word == '*':
                col_name = word
            elif '$' == word[0]:
                col_name = int(word[1:])-1
            else:
                print("Something is wrong in encode_query phase zero!")
            selected.append(col_name)
        elif phase == 1:
            data_name = word
        elif phase == 2:
            where.append(word)
    print(f'select "{selected}" from "{data_name}" where "{where}"')
    return selected, data_name, where


def return_relevant(row, parts=['*']):
    if parts == ['*']:
        return ', '.join(row)
    cols = list(row)
    result_cols = list()
    print('parts', parts, 'row', row)
    for col_id in parts:
        print('cold_id info:', col_id, cols[int(col_id)])
        result_cols.append(cols[int(col_id)])
    result = ', '.join(result_cols) if len(result_cols) > 1 else str(result_cols[0])
    print('result', result)
    return result


def execute_query(return_cols, data_file_name, delimiter, constraints):
    delimiter_regex = encode_delimiter(delimiter)
    results = []
    with open(data_file_name, 'r') as data_file:
        for line in data_file:
            tokens = re.split(delimiter_regex, line.rstrip())
            results.append(return_relevant(tokens, return_cols))
    return results
