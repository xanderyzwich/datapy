"""
The interface for select statements
"""
import re

functions = {
    '=': lambda x, y: x == y
}


def word_indexes(word, string):
    word_start = string.find(word)
    word_end = word_start + len(word) + 1
    return word_start, word_end


def select(statement):
    words = re.split(' ', statement)

    select_start, select_end = word_indexes('select', statement)
    from_start, from_end = word_indexes('from', statement)
    # print(words)

    cols = statement[select_end:from_start].strip(' ').split(', ')
    print('select', cols)

    where_start, where_end = word_indexes('where', statement)
    table_meta = statement[from_end:where_start]
    if 'as' in table_meta:
        as_start, as_end = word_indexes('as', table_meta)
        table_name = table_meta[:as_start]
        data_cols = table_meta[as_end:].strip(' ').replace('(', '').replace(')', '').split(', ')
        print('from', table_name, data_cols)
    else:
        table_name = table_meta
        print(table_name)

    predicate = statement[where_end:]
    print('where', predicate)

    """Currently assuming singe predicate statement"""
    left, operator, right = predicate.split(' ')
    print(f'{left} {operator} {right} =', functions[operator](left, right))


if __name__ == '__main__':
    """With cols names provided"""
    query = "select name, age from data.csv as (age, name, job, phone) where age = 49"
    select(query)

    """Without cols names provided"""
    query = "select $2, $1 from data.csv where $1 = 49"
    select(query)

