from file_handle import extract_file_title, extract_prefix
from reader.read_insert_method import insert_data_ths, insert_data_mysteel

file_construct = {
    'mysteel': {
        'folder_path': '.././data/mysteel',
        'filter_function': extract_file_title,
        'read_insert_function': insert_data_mysteel,
        'col': {
            'date_col': 0,
            'data_start_col': 1,
        },
        'row': {
            'indicator_name': 1,
            'indicator_unit': 2,
            'indicator_resource': 3,
            'indicator_id': 4,
            'indicator_frequency': 5,
            'data_start_row': 7,
        },
        'custom_row': {
            'descript': 6
        }
    },
    'ths': {
        'folder_path': '.././data/ths',
        'filter_function': extract_prefix,
        'read_insert_function': insert_data_ths,
        'col': {
            'date_col': 0,
            'data_start_col': 1,
        },
        'row': {
            'indicator_name': 0,
            'indicator_frequency': 1,
            'indicator_unit': 2,
            'indicator_id': 3,
            'indicator_resource': 4,
            'data_start_row': 6,
        },
        'custom_row': {
        }
    }
}
