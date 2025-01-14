import os

import pandas as pd

from reader.local_file_map import file_construct


def get_folder_paths():
    folder_paths = {}
    for key in file_construct.keys():
        folder_paths[file_construct[key]['folder_path']] = file_construct[key]['filter_function']

    return folder_paths

def get_data_file_map_from_folder(file_construct):
    data_file_map = {}
    for filename in os.listdir(file_construct.folder_path):
        if filename.endswith(".xlsx") or filename.endswith(".xls"):
            file_path = os.path.join(file_construct.folder_path, filename)
            df = pd.read_excel(file_path).dropna(how='all')
            indicator_name_row = df.iloc(file_construct.row.indicator_name)

            data_col_size = indicator_name_row.size
            for i in range(file_construct.col.start,data_col_size):
                indicator_name = indicator_name_row[i]


def test(file_path='.././data/mysteel/中国粗钢产量钢联数据_行业数据_2024-12-3_1733193254732.xlsx'):
    df = pd.read_excel(file_path).dropna(how='all')
    data = df

if __name__ == '__main__':
    test()