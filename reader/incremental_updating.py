import os
from datetime import datetime

import pandas as pd

from file_handle import file_is_update, extract_file_title, extract_prefix

# 记录目录中文件状态，据此文件来进行文件层面的增量更新
LOCAL_FILE_STATUS = 'file_status.csv'

TABLE_NAME = ['TXBBG01',
              'TXBBG02',
              'TXBBG03',
              'TXBBG04',
              'TXBBG05',
              'TXBBG06',
              'TXBBG07']

def filter_update_files(folder_paths):
    """
    通过本地文件修改时间过滤出需要更新数据的文件，分目录返回
    :param folder_paths: 目录列表
    :return: 需要更新的文件列表，按目录分类，例如
    {
        'folder_path':[file_name1,file_name2,...],
        'folder_path2':[file_name1,file_name2,...],
        ...
    }
    """
    result = {}
    for folder_path in folder_paths.keys():
        file_list = os.listdir(folder_path)
        target = list(
            filter(lambda f: not f.startswith('~$') and (f.endswith('.xlsx') or f.endswith('.xls')), file_list))
        df = get_file_last_update_time(folder_path)
        last_file_time = collect_file_status(df)
        new_list = list(filter(lambda f: folder_paths[folder_path](f) not in last_file_time, target))
        update_list = list(filter(lambda f: folder_paths[folder_path](f) in last_file_time and file_is_update(
            os.path.join(folder_path, f), last_file_time[folder_paths[folder_path](f)]), target))
        update_file_stats(folder_path, target, folder_paths[folder_path])
        result[folder_path] = update_list + new_list

    return result


def get_file_last_update_time(folder_path):
    """
    读取文件目录下的状态记录文件，获取上次的文件更新状态
    :param folder_path: 目标文件目录
    :return: df对象，[(filename,last_update_time)]
    """
    file = os.path.join(folder_path, LOCAL_FILE_STATUS)
    if not os.path.exists(file):
        # 没有文件就创建
        with open(file, 'w', encoding='utf-8') as f:
            pass
        return pd.DataFrame()
    df = pd.read_csv(file, header=None)
    return df


def update_file_stats(folder_path, file_name_list, filter_function):
    """
    更新文件状态
    :param folder_path: 目标目录
    :param file_name_list:  文件名列表
    :param filter_function:  文件名过滤器
    """
    file_status_list = []
    for file_name in file_name_list:
        file_path = os.path.join(folder_path, file_name)
        modify_time = datetime.fromtimestamp(os.path.getmtime(file_path)).replace(microsecond=0)
        row = [filter_function(file_name), modify_time]
        file_status_list.append(row)
    df = pd.DataFrame(file_status_list)
    df.to_csv(os.path.join(folder_path, LOCAL_FILE_STATUS), index=False, header=False)


def collect_file_status(df):
    """
    将 DataFrame 中的数据第一列作为 key，第二列作为值，形成 dict
    :param df: 原始 DataFrame
    :return: 组合好的 dict，形式例：
            {
                file_name: last_update_time,
                ...
            }
    """
    group = {}
    for index, row in df.iterrows():
        key = row[0]
        group[key] = row[1]

    return group


if __name__ == '__main__':
    folder_paths = {'.././data/mysteel': extract_file_title, '.././data/ths': extract_prefix}
    result = filter_update_files(folder_paths)
    print(result)
