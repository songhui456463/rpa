import os
from datetime import datetime

from datasource_connect import get_connetcion
from file_handle import file_is_update, extract_file_title, extract_prefix


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
        with get_connetcion() as conn:
            last_file_time = collect_rows(get_file_last_update_time(conn, folder_path))
            new_list = list(filter(lambda f: folder_paths[folder_path](f) not in last_file_time, target))
            update_file_stats(conn, folder_path, new_list, folder_paths[folder_path], True)
            update_list = list(filter(lambda f: folder_paths[folder_path](f) in last_file_time and file_is_update(
                os.path.join(folder_path, f), last_file_time[folder_paths[folder_path](f)]), target))
            update_file_stats(conn, folder_path, update_list, folder_paths[folder_path])
            result[folder_path] = update_list + new_list

    return result


def get_file_last_update_time(conn, folder_path):
    with conn.cursor() as cur:
        sql = "select file_name, last_modify_time from local_file_stats where folder_path = %s"
        cur.execute(sql, folder_path)
        return cur.fetchall()


def update_file_stats(conn, folder_path, file_name_list, filter_function, is_new=False):
    for file_name in file_name_list:
        file_path = os.path.join(folder_path, file_name)
        modify_time = datetime.fromtimestamp(os.path.getmtime(file_path))
        with conn.cursor() as cur:
            if is_new:
                sql = "insert into local_file_stats (folder_path, file_name, last_modify_time) values (%s, %s, %s)"
            else:
                sql = "update local_file_stats set last_modify_time = %s where folder_path = %s and file_name = %s"
            cur.execute(sql, (folder_path, filter_function(file_name), modify_time))
            conn.commit()


def get_mysteel_indicators_date_map(conn):
    with conn.cursor() as cur:
        sql = "select INDICATOR_NAME, MAX(DATE) from TXBBG01 group by INDICATOR_NAME"
        cur.execute(sql)
        return collect_rows(cur.fetchall())


def get_ths_indicators_date_map(conn):
    with conn.cursor() as cur:
        sql = "select INDICATOR_NAME, MAX(DATE) from TXBBG02 group by INDICATOR_NAME"
        cur.execute(sql)
        return collect_rows(cur.fetchall())


def collect_rows(rows):
    group = {}
    for row in rows:
        key = row[0]
        group[key] = row[1]
    return group


if __name__ == '__main__':
    folder_paths = {'.././data/mysteel': extract_file_title, '.././data/ths': extract_prefix}
    filter_update_files(folder_paths)
