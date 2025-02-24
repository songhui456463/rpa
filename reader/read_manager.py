import os
import sys

# 设置工作目录
work_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(work_dir)
sys.path.append(root_dir)
os.chdir(work_dir)

import pandas as pd

from datasource_connect import get_connetcion
from reader.data_map_construct import get_folder_paths
from reader.incremental_updating import filter_update_files, TABLE_NAME
from reader.local_file_map import file_construct
from utils.data_structure import collect_rows

# 如果使用 mysql 将此占位符改为 '%s'
db_hold = '?'


def run(database='dm'):
    if database == 'mysql':
        global db_hold
        db_hold = '%s'
    with get_connetcion(database) as connection:
        indicators_date_map = get_indicators_id_date_map(connection)
        file_to_update_map = filter_update_files(get_folder_paths())
        # 遍历配置表，对每个文件目录进行操作
        for config in file_construct.values():
            read_and_insert_data(config['read_insert_function'], indicators_date_map, config,
                                 file_to_update_map[config['folder_path']], connection)


def read_and_insert_data(insert_function, indicators_date_map, data_construct, file_list, conn):
    """
    解析一个文件目录下所有数据文件

    :param insert_function:     数据解析插入方法
    :param indicators_date_map: 数据库已有指标-最新更新时间 map
    :param data_construct:      local_file_map.file_construct 中的一个类别，包含同目录下文件的解析规则
    :param file_list:           目录下的文件列表
    :param conn:                数据库连接对象
    """
    folder_path = data_construct['folder_path']
    exist_indicator = set()
    # 遍历目录下的文件
    for file in file_list:
        df = pd.read_excel(os.path.join(folder_path, file), header=None).dropna(how='all')
        indicator_name_row = df.iloc[data_construct['row']['indicator_name']]
        data_col_size = indicator_name_row.size
        # 遍历所有数据列，按列执行插入数据库的操作
        for i in range(data_construct['col']['data_start_col'], data_col_size):
            insert_function(indicators_date_map, data_construct,
                            df.iloc[:, [data_construct['col']['date_col'], i]],
                            data_construct['filter_function'](file), conn, exist_indicator, db_hold)


def get_indicators_id_date_map(conn):
    result = []
    with conn.cursor() as cur:
        for table_name in TABLE_NAME:
            sql = f"""select INDICATOR_ID, MAX(RECORD_DATE) from {table_name} group by INDICATOR_ID"""
            cur.execute(sql)
            result.extend(cur.fetchall())

    return collect_rows(result)


if __name__ == '__main__':
    run()