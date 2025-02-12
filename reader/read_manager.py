import os
from datetime import datetime

import pandas as pd

from datasource_connect import get_connetcion
from reader.category_map import category_map
from reader.data_map_construct import get_folder_paths
from reader.incremental_updating import filter_update_files, TABLE_NAME
from reader.local_file_map import file_construct
from utils.data_structure import is_stop_indicator, generate_uuid, collect_rows

# 如果使用 mysql 将此占位符改为 '%s'
db_hold = '?'


def run(database='dm'):
    if database == 'mysql':
        global db_hold
        db_hold = '%s'
    with get_connetcion(database) as connection:
        indicators_date_map = get_indicators_id_date_map(connection)
        file_to_update_map = filter_update_files(get_folder_paths())
        read_and_insert_data(insert_data_ths, indicators_date_map, file_construct['ths'],
                             file_to_update_map[file_construct['ths']['folder_path']], connection)
        read_and_insert_data(insert_data_mysteel, indicators_date_map, file_construct['mysteel'],
                             file_to_update_map[file_construct['mysteel']['folder_path']], connection)


def read_and_insert_data(insert_function, indicators_date_map, data_construct, file_list, conn):
    folder_path = data_construct['folder_path']
    exist_indicator = set()
    for file in file_list:
        df = pd.read_excel(os.path.join(folder_path, file), header=None).dropna(how='all')
        indicator_name_row = df.iloc[data_construct['row']['indicator_name']]
        data_col_size = indicator_name_row.size
        for i in range(data_construct['col']['data_start_col'], data_col_size):
            insert_function(indicators_date_map, data_construct,
                            df.iloc[:, [data_construct['col']['date_col'], i]],
                            data_construct['filter_function'](file), conn, exist_indicator)


def insert_data_ths(indicators_date_map, data_construct, df, file_name, conn, exist_indicator):
    df = df.dropna(subset=[df.columns[1]])
    indicator_name = df.iloc[data_construct['row']['indicator_name'], 1]
    indicator_frequency = df.iloc[data_construct['row']['indicator_frequency'], 1]
    indicator_id = df.iloc[data_construct['row']['indicator_id'], 1]
    indicator_unit = df.iloc[data_construct['row']['indicator_unit'], 1]
    indicator_resource = df.iloc[data_construct['row']['indicator_resource'], 1]
    data_df = df.iloc[data_construct['row']['data_start_row']:, :]
    # new_indicator_flag = False
    if indicator_id in indicators_date_map.keys():
        temp_date = indicators_date_map[indicator_id]
        last_date = temp_date if isinstance(temp_date, datetime) else datetime.combine(temp_date, datetime.min.time())
    else:
        last_date = datetime.min
        # new_indicator_flag = True
    # 同目录下同 id 的指标去重，一次更新不会对同一指标更新两遍
    if indicator_id in exist_indicator:
        return
    else:
        exist_indicator.add(indicator_id)
    if indicator_id in category_map:
        sql_table = category_map[indicator_id]
    else:
        return

    now = datetime.now()
    # 数据入库时间
    current_time = now.strftime('%Y%m%d%H%M%S') + now.strftime('%f')[:3]
    # 缓存指标最后更新日期
    new_date = last_date
    many_data = []
    for index, row in data_df.iterrows():
        current_date = row.iloc[0]
        indicator_value = row.iloc[1]
        # 当前数据的日期小于或者等于缓存日期，说明已经存入数据库，跳过，防止重复入库
        if current_date <= last_date:
            break
        many_data.append((generate_uuid(), current_date, str(indicator_value), indicator_id, indicator_unit,
                          indicator_frequency, indicator_resource, indicator_name, file_name, current_time,
                          current_time))
        new_date = max(new_date, current_date)
    if not many_data:
        return
    with conn.cursor() as cursor:
        insert_sql = f"""
                            INSERT INTO {sql_table} (
                                UUID,
                                RECORD_DATE, 
                                INDICATOR_VALUE, 
                                INDICATOR_ID, 
                                INDICATOR_UNIT, 
                                INDICATOR_FREQUENCY, 
                                INDICATOR_RESOURCE, 
                                INDICATOR_NAME, 
                                INDICATOR_TITLE, 
                                REC_CREATOR, 
                                REC_CREATE_TIME, 
                                REC_REVISOR, 
                                REC_REVISOR_TIME
                            ) 
                            VALUES (
                                {db_hold}, {db_hold}, {db_hold}, {db_hold}, {db_hold}, {db_hold}, {db_hold}, {db_hold}, {db_hold}, 'System', {db_hold}, 'System', {db_hold}
                            );
                        """
        cursor.executemany(insert_sql, many_data)
        conn.commit()


def insert_data_mysteel(indicators_date_map, data_construct, df, file_name, conn, exist_indicator):
    indicator_name = df.iloc[data_construct['row']['indicator_name'], 1]
    indicator_frequency = df.iloc[data_construct['row']['indicator_frequency'], 1]
    indicator_id = df.iloc[data_construct['row']['indicator_id'], 1]
    indicator_unit = df.iloc[data_construct['row']['indicator_unit'], 1]
    indicator_resource = df.iloc[data_construct['row']['indicator_resource'], 1]
    data_df = df.iloc[data_construct['row']['data_start_row']:, :].dropna(subset=[df.columns[1]])
    if is_stop_indicator(indicator_name):
        # 需要记录日志，提醒指标已停用
        return
    if indicator_id in indicators_date_map.keys():
        temp_date = indicators_date_map[indicator_id]
        last_date = temp_date if isinstance(temp_date, datetime) else datetime.combine(temp_date, datetime.min.time())
    else:
        last_date = datetime.min
    # 同一指标
    if indicator_name in exist_indicator:
        return
    else:
        exist_indicator.add(indicator_name)
    if indicator_id in category_map:
        sql_table = category_map[indicator_id]
    else:
        return
    now = datetime.now()
    current_time = now.strftime('%Y%m%d%H%M%S') + now.strftime('%f')[:3]
    many_data = []
    for index, row in data_df.iterrows():
        current_date = row.iloc[0]
        current_value = row.iloc[1]
        if current_date <= last_date:
            break
        many_data.append((generate_uuid(), current_date, indicator_id, indicator_unit, indicator_name,
                          indicator_frequency, indicator_resource, str(current_value), file_name, current_time,
                          current_time))
    with conn.cursor() as cursor:
        insert_sql = f"""insert into {sql_table} (
                            UUID,
                            RECORD_DATE, 
                            INDICATOR_ID, 
                            INDICATOR_UNIT, 
                            INDICATOR_NAME,
                            INDICATOR_FREQUENCY, 
                            INDICATOR_RESOURCE, 
                            INDICATOR_VALUE,
                            INDICATOR_TITLE, 
                            REC_CREATOR, 
                            REC_CREATE_TIME,
                            REC_REVISOR,
                            REC_REVISOR_TIME) values ({db_hold}, {db_hold}, {db_hold}, {db_hold}, {db_hold}, {db_hold}, {db_hold}, {db_hold}, {db_hold}, 'System', {db_hold}, 'System', {db_hold})"""
        try:
            cursor.executemany(insert_sql, many_data)
        except Exception as e:
            print(e)
        conn.commit()


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