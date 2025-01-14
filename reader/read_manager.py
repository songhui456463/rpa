import os
from datetime import datetime

import pandas as pd

from datasource_connect import get_connetcion
from reader.data_map_construct import get_folder_paths
from reader.incremental_updating import get_mysteel_indicators_date_map, get_ths_indicators_date_map, \
    filter_update_files
from reader.local_file_map import file_construct
from utils.data_structure import is_stop_indicator


def run():
    with get_connetcion() as connection:
        mysteel_indicators_date_map = get_mysteel_indicators_date_map(connection)
        ths_indicators_date_map = get_ths_indicators_date_map(connection)
        file_to_update_map = filter_update_files(get_folder_paths())
        read_and_insert_data(insert_data_ths, ths_indicators_date_map, file_construct['ths'],
                             file_to_update_map[file_construct['ths']['folder_path']], connection)
        read_and_insert_data(insert_data_mysteel, mysteel_indicators_date_map, file_construct['mysteel'],
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
        last_date = indicators_date_map[indicator_id]
    else:
        last_date = datetime.min
        # new_indicator_flag = True
    # 同目录下同 id 的指标去重，一次更新不会对同一指标更新两遍
    if indicator_id in exist_indicator:
        return
    else:
        exist_indicator.add(indicator_id)
    with conn.cursor() as cursor:
        insert_sql = """
                    INSERT INTO TXBBG02 (
                        DATE, 
                        INDICATOR_VALUE, 
                        INDICATOR_ID, 
                        INDICATOR_UNIT, 
                        INDICATOR_FREQUENCY, 
                        INDICATOR_RESOURCE, 
                        INDICATOR_NAME, 
                        INDICATOR_TITLE, 
                        REC_CREATOR, 
                        RED_CREATED_TIME, 
                        REC_MODIFIED, 
                        REC_MODIFIED_TIME
                    ) 
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, 'SYS', NOW(), %s, NOW()
                    );
                """
        new_date = last_date
        for index, row in data_df.iterrows():
            current_date = row.iloc[0]
            indicator_value = row.iloc[1]
            if current_date <= last_date:
                break
            cursor.execute(insert_sql, (
                current_date, indicator_value, indicator_id, indicator_unit, indicator_frequency, indicator_resource,
                indicator_name, file_name, None))
            new_date = max(new_date, current_date)
        # 记录指标最后更新日期
        # if new_date != last_date:
        #     if new_indicator_flag:
        #         sql = "insert into "
        #     cursor.execute(insert_sql, ())
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
        last_date = indicators_date_map[indicator_id]
    else:
        last_date = datetime.min
    # 同一指标
    if indicator_name in exist_indicator:
        return
    else:
        exist_indicator.add(indicator_name)
    with conn.cursor() as cursor:
        insert_sql = """insert into TXBBG01 (
                        DATE, 
                        INDICATOR_ID, 
                        INDICATOR_UNIT, 
                        INDICATOR_NAME,
                        INDICATOR_FREQUENCY, 
                        INDICATOR_RESOURCE, 
                        INDICATOR_VALUE,
                        INDICATOR_TITLE, 
                        REC_CREATOR, 
                        REC_CREATED_TIME) values (%s, %s, %s, %s, %s, %s, %s, %s, 'SYS', NOW())"""
        for index, row in data_df.iterrows():
            current_date = row.iloc[0]
            current_value = row.iloc[1]
            if current_date <= last_date:
                break
            try:
                cursor.execute(insert_sql, (
                current_date, indicator_id, indicator_unit, indicator_name, indicator_frequency, indicator_resource,
                current_value, file_name))
            except Exception as e:
                print(current_value)
                print(e)
        conn.commit()


if __name__ == '__main__':
    run()
