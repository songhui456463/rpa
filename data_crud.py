# 数据落表
import os
import pandas as pd
from file_handle import extract_prefix

# 插入数据库
def insert_data(file_paths=None, conn=None):
    # 去重
    exist_id = set()
    cursor = conn.cursor()

    # 输出所有文件的路径
    for path in file_paths:
        # 读取 Excel 文件（假设数据在第一个工作表中）
        df = pd.read_excel(path)

        file_name, file_extension = os.path.splitext(os.path.basename(path))

        # 查找第一行中首次出现空列的位置
        first_row = df.iloc[0]  # 取第一行数据
        pd_cnt = first_row.size  # 计算列数

        # 遍历每一列，组合第一列和其它列
        for i in range(1, pd_cnt):  # 从第二列开始，组合第一列与其他列
            combined_df = df.iloc[:, [0, i]]  # 选择第0列和第i列

            # 若指标 ID 已存在，则跳过
            if combined_df.iloc[2, 1] in exist_id:
                continue

            # 删除包含空值的行
            combined_df = combined_df.dropna(subset=[combined_df.columns[1]])

            # 重置索引，去掉原有的索引列
            combined_df = combined_df.reset_index(drop=True)

            # 提取指标名称、描述、日期和价格
            indicator_name = combined_df.columns[1]
            indicator_frequency = combined_df.iloc[0, 1]
            indicator_unit = combined_df.iloc[1, 1]
            indicator_ID = combined_df.iloc[2, 1]
            indicator_resource = combined_df.iloc[3, 1]
            indicator_title = extract_prefix(file_name)
            indicator_value = combined_df.iloc[5:, :]
            exist_id.add(combined_df.iloc[2, 1])

            # 遍历日期和价格数据并插入数据库
            for index, row in indicator_value.iterrows():
                # 获取日期和价格数据
                date = row[0]  # 第一列为日期
                value = row[1]  # 第二列为值

                insert_query = """
                    INSERT INTO TXDAI01 (
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
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s, NOW()
                    );
                """

                # 设置插入数据，若某些数据不存在则为 None（数据库会自动处理为 NULL）
                data = (
                    date, value, indicator_ID, indicator_unit, indicator_frequency, indicator_resource, indicator_name,
                    indicator_title, None,
                    None)

                # 执行插入操作
                cursor.execute(insert_query, data)

            # 提交事务
            conn.commit()

    # 关闭游标和连接
    cursor.close()
    conn.close()
