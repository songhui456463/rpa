# 文件处理
import os
import re
from datetime import datetime


# 获取一个文件夹下所有文件的文件路径
def get_file_names(folder_path=''):
    # 路径列表
    file_paths = []

    # os.walk() 遍历文件夹及其子文件夹
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            # 排除以 ~$ 开头的文件（临时文件）
            if file.startswith('~$'):
                continue

            # 获取完整文件路径
            file_path = os.path.join(root, file)
            file_paths.append(file_path)
    return file_paths


# 定义提取文件名中第一个下划线前的部分
def extract_prefix(file_name):
    base_name = os.path.splitext(file_name)[0]  # 去掉扩展名
    parts = base_name.split('_')  # 根据下划线分割
    return parts[0]


def file_is_update(file_name, last_time):
    current_time = datetime.fromtimestamp(os.path.getmtime(file_name)).replace(microsecond=0)
    if isinstance(last_time, str):
        last_time = datetime.strptime(last_time, '%Y-%m-%d %H:%M:%S')
    return current_time > last_time


def extract_file_title(file_name):
    base_name = os.path.splitext(file_name)[0]
    pattern = r'_(\d{4}-\d{1,2}-\d{1,2})_\d+$'
    return re.sub(pattern, '', base_name)

if __name__ == '__main__':
    file_list = os.listdir('./data/mysteel')
    for file_name in file_list:
        extract_file_title(file_name)