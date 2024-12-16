# 文件处理
import os

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