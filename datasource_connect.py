# 数据库连接配置
import pymysql
import dmPython

# 本地mysql数据库连接配置
db_local_mysql_config = {
    'host': 'localhost',  # 数据库地址
    'user': 'root',  # 数据库用户名
    'password': '123456',  # 数据库密码
    'database': 'hmdp',  # 数据库名称
    'port': 3306  # 数据库端口
}

# 达梦数据库连接配置
db_remote_dameng_config = {
    'server': '10.81.57.3',  # 数据库地址
    'user': 'iplat4j',  # 数据库用户名
    'password': 'dameng123',  # 数据库密码
    'schema': 'IAI',  # 数据库名称
    'port': 5236  # 数据库端口
}

# 获取数据库连接
def get_connetcion(database_name='mysql'):
    try:
        if database_name == 'mysql':
            db_config = db_local_mysql_config
            conn = pymysql.connect(**db_config)
        else:
            db_config = db_remote_dameng_config
            conn = dmPython.connect(**db_config)
    except Exception as e:
        print('python: conn fail!')
        print(e)
    print('python: conn success!')
    return conn
