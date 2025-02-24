## 脚本运行
### 1 安装依赖

pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple/

### 2 创建数据库
导入sql文件下的脚本

### 3 运行脚本
直接运行 reader/read_manager.py

## 项目结构
```
root
|   datasource_connect.py   管理数据库连接
|   data_crud.py            文件解析/入库示例代码
|   file.txt
|   file_handle.py
|   README.md
|   requirements.txt        依赖
|   ths.py
|
+---data    数据目录
|   +---mysteel             
|   |       file_status.csv 当前目录文件更新时间记录
|   |       
|   \---ths
|           file_status.csv 当前目录文件更新时间记录
|           
+---reader  解析/入库核心代码
|       category_map.py             指标分类表
|       data_map_construct.py       
|       incremental_updating.py     文件增量更新处理
|       local_file_map.py           本地文件配置表
|       read_insert_method.py       解析/入库代码，新增数据来源可以在此增加解析方法
|       read_manager.py             脚本入口
|           
+---sql     sql建表语句
|   |   txdai01.sql
|   |   
|   +---dmdb    达梦数据库建表脚本
|   |       价格_dm.sql
|   |       供应_dm.sql
|   |       双碳_dm.sql
|   |       宏观数据_dm.sql
|   |       库存_dm.sql
|   |       成本利润_dm.sql
|   |       需求_dm.sql
|   |       
|   \---mysql   mysql建表脚本
|           价格_mysql.sql
|           供应_mysql.sql
|           双碳_mysql.sql
|           宏观数据_mysql.sql
|           库存_mysql.sql
|           成本利润_mysql.sql
|           需求_mysql.sql
|           
\---utils
        data_structure.py       数据结构工具类
```
