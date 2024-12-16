from data_crud import insert_data
from datasource_connect import get_connetcion
from file_handle import get_file_names

conn = get_connetcion('mysql')

file_paths = get_file_names('./data/ths/')

insert_data(file_paths, conn)


