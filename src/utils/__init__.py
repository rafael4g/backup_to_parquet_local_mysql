from typing import List
from sqlalchemy import create_engine, text
from unicodedata import normalize
from datetime import datetime
import re
import pandas as pd
import hashlib
from decouple import config

MYSQL_USER = config('MYSQL_USER')
MYSQL_PASS = config('MYSQL_PASS')
MYSQL_HOST = config('MYSQL_HOST')
MYSQL_PORT = config('MYSQL_PORT')
MYSQL_DEFAULT_DB = config('MYSQL_DEFAULT_DB')
ENV_BRONZE = config('ENV_BRONZE')
PATH_BUCKET = config('PATH_BUCKET')
DUCKDB_DATABASE = config('DUCKDB_DATABASE')
                            
DATETIME_HOUR_MINUTES = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")

def handle_conect_db(_mysql_db_name: str): # -> create_engine:
    # -- handle_conect_db
    engine = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASS}@{MYSQL_HOST}:{MYSQL_PORT}/{_mysql_db_name}')
    return engine

def create_db_or_connect(db_name: str):
    # -- Verifica se o banco de dados existe e, se nao, cria.    
    engine = handle_conect_db(MYSQL_DEFAULT_DB)
    
    with engine.connect() as conn:      
        result = conn.execute(f"SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '{db_name}'").fetchone()

        if not result:          
            conn.execute(f"CREATE DATABASE {db_name}")
            print(f"Banco de dados '{db_name}' criado com sucesso!")
    return handle_conect_db(db_name)   


def execute_sql_commands(sql_file, db_name, flag=None):
    # -- execute_sql_commands
    engine = create_db_or_connect(db_name)
    
    with open(sql_file, "r", encoding="ISO-8859-1") as f:
        sql_script = f.read()

    comandos = sql_script.split(";")
    if flag:        
        comandos = sql_script.split("#450e")


    with engine.connect() as conn:
        for comando in comandos:
            comando = comando.strip() 
            if comando:               
                conn.execute(text(comando))  

def upload_parquet_files(path_parquet_dir, file_name, db_name):
    # upload_parquet_files
    engine = create_db_or_connect(db_name)
    #table_name = file_name.replace(".parquet", "")

    match = re.match(r"(.*)_part_\d+\.parquet", file_name)
    if match:
        table_name = match.group(1)
        
    df = pd.read_parquet(f'{path_parquet_dir}/{file_name}')
    df.to_sql(table_name, con=engine, if_exists="append", index=False)
    print(f"✅ Parquet refer {table_name} restaurados de {file_name}")


def handle_strip_string(str1_in: str) -> str:
    # -- Fun��o para remover objetos de strings
    # -- str1_in: string de entrada
 
    convert_string = str(str1_in)
    clear_obj = re.sub(r"^\s+|\s+$", "", convert_string)
    strip_string = clear_obj.strip().replace(' ', '').upper()  # equal TRIM
    hash_string_stripped = hashlib.md5(strip_string.encode())
    return hash_string_stripped.hexdigest()

def handle_normalize_strings(in_string: str) -> str:
    # -- handle_normalize_strings
    target = normalize('NFKD', in_string).encode('ASCII','ignore').decode('ASCII')
    target = target.replace('.','')
    target = target.replace('(','')
    target = target.replace(')','')
    target = target.replace('/','')
    target = target.replace('-','')
    return target

# comparando e adcionando colunas faltantes ao dataset original
def handle_headers_comparation(header_list, header_original) -> list:
    # -- handle_headers_comparation
    new_list = []
    for i in header_list:
        if i in header_original:
            pass
        else:
            new_list.append(i)
    return new_list

def handle_without_zero(in_string: str) -> str:
    # -- handle_without_zero
    _str_in = str(in_string)
    target = _str_in.replace('.0', '')
    target = target.strip()
    if target == '-3':
        str_output = 0
    else:
        str_output = target
    return str_output

def handle_ymonth(_dt: datetime) -> int:
    # -- handle_ymonth  
    s_year = _dt.year
    s_month = _dt.month
    s_ymonth = (s_year * 100 + s_month)
    return s_ymonth                                                        

def convert_datetime_to_timestamp_unix(_date: str) -> int:
    # -- convert_datetime_to_timestamp_unix    
	_date_datetime = datetime.strptime(_date, '%Y-%m-%d %H:%M')
	_data_unix = int(_date_datetime.timestamp())    
	return _data_unix

def convert_timestamp_unix_to_datetime(_date: str) -> int:
    # -- convert_timestamp_unix_to_datetime    
	_date_datetime = datetime.fromtimestamp(_date)    
	return _date_datetime
                            
def handle_divide_into_groups(_list, _group_size) -> List[str]:
    group = [_list[i:i + _group_size] for i in range(0, len(_list), _group_size)]
    return group                              

if __name__ == '__main__':
    print('Tested!')


