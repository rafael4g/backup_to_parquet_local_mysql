import src.utils as utils
import pandas as pd #type: ignore
import os

DB_NAME = "db_movel"
# Conectar ao banco
#engine = utils.create_db_or_connect(DB_NAME)

# Diretório do Backup
backup_dir = f'./src/backup_{DB_NAME}'
tables_dir = os.path.join(backup_dir, "schema_tables").replace('\\','/')
index_dir = os.path.join(backup_dir, "schema_indices").replace('\\','/')
functions_dir = os.path.join(backup_dir, "schema_functions").replace('\\','/')
procedures_dir = os.path.join(backup_dir, "schema_procedures").replace('\\','/')
parquet_dir = os.path.join(backup_dir, "tables_parquet").replace('\\','/')
# ==============================
# 1️⃣ RESTAURAR ESTRUTURA DAS TABELAS
# ==============================
for file in os.listdir(tables_dir):
    if file.endswith(".sql"):     
        file_read = f'{tables_dir}/{file}'            
        utils.execute_sql_commands(sql_file=file_read, db_name=DB_NAME)
        print(f"✅ Estrutura das tabelas restaurada de {file}")
# ==============================
# 3️⃣ RESTAURAR STORED PROCEDURES
# ==============================
for file in os.listdir(procedures_dir):
    if file.endswith(".sql"):     
        file_read = f'{procedures_dir}/{file}'            
        utils.execute_sql_commands(sql_file=file_read, db_name=DB_NAME, flag=True)
        print(f"✅ Stored Procedures restauradas de {file}")
# ==============================
# 4️⃣ RESTAURAR FUNCTIONS
# ==============================
for file in os.listdir(functions_dir):
    if file.endswith(".sql"):     
        file_read = f'{functions_dir}/{file}'            
        utils.execute_sql_commands(sql_file=file_read, db_name=DB_NAME, flag=True)
        print(f"✅ Functions restauradas de {file}")
# ==============================
# 5️⃣ RESTAURAR DADOS DAS TABELAS
# ==============================
for file in os.listdir(parquet_dir):
    if file.endswith(".parquet"):      
        utils.upload_parquet_files(path_parquet_dir=parquet_dir, file_name=file, db_name=DB_NAME)
        
print("\n🎉 Restauração do banco de dados finalizada!")
# ==============================
# 2️⃣ RESTAURAR ÍNDICES
# ==============================
for file in os.listdir(index_dir):
    if file.endswith(".sql"):     
        file_read = f'{index_dir}/{file}'            
        utils.execute_sql_commands(sql_file=file_read, db_name=DB_NAME)
        print(f"✅ Índices restaurados de {file}")
        