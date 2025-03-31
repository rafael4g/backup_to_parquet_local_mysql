import src.utils as utils
import pandas as pd
import os
import math

DB_NAME = "db_movel"

# Conectar ao banco
engine = utils.handle_conect_db(DB_NAME)

# Criar diretório de backup
backup_dir = f'./src/backup_{DB_NAME}'
tables_dir = os.path.join(backup_dir, "schema_tables")
index_dir = os.path.join(backup_dir, "schema_indices")
functions_dir = os.path.join(backup_dir, "schema_functions")
procedures_dir = os.path.join(backup_dir, "schema_procedures")
parquet_dir = os.path.join(backup_dir, "tables_parquet")
PARTITION_SIZE_MB = 180  # Tamanho máximo de cada arquivo parquet

os.makedirs(backup_dir, exist_ok=True)
os.makedirs(tables_dir, exist_ok=True)
os.makedirs(index_dir, exist_ok=True)
os.makedirs(functions_dir, exist_ok=True)
os.makedirs(procedures_dir, exist_ok=True)
os.makedirs(parquet_dir, exist_ok=True)


df_tables = pd.read_sql("SHOW TABLES;", con=engine)
tables = df_tables.iloc[:, 0].tolist()
# ==============================
# 1️⃣ BACKUP DOS ÍNDICES (CREATE INDEX)
# ==============================
indices_sql = ""

for tabela in tables:
    query = f"""
    SELECT INDEX_NAME, COLUMN_NAME, NON_UNIQUE 
    FROM INFORMATION_SCHEMA.STATISTICS 
    WHERE TABLE_SCHEMA = '{DB_NAME}' 
      AND TABLE_NAME = '{tabela}'
      AND INDEX_NAME NOT IN ('PRIMARY');  -- Exclui chave primária
    """
    df_indices = pd.read_sql(query, con=engine)

    for _, row in df_indices.iterrows():
        index_name = row["INDEX_NAME"]
        column_name = row["COLUMN_NAME"]
        is_unique = "UNIQUE" if row["NON_UNIQUE"] == 0 else ""
        indices_sql += f"CREATE {is_unique} INDEX {index_name} ON {tabela}({column_name});\n"

indices_file = os.path.join(index_dir, "indices.sql")
with open(indices_file, "w", encoding="utf-8") as f:
    f.write(indices_sql)

print(f"✅ Índices salvos em {indices_file}")

# ==============================
# 1️⃣.2️⃣ REMOVE ÍNDICES (DROP INDEX)
# ==============================

# Buscar todos os índices que não são PRIMARY KEY
query = """
SELECT TABLE_NAME, INDEX_NAME 
FROM INFORMATION_SCHEMA.STATISTICS 
WHERE TABLE_SCHEMA = %s AND INDEX_NAME != 'PRIMARY';
"""

# Executar a query para listar os índices
with engine.connect() as conn:
    result = conn.execute(query, (DB_NAME,))
    indices = result.fetchall()

# Se houver índices, removê-los
if indices:
    for table_name, index_name in indices:
        drop_index_query = f"DROP INDEX {index_name} ON {table_name};"
        with engine.connect() as conn:
            try:
                conn.execute(drop_index_query)
                print(f"✅ Índice {index_name} removido da tabela {table_name}")
            except Exception as e:
                print(f"⚠️ Erro ao remover {index_name} da tabela {table_name}: {e}")
else:
    print("✅ Nenhum índice encontrado para remover.")
    

# ==============================
# 2️⃣ BACKUP DA ESTRUTURA DAS TABELAS
# ==============================
tables_sql = ""

for tabela in tables:
    df_create = pd.read_sql(f"SHOW CREATE TABLE {tabela};", con=engine)
    tables_sql = f'USE `{DB_NAME}`;\nDROP TABLE IF EXISTS `{tabela}`;\n' + df_create.iloc[0, 1] + ";"

    tables_file = os.path.join(tables_dir, f'{tabela}.sql')
    with open(tables_file, "w", encoding="utf-8") as f:
        f.write(tables_sql)

print(f"✅ Estrutura das tabelas salva em {tables_file}")


# ==============================
# 3️⃣ BACKUP DE STORED PROCEDURES
# ==============================
procedures_sql = ""
df_procs = pd.read_sql(f"SHOW PROCEDURE STATUS WHERE Db = '{DB_NAME}';", con=engine)

for proc in df_procs["Name"]:
    df_create_proc = pd.read_sql(f"SHOW CREATE PROCEDURE {proc};", con=engine)
    create_proc_sql = df_create_proc.iloc[0, 2]
    
    # Adiciona DELIMITER antes e depois
    procedures_sql += f"{create_proc_sql.replace('CREATE DEFINER','CREATE OR REPLACE DEFINER')};#450e\n\n"

procedures_file = os.path.join(procedures_dir, "procedures.sql")
with open(procedures_file, "w", encoding="utf-8") as f:
    f.write(procedures_sql)

print(f"✅ Procedures salvas em {procedures_file}")

# ==============================
# 4️⃣ BACKUP DE FUNCTIONS
# ==============================
functions_sql = ""
df_funcs = pd.read_sql(f"SHOW FUNCTION STATUS WHERE Db = '{DB_NAME}';", con=engine)

for func in df_funcs["Name"]:
    df_create_func = pd.read_sql(f"SHOW CREATE FUNCTION {func};", con=engine)
    create_func_sql = df_create_func.iloc[0, 2]
    functions_sql += f"{create_func_sql.replace('CREATE DEFINER','CREATE OR REPLACE DEFINER')};#450e\n\n"

functions_file = os.path.join(functions_dir, "functions.sql")
with open(functions_file, "w", encoding="utf-8") as f:
    f.write(functions_sql)

print(f"✅ Functions salvas em {functions_file}")

# ==============================
#  5️⃣ BACKUP DOS DADOS EM `.parquet`
# ==============================

for tabela in tables:
    df = pd.read_sql(f"SELECT * FROM {tabela};", con=engine)
    # Calcular o tamanho estimado do DataFrame em bytes
    estimated_size = df.memory_usage(deep=True).sum()
    if estimated_size > 0:
        estimated_size_mb = estimated_size / (1024 * 1024)
        print(f"Tamanho estimado do DataFrame: {estimated_size_mb:.2f} MB")
        
        # Calcular o número de partições
        num_partitions = math.ceil(estimated_size_mb / PARTITION_SIZE_MB)
        print(f"Número de partições: {num_partitions}")
        
        partition_size = math.ceil(len(df) / num_partitions)
        for i in range(num_partitions):
            start = i * partition_size
            end = min((i + 1) * partition_size, len(df))
            partition_df = df.iloc[start:end]           
            file_name = os.path.join(parquet_dir, f"{tabela}_part_{i+1}.parquet")
            partition_df.to_parquet(file_name, compression='snappy', index=False)
            print(f"Backup salvo: {file_name}")

print("\n🎉 Backup particionado do banco de dados finalizado!")

engine.dispose()