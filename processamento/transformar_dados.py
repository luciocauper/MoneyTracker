import pandas as pd
import json
import sqlite3
import os
import boto3
from io import BytesIO

MINIO_ENDPOINT = 'http://localhost:9000'
MINIO_ACCESS_KEY = 'minioadmin'         
MINIO_SECRET_KEY = 'minioadmin'          
BUCKET_NAME = 'raw'                      

# Conexão com o MinIO (S3)
s3 = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    region_name='us-east-1'
)

# Pasta de saída dos dados transformados
current_script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.join(current_script_dir, os.pardir)
processed_data_dir = os.path.join(project_root, 'dados_processados')
os.makedirs(processed_data_dir, exist_ok=True)

# ========== 1. ibm_global_quote.json ==========
try:
    obj = s3.get_object(Bucket=BUCKET_NAME, Key='ibm_global_quote.json')
    content = obj['Body'].read()
    global_quote_data = json.loads(content)

    df_global_quote = pd.DataFrame([global_quote_data])
    df_global_quote['change_percent'] = df_global_quote['change_percent'].str.replace('%', '').astype(float)

    numeric_cols = ['price', 'open', 'high', 'low']
    for col in numeric_cols:
        df_global_quote[col] = pd.to_numeric(df_global_quote[col])
    df_global_quote['volume'] = pd.to_numeric(df_global_quote['volume'], errors='coerce').astype(pd.Int64Dtype())

    print("--- Dados Transformados de ibm_global_quote.json ---")
    print(df_global_quote.info())
    print(df_global_quote.head())
    print("\n")

    df_global_quote.to_csv(os.path.join(processed_data_dir, 'ibm_global_quote_transformed.csv'), index=False)
    print("Salvo: ibm_global_quote_transformed.csv")

except Exception as e:
    print(f"Erro ao acessar ibm_global_quote.json no MinIO: {e}")
    df_global_quote = pd.DataFrame()

# ========== 2. ibm_intraday.csv ==========
try:
    obj = s3.get_object(Bucket=BUCKET_NAME, Key='ibm_intraday.csv')
    content = obj['Body'].read()
    df_intraday = pd.read_csv(BytesIO(content))

    df_intraday['timestamp'] = pd.to_datetime(df_intraday['timestamp'])

    numeric_cols_intraday = ['open', 'high', 'low', 'close']
    for col in numeric_cols_intraday:
        df_intraday[col] = pd.to_numeric(df_intraday[col])
    df_intraday['volume'] = pd.to_numeric(df_intraday['volume'], errors='coerce').astype(pd.Int64Dtype())

    print("--- Dados Transformados de ibm_intraday.csv ---")
    print(df_intraday.info())
    print(df_intraday.head())
    print("\n")

    df_intraday.to_csv(os.path.join(processed_data_dir, 'ibm_intraday_transformed.csv'), index=False)
    print("Salvo: ibm_intraday_transformed.csv")

except Exception as e:
    print(f"Erro ao acessar ibm_intraday.csv no MinIO: {e}")
    df_intraday = pd.DataFrame()

# ========== 3. ibm_dados_raw.db ==========
try:
    obj = s3.get_object(Bucket=BUCKET_NAME, Key='ibm_dados_raw.db')
    content = obj['Body'].read()

    # Salvar o arquivo temporariamente localmente
    temp_db_path = os.path.join(project_root, 'temp_ibm_dados_raw.db')
    with open(temp_db_path, 'wb') as f:
        f.write(content)

    conn = sqlite3.connect(temp_db_path)
    df_daily = pd.read_sql_query("SELECT * FROM diario_ibm_extra", conn)
    conn.close()
    os.remove(temp_db_path)

    df_daily['timestamp'] = pd.to_datetime(df_daily['timestamp'])
    numeric_cols_daily = ['open', 'high', 'low', 'close']
    for col in numeric_cols_daily:
        df_daily[col] = pd.to_numeric(df_daily[col])

    print("--- Dados Transformados de ibm_dados_raw.db ---")
    print(df_daily.info())
    print(df_daily.head())
    print("\n")

    df_daily.to_csv(os.path.join(processed_data_dir, 'ibm_daily_transformed.csv'), index=False)
    print("Salvo: ibm_daily_transformed.csv")

except Exception as e:
    print(f"Erro ao acessar ibm_dados_raw.db no MinIO: {e}")
    df_daily = pd.DataFrame()
