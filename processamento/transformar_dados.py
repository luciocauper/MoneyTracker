# MoneyTracker/processamento/transformar_dados.py
import pandas as pd
import json
import sqlite3
import os

# --- Configuração de Caminhos ---
# Obtém o diretório do script atual (ex: MoneyTracker/processamento/)
current_script_dir = os.path.dirname(os.path.abspath(__file__))

# Volta um nível para chegar à raiz do projeto (ex: MoneyTracker/)
project_root = os.path.join(current_script_dir, os.pardir) # os.pardir é o mesmo que '..'

# Define os caminhos completos para as pastas de dados
raw_data_dir = os.path.join(project_root, 'dados_brutos')
processed_data_dir = os.path.join(project_root, 'dados_processados')

# Garante que a pasta de saída de dados processados exista
os.makedirs(processed_data_dir, exist_ok=True)

# --- 1. Carregar e transformar ibm_global_quote.json ---
json_file_path = os.path.join(raw_data_dir, 'ibm_global_quote.json')

if os.path.exists(json_file_path):
    with open(json_file_path, 'r', encoding='utf-8') as f:
        global_quote_data = json.load(f)

    df_global_quote = pd.DataFrame([global_quote_data])

    # Convertendo tipos de dados
    df_global_quote['change_percent'] = df_global_quote['change_percent'].str.replace('%', '').astype(float)
    numeric_cols = ['price', 'open', 'high', 'low']
    for col in numeric_cols:
        df_global_quote[col] = pd.to_numeric(df_global_quote[col])
    df_global_quote['volume'] = pd.to_numeric(df_global_quote['volume'], errors='coerce').astype(pd.Int64Dtype())

    print("--- Dados Transformados de ibm_global_quote.json ---")
    print(df_global_quote.info())
    print(df_global_quote.head())
    print("\n")
else:
    print(f"Aviso: Arquivo {json_file_path} não encontrado. Certifique-se de que o Victor o gerou rodando 'docker-compose up --build'.")
    df_global_quote = pd.DataFrame() # Cria um DataFrame vazio se o arquivo não existir


# --- 2. Carregar e transformar ibm_intraday.csv ---
csv_intraday_path = os.path.join(raw_data_dir, 'ibm_intraday.csv')

if os.path.exists(csv_intraday_path):
    df_intraday = pd.read_csv(csv_intraday_path)

    df_intraday['timestamp'] = pd.to_datetime(df_intraday['timestamp'])

    numeric_cols_intraday = ['open', 'high', 'low', 'close']
    for col in numeric_cols_intraday:
        df_intraday[col] = pd.to_numeric(df_intraday[col])
    df_intraday['volume'] = pd.to_numeric(df_intraday['volume'], errors='coerce').astype(pd.Int64Dtype())

    print("--- Dados Transformados de ibm_intraday.csv ---")
    print(df_intraday.info())
    print(df_intraday.head())
    print("\n")
else:
    print(f"Aviso: Arquivo {csv_intraday_path} não encontrado. Certifique-se de que o Victor o gerou rodando 'docker-compose up --build'.")
    df_intraday = pd.DataFrame() # Cria um DataFrame vazio se o arquivo não existir


# --- 3. Carregar e transformar ibm_dados_raw.db (SQLite) ---
db_path = os.path.join(raw_data_dir, 'ibm_dados_raw.db')

if os.path.exists(db_path):
    try:
        conn = sqlite3.connect(db_path)
        df_daily = pd.read_sql_query("SELECT * FROM diario_ibm_extra", conn)
        conn.close()

        df_daily['timestamp'] = pd.to_datetime(df_daily['timestamp'])
        numeric_cols_daily = ['open', 'high', 'low', 'close']
        for col in numeric_cols_daily:
            df_daily[col] = pd.to_numeric(df_daily[col])

        print("--- Dados Transformados de ibm_dados_raw.db ---")
        print(df_daily.info())
        print(df_daily.head())
        print("\n")

    except Exception as e:
        print(f"Erro ao carregar ou processar {db_path}: {e}")
        df_daily = pd.DataFrame() # Cria um DataFrame vazio em caso de erro
else:
    print(f"Aviso: Arquivo de banco de dados {db_path} não encontrado. Certifique-se de que o Victor o gerou rodando 'docker-compose up --build'.")
    df_daily = pd.DataFrame() # Cria um DataFrame vazio se o arquivo não existir

# --- Salvar os DataFrames transformados em um formato intermediário (ex: CSV) ---
if not df_global_quote.empty:
    df_global_quote.to_csv(os.path.join(processed_data_dir, 'ibm_global_quote_transformed.csv'), index=False)
    print(f"Dados globais transformados salvos em {os.path.join(processed_data_dir, 'ibm_global_quote_transformed.csv')}")

if not df_intraday.empty:
    df_intraday.to_csv(os.path.join(processed_data_dir, 'ibm_intraday_transformed.csv'), index=False)
    print(f"Dados intraday transformados salvos em {os.path.join(processed_data_dir, 'ibm_intraday_transformed.csv')}")

if not df_daily.empty:
    df_daily.to_csv(os.path.join(processed_data_dir, 'ibm_daily_transformed.csv'), index=False)
    print(f"Dados diários transformados salvos em {os.path.join(processed_data_dir, 'ibm_daily_transformed.csv')}")