import requests
import os
import sqlite3

url_json_ações_diarias = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol=IBM&apikey=demo'
resposta = requests.get(url_json_ações_diarias)
dados_json_daily = resposta.json()
print(dados_json_daily)
dados_diarios = dados_json_daily.get("Time Series (Daily)", {})

pasta_raw_sqlite = "data_lake/raw/sqlite"
os.makedirs(pasta_raw_sqlite, exist_ok=True)

caminho_sqlite = os.path.join(pasta_raw_sqlite, 'ibm_dados_raw.db')
conn = sqlite3.connect(caminho_sqlite)
cursor = conn.cursor()

cursor.execute('''
    CREATE TABLE IF NOT EXISTS diario_ibm_extra (
        timestamp TEXT PRIMARY KEY,
        open REAL,
        high REAL,
        low REAL,
        close REAL
    )
''')

for data, valores in dados_diarios.items():
    try:
        cursor.execute('''
            INSERT OR REPLACE INTO diario_ibm_extra 
            (timestamp, open, high, low, close)
            VALUES (?, ?, ?, ?, ?)
        ''', (
            data,
            float(valores.get("1. open")),
            float(valores.get("2. high")),
            float(valores.get("3. low")),
            float(valores.get("4. close")),
        ))
    except Exception as e:
        print(f"Erro ao inserir {data}: {e}")

conn.commit()
conn.close()