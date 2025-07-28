import os
import requests

api_key = 'demo'

# URLs para as requisições
url_intraday = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&apikey={api_key}&datatype=csv'
url_sma = f'https://www.alphavantage.co/query?function=SMA&symbol=IBM&interval=5min&time_period=10&series_type=close&apikey={api_key}&datatype=csv'

# Fazer as requisições
r_intraday = requests.get(url_intraday)
r_sma = requests.get(url_sma)

# Criar pasta se não existir
pasta_dados_brutos = "dados_brutos"
os.makedirs(pasta_dados_brutos, exist_ok=True)

# Salvar os arquivos CSV
caminho_intraday = os.path.join(pasta_dados_brutos, 'ibm_intraday.csv')
caminho_sma = os.path.join(pasta_dados_brutos, 'ibm_sma.csv')

with open(caminho_intraday, 'w', encoding='utf-8') as f:
    f.write(r_intraday.text)

with open(caminho_sma, 'w', encoding='utf-8') as f:
    f.write(r_sma.text)

print("Arquivos salvos com sucesso!")
