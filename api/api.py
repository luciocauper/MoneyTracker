import requests
import os
import json

# Requisição da API
url = 'https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=IBM&apikey=demo'
r = requests.get(url)
data = r.json()

# Acessar os dados dentro do "Global Quote"
dados_cotacao = data.get("Global Quote", {})

# Extrair apenas os campos desejados
dados_filtrados = {
    "symbol": dados_cotacao.get("01. symbol"),
    "price": dados_cotacao.get("05. price"),
    "change_percent": dados_cotacao.get("10. change percent"),
    "open": dados_cotacao.get("02. open"),
    "high": dados_cotacao.get("03. high"),
    "low": dados_cotacao.get("04. low"),
    "volume": dados_cotacao.get("06. volume")
}

# Criar pasta se não existir
pasta_raw_api = "data_lake/raw/api"
os.makedirs(pasta_raw_api, exist_ok=True)

# Caminho do arquivo onde o JSON será salvo
arquivo_json = os.path.join(pasta_raw_api, "ibm_global_quote.json")

# Salvar os dados filtrados no arquivo
with open(arquivo_json, 'w', encoding='utf-8') as f:
    json.dump(dados_filtrados, f, ensure_ascii=False, indent=4)

# Mostrar no terminal os dados
print("-> Dados filtrados:")
for chave, valor in dados_filtrados.items():
    print(f"{chave}: {valor}")

print(f"\nArquivo salvo em: {arquivo_json}")
