# Imagem base específica e leve
FROM python:3.11-slim

# Diretório de trabalho
WORKDIR /app

# Copiar todos os arquivos do projeto
COPY . .

# Instalar dependências
RUN pip install --no-cache-dir requests minio pandas psycopg2-binary sqlalchemy boto3

# Comando padrão
CMD ["python", "extracao/api/api.py"]
