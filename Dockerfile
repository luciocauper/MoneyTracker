# Imagem base
FROM python

# Diretório de trabalho
WORKDIR /app

# Copiar todos os arquivos do projeto
COPY . .

# Instalar dependências
RUN pip install requests minio pandas psycopg2-binary sqlalchemy boto3


# Comando padrão (substituível via docker-compose ou docker run)
CMD ["python", "extracao/api/api.py"]
