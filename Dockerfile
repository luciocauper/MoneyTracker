# Imagem base
FROM python

# Diretório de trabalho
WORKDIR /app

# Copiar todos os arquivos do projeto
COPY . .

# Instalar dependências
RUN pip install requests minio

# Comando padrão (substituível via docker-compose ou docker run)
CMD ["python", "extracao/api/api.py"]
