FROM python:3.11-slim

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir \
    pandas \
    minio \
    psycopg2-binary \
    sqlalchemy \
    boto3

# Altere pro script real que inicia o processamento
CMD ["python", "processamento/transformar_dados.py"]
