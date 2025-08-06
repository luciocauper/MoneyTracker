# Dockerfile.data_loader
FROM python:3.11-slim

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir pandas sqlalchemy psycopg2-binary

CMD ["python", "load/load_to_dw.py"]
