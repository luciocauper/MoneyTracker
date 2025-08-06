FROM python:3.11-slim

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir \
    requests \
    pandas \
    minio \
    psycopg2-binary \
    sqlalchemy \
    boto3


CMD ["sh", "-c", "python extracao/api/api.py && python extracao/csv/csv.py && python extracao/database/database.py && python uploader/uploader.py"]
