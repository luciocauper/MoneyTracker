import os
import time
from minio import Minio
from minio.error import S3Error

# Lista de arquivos a enviar (com caminhos locais)
arquivos = [
    os.path.join("dados_brutos", "ibm_global_quote.json"),
    os.path.join("dados_brutos", "ibm_intraday.csv"),
    os.path.join("dados_brutos", "ibm_sma.csv"),
    os.path.join("dados_brutos", "ibm_dados_raw.db")
]

# Nome do bucket no MinIO
bucket = "raw"

# Configurações de conexão com MinIO
endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")

# Inicializa o cliente
client = Minio(
    endpoint,
    access_key=access_key,
    secret_key=secret_key,
    secure=False
)

# Aguarda MinIO subir (caso necessário)
time.sleep(5)

# Cria o bucket se não existir
if not client.bucket_exists(bucket):
    client.make_bucket(bucket)
    print(f"Bucket '{bucket}' criado.")
else:
    print(f"Bucket '{bucket}' já existe.")

# Envia cada arquivo
for file_path in arquivos:
    if os.path.isfile(file_path):
        object_name = os.path.relpath(file_path, "dados_brutos")  
        try:
            client.fput_object(bucket, object_name, file_path)
            print(f"Arquivo '{file_path}' enviado como '{object_name}' para bucket '{bucket}'")
        except S3Error as err:
            print(f"Erro ao enviar '{file_path}':", err)
    else:
        print(f"Arquivo não encontrado: {file_path}")
