from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2025, 8, 1),
    'catchup': False
}

with DAG(
    'pipeline_bolsa_valores',
    schedule_interval='*/5 * * * *',  # roda de 5 em 5 min
    default_args=default_args,
    description='Pipeline ETL para ações IBM com MinIO e PostgreSQL',
    tags=['etl', 'ibm', 'bolsa']
) as dag:

    extrair = BashOperator(
        task_id='extrair_dados',
        bash_command='python /opt/airflow/scripts/extracao.py'
    )

    upload = BashOperator(
        task_id='upload_minio',
        bash_command='python /opt/airflow/scripts/uploader.py'
    )

    transformar = BashOperator(
        task_id='transformar_dados',
        bash_command='python /opt/airflow/scripts/transformar_dados.py'
    )

    carregar = BashOperator(
        task_id='carregar_dw',
        bash_command='python /opt/airflow/scripts/carregar_dw.py'
    )

    extrair >> upload >> transformar >> carregar
