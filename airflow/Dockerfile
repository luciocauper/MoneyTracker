FROM apache/airflow:2.6.1

COPY requirements.txt .

USER airflow

RUN pip install --no-cache-dir -r requirements.txt

# Faz a inicialização do banco antes de rodar o scheduler e webserver
CMD ["bash", "-c", "airflow db init && airflow scheduler & airflow webserver"]
