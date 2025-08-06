import subprocess

def run_script(path):
    print(f"Rodando {path}...")
    result = subprocess.run(['python', path], capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        print(f"Erro ao rodar {path}:", result.stderr)

if __name__ == "__main__":
    run_script('/opt/airflow/scripts/api.py')
    run_script('/opt/airflow/scripts/csv.py')
    run_script('/opt/airflow/scripts/database.py')
