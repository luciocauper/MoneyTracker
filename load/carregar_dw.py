# MoneyTracker/load/carregar_dw.py
import pandas as pd
import psycopg2
import os
from sqlalchemy import create_engine, text # Importa create_engine e text do SQLAlchemy

# --- Configuração de Caminhos e Conexão com o PostgreSQL ---
processed_data_dir = '/app/dados_processados'

DB_HOST = os.getenv('PGHOST', 'localhost')
DB_NAME = os.getenv('PGDATABASE', 'ibm_dw')
DB_USER = os.getenv('PGUSER', 'user')
DB_PASSWORD = os.getenv('PGPASSWORD', 'password')
DB_PORT = os.getenv('PGPORT', '5432')

print(f"Tentando conectar ao PostgreSQL em: {DB_HOST}:{DB_PORT}/{DB_NAME} como {DB_USER}")

def get_db_engine():
    """Cria e retorna um SQLAlchemy engine para o banco de dados PostgreSQL."""
    try:
        # Formato da string de conexão para SQLAlchemy:
        # 'postgresql+psycopg2://user:password@host:port/database'
        engine_str = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(engine_str)
        # Tenta conectar para verificar se a conexão é bem-sucedida
        with engine.connect() as conn:
            print("SQLAlchemy Engine conectado ao PostgreSQL com sucesso!")
        return engine
    except Exception as e:
        print(f"Erro ao criar SQLAlchemy Engine ou conectar ao PostgreSQL: {e}")
        raise

def create_tables(engine): # Agora a função recebe o engine do SQLAlchemy
    """Cria as tabelas no data warehouse se não existirem."""
    with engine.connect() as conn: # Usa a conexão do engine
        # Tabela para dados de cotação global (ibm_global_quote)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS ibm_global_quote (
                symbol VARCHAR(10) PRIMARY KEY,
                price REAL,
                change_percent REAL,
                open REAL,
                high REAL,
                low REAL,
                volume BIGINT
            );
        """)) # <<< Adicionado 'text()' aqui
        print("Tabela 'ibm_global_quote' verificada/criada.")

        # Tabela para dados intraday (ibm_intraday)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS ibm_intraday (
                timestamp TIMESTAMP PRIMARY KEY,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume BIGINT
            );
        """)) # <<< Adicionado 'text()' aqui
        print("Tabela 'ibm_intraday' verificada/criada.")

        # Tabela para dados diários (ibm_daily)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS ibm_daily (
                timestamp DATE PRIMARY KEY, -- Usamos DATE pois são dados diários
                open REAL,
                high REAL,
                low REAL,
                close REAL
            );
        """)) # <<< Adicionado 'text()' aqui
        print("Tabela 'ibm_daily' verificada/criada.")
        conn.commit() # Commita as criações de tabela

def load_data_to_db(df, table_name, engine): # Agora a função recebe o engine
    """Carrega um DataFrame para uma tabela específica no banco de dados."""
    if df.empty:
        print(f"DataFrame para '{table_name}' está vazio, pulando carregamento.")
        return

    try:
        # Pandas to_sql agora usa o engine do SQLAlchemy
        if table_name == 'ibm_global_quote':
            df.to_sql(table_name, engine, if_exists='replace', index=False)
        else:
            # Ajusta tipos para PostgreSQL se necessário (e.g., Int64 para BigInt)
            for col in df.columns:
                if str(df[col].dtype) == 'Int64': # Pandas nullable integer type
                    df[col] = df[col].astype(int) # Convert to standard int for psycopg2

            df.to_sql(table_name, engine, if_exists='append', index=False)
        print(f"Dados carregados para a tabela '{table_name}' com sucesso.")
    except Exception as e:
        print(f"Erro ao carregar dados para a tabela '{table_name}': {e}")
        raise # Propaga o erro para o bloco try/except principal

if __name__ == "__main__":
    db_engine = None
    try:
        db_engine = get_db_engine() # Obtém o engine do DB

        create_tables(db_engine) # Passa o engine para a criação de tabelas

        # Carregar ibm_global_quote
        global_quote_path = os.path.join(processed_data_dir, 'ibm_global_quote_transformed.csv')
        if os.path.exists(global_quote_path):
            df_global_quote = pd.read_csv(global_quote_path)
            load_data_to_db(df_global_quote, 'ibm_global_quote', db_engine) # Passa o engine
        else:
            print(f"Arquivo '{global_quote_path}' não encontrado para carregamento.")

        # Carregar ibm_intraday
        intraday_path = os.path.join(processed_data_dir, 'ibm_intraday_transformed.csv')
        if os.path.exists(intraday_path):
            df_intraday = pd.read_csv(intraday_path, parse_dates=['timestamp'])
            load_data_to_db(df_intraday, 'ibm_intraday', db_engine) # Passa o engine
        else:
            print(f"Arquivo '{intraday_path}' não encontrado para carregamento.")

        # Carregar ibm_daily
        daily_path = os.path.join(processed_data_dir, 'ibm_daily_transformed.csv')
        if os.path.exists(daily_path):
            df_daily = pd.read_csv(daily_path, parse_dates=['timestamp'])
            load_data_to_db(df_daily, 'ibm_daily', db_engine) # Passa o engine
        else:
            print(f"Arquivo '{daily_path}' não encontrado para carregamento.")

        print("Processo de carregamento de dados finalizado com sucesso!")

    except Exception as e:
        print(f"Ocorreu um erro geral no processo de carregamento: {e}")
    finally:
        # Com SQLAlchemy Engine, não precisamos fechar cursores ou conexões manualmente aqui
        # O engine gerencia isso por baixo dos panos.
        if db_engine:
            db_engine.dispose() # Limpa as conexões do pool do engine
            print("SQLAlchemy Engine disposeado.")