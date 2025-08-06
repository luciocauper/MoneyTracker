FROM python:3.10-slim

# Define diretório de trabalho
WORKDIR /app

# Copia os arquivos pro container
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Expõe a porta padrão do Streamlit
EXPOSE 8501

# Comando padrão do container
CMD ["streamlit", "run", "app.py"]
