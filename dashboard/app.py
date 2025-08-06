import streamlit as st
import pandas as pd
import altair as alt
from pathlib import Path
from streamlit_autorefresh import st_autorefresh

# Atualiza a cada 10 segundos
st_autorefresh(interval=10000, key="dashboard_refresh")

st.set_page_config(page_title="Dashboard IBM", layout="wide")
st.title("📊 Dashboard IBM - Visualização de Dados")

def carregar_dados_csv(arquivo):
    """
    Lê um CSV e converte a coluna 'timestamp' pra datetime se ela existir.
    """
    if not Path(arquivo).exists():
        return None
    try:
        df = pd.read_csv(arquivo)
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], infer_datetime_format=True)
        return df
    except Exception as e:
        st.error(f"Erro ao carregar {arquivo}: {e}")
        return None

# --- Carregamento dos dados ---
df_daily = carregar_dados_csv("/app/dados_processados/ibm_daily_transformed.csv")
df_intraday = carregar_dados_csv("/app/dados_processados/ibm_intraday_transformed.csv")
df_quote = carregar_dados_csv("/app/dados_processados/ibm_global_quote_transformed.csv")

# --- DAILY ---
if df_daily is not None and not df_daily.empty:
    st.subheader("📅 Dados Diários")
    tipo_visual = st.radio("Tipo de gráfico (Daily):", ["Fechamento (linha)", "Candlestick"])

    if tipo_visual == "Fechamento (linha)":
        chart = alt.Chart(df_daily).mark_line(point=True).encode(
            x=alt.X('timestamp:T', title='Data'),
            y=alt.Y('close:Q', title='Preço de Fechamento'),
            tooltip=['timestamp:T', 'open', 'high', 'low', 'close']
        ).interactive()
        st.altair_chart(chart, use_container_width=True)

    elif tipo_visual == "Candlestick":
        base = alt.Chart(df_daily).encode(x='timestamp:T')
        regra_alta = base.mark_rule(color="green").encode(y='low:Q', y2='high:Q')
        corpo_alta = base.mark_bar(color="green").encode(y='open:Q', y2='close:Q').transform_filter('datum.close > datum.open')
        corpo_baixa = base.mark_bar(color="red").encode(y='open:Q', y2='close:Q').transform_filter('datum.close <= datum.open')
        st.altair_chart((regra_alta + corpo_alta + corpo_baixa).interactive(), use_container_width=True)
else:
    st.warning("⚠️ Arquivo ibm_daily_transformed.csv não encontrado ou está vazio.")

# --- INTRADAY ---
if df_intraday is not None and not df_intraday.empty:
    st.subheader("⏱️ Dados Intraday")
    chart_intraday = alt.Chart(df_intraday).mark_line().encode(
        x=alt.X('timestamp:T', title='Horário'),
        y=alt.Y('close:Q', title='Preço de Fechamento'),
        tooltip=['timestamp:T', 'open', 'high', 'low', 'close']
    ).interactive()
    st.altair_chart(chart_intraday, use_container_width=True)
else:
    st.warning("⚠️ Arquivo ibm_intraday_transformed.csv não encontrado ou está vazio.")

# --- QUOTE ---
if df_quote is not None and not df_quote.empty:
    st.subheader("💬 Cotação Atual (Quote)")

    st.dataframe(df_quote.tail(5), use_container_width=True)

    colunas_disponiveis = [col for col in ['price', 'open', 'high', 'low', 'volume', 'change_percent'] if col in df_quote.columns]
    
    if colunas_disponiveis:
        col_selecionada = st.selectbox("📈 Escolha um valor para o gráfico (Quote):", colunas_disponiveis)

        if 'timestamp' not in df_quote.columns:
            df_quote['timestamp'] = pd.date_range(start='2025-01-01', periods=len(df_quote), freq='H')

        chart_quote = alt.Chart(df_quote).mark_line(point=True).encode(
            x=alt.X('timestamp:T', title='Horário'),
            y=alt.Y(f'{col_selecionada}:Q', title=col_selecionada.capitalize()),
            tooltip=['timestamp:T', col_selecionada]
        ).interactive()

        st.altair_chart(chart_quote, use_container_width=True)
    else:
        st.info("O arquivo de cotação não contém colunas numéricas disponíveis para gráfico.")
else:
    st.warning("⚠️ Arquivo ibm_global_quote_transformed.csv não encontrado ou está vazio.")
