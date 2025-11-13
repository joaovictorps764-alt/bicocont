import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime
from pathlib import Path

# Caminho do banco de dados
DB_PATH = Path(__file__).parent / "bicocont.db"

# ==========================
# Funções principais
# ==========================
def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

@st.cache_data
def load_materials():
    conn = get_conn()
    df = pd.read_sql_query("SELECT * FROM materials", conn)
    conn.close()
    return df

def save_count(code, name, deposit, sap, physical, user=""):
    diff = int(physical) - int(sap)
    ts = datetime.now().isoformat(sep=' ', timespec='seconds')
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO counts (timestamp, code, name, deposit, sap, physical, diff, user) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (ts, code, name, deposit, int(sap), int(physical), int(diff), user)
    )
    conn.commit()
    conn.close()
    return diff, ts

def query_history(limit=200, code=None, deposit=None, date_from=None, date_to=None):
    conn = get_conn()
    sql = "SELECT * FROM counts WHERE 1=1"
    params = []
    if code:
        sql += " AND code = ?"
        params.append(code)
    if deposit:
        sql += " AND deposit = ?"
        params.append(deposit)
    if date_from:
        sql += " AND timestamp >= ?"
        params.append(date_from)
    if date_to:
        sql += " AND timestamp <= ?"
        params.append(date_to)
    sql += " ORDER BY timestamp DESC LIMIT ?"
    params.append(limit)
    df = pd.read_sql_query(sql, conn, params=params)
    conn.close()
    return df

# ==========================
# Layout e Configuração
# ==========================
st.set_page_config(page_title="BicoCont", layout="wide", page_icon="📦")

st.markdown("""
    <style>
    body {
        background-color: #f5f7fa;
    }
    .stButton>button {
        background-color: #2563eb;
        color: white;
        border-radius: 6px;
        border: none;
        padding: 0.5rem 1rem;
    }
    .stButton>button:hover {
        background-color: #1d4ed8;
        color: white;
    }
    </style>
""", unsafe_allow_html=True)

st.title("📦 BicoCont")
st.write("Ferramenta para controle de contagem física x saldo SAP")

materials = load_materials()

col1, col2 = st.columns([2, 1])
with col1:
    st.subheader("Registrar Contagem")
    code_input = st.text_input("Código ou nome do material", "")
    matches = pd.DataFrame()

    if code_input:
        matches = materials[materials['code'].str.contains(code_input, case=False, na=False)]
        if matches.empty:
            matches = materials[materials['name'].str.contains(code_input, case=False, na=False)]
    else:
        matches = materials.copy()

    if not matches.empty:
        selected_code = st.selectbox("Código", sorted(matches['code'].unique()))
        dep_options = matches[matches['code'] == selected_code][['deposit', 'sap', 'name']].drop_duplicates().reset_index(drop=True)
        dep_map = dep_options.to_dict(orient='records')

        # 🔧 Linha corrigida (sem barras invertidas!)
        labels = [f"{d['deposit']} (SAP: {d['sap']})" for d in dep_map]

        sel_idx = st.selectbox("Depósito", options=list(range(len(labels))), format_func=lambda i: labels[i])
        selected_deposit = dep_map[sel_idx]['deposit']
        selected_name = dep_map[sel_idx]['name']
        selected_sap = dep_map[sel_idx]['sap']

        st.markdown(f"**Nome:** {selected_name}")
        st.markdown(f"**Saldo SAP atual:** {selected_sap}")

        physical = st.number_input("Contagem física", min_value=0, value=0, step=1)
        user = st.text_input("Usuário (opcional)", "")
        if st.button("Salvar contagem"):
            diff, ts = save_count(selected_code, selected_name, selected_deposit, selected_sap, physical, user)
            st.success(f"Contagem salva. Diferença: {diff} (salvo em {ts})")
    else:
        st.warning("Nenhum material encontrado. Verifique o código ou nome digitado.")

with col2:
    st.subheader("Importar / Atualizar Base de Materiais")
    uploaded = st.file_uploader("Envie arquivo CSV ou XLSX", type=["csv", "xlsx"])
    if uploaded:
        try:
            if uploaded.name.endswith(".xlsx"):
                df_new = pd.read_excel(uploaded)
            else:
                df_new = pd.read_csv(uploaded, sep=None, engine='python')
            st.write("Prévia dos dados:", df_new.head())

            if st.button("Atualizar Base"):
                colmap = {}
                for c in df_new.columns:
                    lc = c.lower().strip()
                    if 'code' in lc or 'material' in lc:
                        colmap[c] = 'code'
                    elif 'name' in lc or 'descr' in lc:
                        colmap[c] = 'name'
                    elif 'deposit' in lc or 'dep' in lc:
                        colmap[c] = 'deposit'
                    elif 'sap' in lc:
                        colmap[c] = 'sap'

                df_new = df_new.rename(columns=colmap)
                for req in ['code', 'name', 'deposit', 'sap']:
                    if req not in df_new.columns:
                        df_new[req] = 0

                conn = get_conn()
                cur = conn.cursor()
                cur.execute("DELETE FROM materials")
                conn.commit()
                for _, row in df_new.iterrows():
                    cur.execute("INSERT INTO materials (code, name, deposit, sap) VALUES (?, ?, ?, ?)",
                                (row['code'], row['name'], row['deposit'], row['sap']))
                conn.commit()
                conn.close()
                st.success("Base atualizada com sucesso.")
                st.experimental_rerun()
        except Exception as e:
            st.error(f"Erro ao processar o arquivo: {e}")

st.markdown("---")
st.subheader("Histórico de Contagens")
f_code = st.text_input("Filtrar por código", "")
f_dep = st.text_input("Filtrar por depósito", "")
hist = query_history(limit=500, code=f_code or None, deposit=f_dep or None)

if hist.empty:
    st.info("Nenhum registro encontrado.")
else:
    st.dataframe(hist)
    csv = hist.to_csv(index=False, sep=";")
    st.download_button("Baixar histórico CSV", data=csv, file_name="historico_bicocont.csv", mime="text/csv")
