
import streamlit as st
import pandas as pd
import sqlite3
from pathlib import Path
from datetime import datetime

DB_PATH = Path(__file__).parent / "bicocont.db"

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
    cur.execute("""
        INSERT INTO counts (timestamp, code, name, deposit, sap, physical, diff, user)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (ts, code, name, deposit, int(sap), int(physical), int(diff), user))
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

st.set_page_config(page_title="BicoCont - Contagem", layout="wide")

st.markdown("# BicoCont - Contagem de Materiais")
st.markdown("**Fluxo:** digite o código → escolha depósito (se houver) → confira saldo SAP → informe contagem física → salve.")

materials = load_materials()

col1, col2 = st.columns([2,1])
with col1:
    st.subheader("Registrar contagem")
    code_input = st.text_input("Código do material", "")
    # try to find by code or name
    matches = pd.DataFrame()
    if code_input:
        matches = materials[materials['code'].str.contains(code_input, case=False, na=False)]
        if matches.empty:
            # try name search
            matches = materials[materials['name'].str.contains(code_input, case=False, na=False)]
    else:
        matches = materials.copy()

    if not matches.empty:
        # group by code to find deposits available
        grouped = matches.groupby('code')
        selected_code = None
        selected_name = None
        selected_deposit = None
        selected_sap = 0

        # if exact code match exists, prefer it
        exact = matches[matches['code'].str.lower() == code_input.lower()]
        if not exact.empty:
            selected_code = exact.iloc[0]['code']
            selected_name = exact.iloc[0]['name']

        # show a selectbox for code if multiple
        codes = sorted(matches['code'].unique())
        selected_code = st.selectbox("Código (selecione)", codes, index=0 if codes else None)
        # show deposits for that code
        dep_options = matches[matches['code'] == selected_code][['deposit', 'sap', 'name']].drop_duplicates().reset_index(drop=True)
        dep_options['deposit'] = dep_options['deposit'].fillna('').astype(str)
        dep_map = dep_options.to_dict(orient='records')
        if len(dep_map) == 0:
            st.info("Nenhum depósito cadastrado para este código.")
        else:
            # build label for deposits
            labels = [f\"{d['deposit']} (SAP: {d['sap']})\" for d in dep_map]
            sel_idx = st.selectbox("Depósito (selecione)", options=list(range(len(labels))), format_func=lambda i: labels[i])
            selected_deposit = dep_map[sel_idx]['deposit']
            selected_sap = int(dep_map[sel_idx]['sap'] or 0)
            selected_name = dep_map[sel_idx]['name']

        st.markdown(f\"**Nome:** {selected_name}\") if selected_name else None
        st.markdown(f\"**Saldo SAP (selecionado):** {selected_sap}\")

        physical = st.number_input("Contagem física", min_value=0, value=0, step=1)
        user = st.text_input("Usuário (opcional)", "")
        if st.button("Salvar contagem"):
            diff, ts = save_count(selected_code, selected_name, selected_deposit, selected_sap, physical, user)
            st.success(f\"Contagem salva. Diferença: {diff} (salvo em {ts})\")
    else:
        st.warning("Nenhum material encontrado com esse código / nome. Você pode importar uma base ou revisar a digitação.")

with col2:
    st.subheader("Importar / Gerenciar base de materiais")
    st.markdown("Você pode carregar um CSV/XLSX para substituir ou complementar a base de materiais.")
    uploaded = st.file_uploader(\"Enviar arquivo CSV ou XLSX (opcional)\", type=['csv','xlsx'])
    if uploaded is not None:
        try:
            if str(uploaded.name).lower().endswith('.xlsx'):
                df_new = pd.read_excel(uploaded)
            else:
                df_new = pd.read_csv(uploaded, sep=None, engine='python')
            st.write(\"Preview dos dados enviados:\", df_new.head())
            if st.button(\"Substituir base de materiais com este arquivo\"):
                # Expect columns code, name, deposit, sap (attempt to map)
                colmap = {}
                for c in df_new.columns:
                    lc = str(c).strip().lower()
                    if 'code' in lc or 'material' in lc or lc.startswith('cod'):
                        colmap[c] = 'code'
                    elif 'name' in lc or 'descr' in lc or 'texto' in lc:
                        colmap[c] = 'name'
                    elif 'deposit' in lc or 'depósito' in lc or 'dep' in lc:
                        colmap[c] = 'deposit'
                    elif 'sap' in lc or 'saldo' in lc or 'quant' in lc or 'util' in lc:
                        colmap[c] = 'sap'
                df_new = df_new.rename(columns=colmap)
                # ensure required columns
                for req in ['code','name','deposit','sap']:
                    if req not in df_new.columns:
                        df_new[req] = '' if req!='sap' else 0
                df_new['code'] = df_new['code'].astype(str).str.strip()
                df_new['name'] = df_new['name'].astype(str).str.strip()
                df_new['deposit'] = df_new['deposit'].astype(str).str.strip()
                df_new['sap'] = pd.to_numeric(df_new['sap'], errors='coerce').fillna(0).astype(int)
                # save to DB (replace materials)
                conn = get_conn()
                cur = conn.cursor()
                cur.execute(\"DELETE FROM materials\")
                conn.commit()
                for _, row in df_new.iterrows():
                    cur.execute(\"INSERT INTO materials (code, name, deposit, sap) VALUES (?, ?, ?, ?)\", (row['code'], row['name'], row['deposit'], int(row['sap'])))
                conn.commit()
                conn.close()
                st.success(\"Base de materiais atualizada com sucesso.\")
                st.experimental_rerun()
        except Exception as e:
            st.error(f\"Erro ao ler arquivo: {e}\")

st.markdown(\"---\")
st.subheader(\"Histórico de contagens\")
colf1, colf2, colf3, colf4 = st.columns([1,1,1,1])
with colf1:
    f_code = st.text_input(\"Filtrar por código\", \"\")
with colf2:
    f_deposit = st.text_input(\"Filtrar por depósito\", \"\")
with colf3:
    f_from = st.date_input(\"De\", value=None)
with colf4:
    f_to = st.date_input(\"Até\", value=None)

# Convert date inputs to datetime strings if provided
date_from = f_from.isoformat() if f_from else None
date_to = (f_to.isoformat() + ' 23:59:59') if f_to else None

hist = query_history(limit=1000, code=f_code.strip() or None, deposit=f_deposit.strip() or None, date_from=date_from, date_to=date_to)
if hist.empty:
    st.info(\"Sem registros no intervalo/filtro selecionado.\")
else:
    st.write(hist)

    csv = hist.to_csv(index=False, sep=';')
    st.download_button(\"Baixar histórico (CSV)\", data=csv, file_name='bicocont_history.csv', mime='text/csv')

