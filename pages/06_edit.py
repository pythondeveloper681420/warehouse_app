import streamlit as st
import pandas as pd
from pymongo import MongoClient
from bson.objectid import ObjectId
import urllib.parse

# Configurações da página
st.set_page_config(page_title="Gestor de Materiais", layout="wide")
st.title("📦 Sistema de Gerenciamento de Materiais")

# Estilos CSS personalizados
st.markdown("""
<style>
    .dataframe {
        font-size: 0.8em !important;
    }
    .dataframe th, .dataframe td {
        padding: 4px 8px !important;
        white-space: nowrap !important;
    }
    button[kind="secondary"] {
        padding: 0.1em 0.3em;
        font-size: 0.8em;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_resource
def get_mongo_client():
    username = urllib.parse.quote_plus(st.secrets["MONGO_USERNAME"])
    password = urllib.parse.quote_plus(st.secrets["MONGO_PASSWORD"])
    return MongoClient(f"mongodb+srv://{username}:{password}@{st.secrets['MONGO_CLUSTER']}/warehouse?retryWrites=true&w=majority")

@st.cache_data
def fetch_data(collection_name, page, per_page):
    client = get_mongo_client()
    collection = client.warehouse[collection_name]
    total = collection.count_documents({})
    docs = list(collection.find().skip((page-1)*per_page).limit(per_page))
    
    # Converter para DataFrame e adicionar ações
    df = pd.json_normalize(docs)
    if '_id' in df.columns:
        df['_id'] = df['_id'].astype(str)
        df['Ações'] = df['_id'].apply(lambda x: f"edit_{x}|delete_{x}")
    
    return df, total

def display_dataframe(df):
    # Formatar colunas numéricas
    money_cols = [col for col in df.columns if 'Valor' in col]
    for col in money_cols:
        if pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].apply(lambda x: f"R$ {x:,.2f}" if pd.notnull(x) else "")
    
    # Configurar colunas
    column_config = {
        "_id": "ID",
        "Ações": st.column_config.Column(
            width="small",
            help="Ações disponíveis para o documento"
        )
    }
    
    # Exibir dataframe
    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        column_order=["_id"] + [col for col in df.columns if col not in ["_id", "Ações"]] + ["Ações"],
        column_config=column_config
    )

def handle_actions(df, collection_name):
    for _, row in df.iterrows():
        if "edit_" in row['Ações']:
            if st.button("✏️ Editar", key=f"edit_{row['_id']}"):
                st.session_state.edit_doc = row.to_dict()
        if "delete_" in row['Ações']:
            if st.button("🗑️ Excluir", key=f"delete_{row['_id']}"):
                with st.spinner("Excluindo..."):
                    client = get_mongo_client()
                    client.warehouse[collection_name].delete_one({'_id': ObjectId(row['_id'])})
                    st.rerun()

def pagination_controls(total_items):
    col1, col2, col3 = st.columns([2, 4, 2])
    with col1:
        page = st.number_input("Página:", 
                             min_value=1, 
                             max_value=(total_items // st.session_state.per_page) + 1,
                             value=st.session_state.page)
    with col3:
        per_page = st.selectbox("Itens/página:", [10, 20, 50], index=0)
    return page, per_page

def edit_modal(columns):
    if 'edit_doc' not in st.session_state:
        return
    
    doc = st.session_state.edit_doc
    with st.form("edit_form"):
        st.subheader(f"Editando: {doc['_id'][:10]}...")
        new_values = {}
        
        cols = st.columns(2)
        for i, (key, value) in enumerate(doc.items()):
            if key in ['_id', 'Ações']: continue
            with cols[i%2]:
                new_values[key] = st.text_input(key, value=str(value))
        
        if st.form_submit_button("💾 Salvar"):
            client = get_mongo_client()
            client.warehouse[st.session_state.collection].update_one(
                {'_id': ObjectId(doc['_id'])},
                {'$set': new_values}
            )
            del st.session_state.edit_doc
            st.rerun()

def main():
    # Configuração inicial
    if 'page' not in st.session_state:
        st.session_state.page = 1
    if 'per_page' not in st.session_state:
        st.session_state.per_page = 10
    if 'collection' not in st.session_state:
        st.session_state.collection = 'xml'
    
    # Seletor de coleção
    st.session_state.collection = st.selectbox("Coleção:", ['xml', 'po', 'nfspdf'])
    
    # Carregar dados
    df, total = fetch_data(st.session_state.collection, st.session_state.page, st.session_state.per_page)
    
    # Controles de paginação
    new_page, new_per_page = pagination_controls(total)
    if new_page != st.session_state.page or new_per_page != st.session_state.per_page:
        st.session_state.page = new_page
        st.session_state.per_page = new_per_page
        st.rerun()
    
    # Exibir dados
    if not df.empty:
        display_dataframe(df)
        handle_actions(df, st.session_state.collection)
    else:
        st.warning("Nenhum documento encontrado.")
    
    # Modal de edição
    edit_modal(df.columns)

if __name__ == "__main__":
    main()