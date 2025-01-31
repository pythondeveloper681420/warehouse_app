import streamlit as st
import pandas as pd
from pymongo import MongoClient
from bson.objectid import ObjectId
import urllib.parse

# Configurações da página
st.set_page_config(page_title="Gestor de Materiais", layout="wide")
st.title(" Sistema de Gerenciamento de Materiais")

# Estilos CSS personalizados


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
    
    df = pd.json_normalize(docs)
    if '_id' in df.columns:
        df['_id'] = df['_id'].astype(str)
    return df, total

def display_and_edit_dataframe(df, collection_name):
    from st_aggrid import AgGrid, GridOptionsBuilder

    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_columns(list(df.columns), editable=True)
    gb.configure_selection('single', use_checkbox=False)

    # Define a Javascript function as a string
    money_format_js = """
        function(params) {
            if (typeof params.value === 'number') {
                return 'R$ ' + params.value.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2});
            }
            return params.value;
        }
    """

    for col in df.columns:
        if 'Valor' in col and pd.api.types.is_numeric_dtype(df[col]):
            # Use js_functions to pass the Javascript code
            gb.configure_column(col, valueFormatter=money_format_js, type=["number"]) # Adicione o tipo number

    gridOptions = gb.build()

    grid_response = AgGrid(
        df,
        gridOptions=gridOptions,
        allow_unsafe_javascrip=True,
        theme='streamlit',
        use_container_width=True,
        update_mode='MODEL_CHANGED',
    )


    # Salva as alterações no banco de dados
    if grid_response['data'] is not None: # Verifica se houve alterações
        updated_df = grid_response['data']
        client = get_mongo_client()
        collection = client.warehouse[collection_name]
        for index, row in updated_df.iterrows():
            _id = row['_id']
            updated_data = row.drop('_id').to_dict() # Remove _id para evitar erro na atualização
            collection.update_one({'_id': ObjectId(_id)}, {'$set': updated_data})
        st.success("Dados atualizados com sucesso!")
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

def main():
    if 'page' not in st.session_state:
        st.session_state.page = 1
    if 'per_page' not in st.session_state:
        st.session_state.per_page = 10
    if 'collection' not in st.session_state:
        st.session_state.collection = 'xml'
    
    st.session_state.collection = st.selectbox("Coleção:", ['xml', 'po', 'nfspdf'])
    
    df, total = fetch_data(st.session_state.collection, st.session_state.page, st.session_state.per_page)
    
    new_page, new_per_page = pagination_controls(total)
    if new_page != st.session_state.page or new_per_page != st.session_state.per_page:
        st.session_state.page = new_page
        st.session_state.per_page = new_per_page
        st.rerun()
    
    if not df.empty:
        display_and_edit_dataframe(df, st.session_state.collection)
    else:
        st.warning("Nenhum documento encontrado.")

if __name__ == "__main__":
    main()