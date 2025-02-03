import streamlit as st
import pandas as pd
from pymongo import MongoClient, UpdateMany
import urllib.parse
from datetime import datetime
from io import BytesIO

# Configuração da página Streamlit
st.set_page_config(page_title="Processador de Dados", layout="wide")
st.title("Processador de Dados - Remoção de Duplicatas")

@st.cache_resource
def get_database():
    try:
        username = st.secrets["MONGO_USERNAME"]
        password = st.secrets["MONGO_PASSWORD"]
        cluster = st.secrets["MONGO_CLUSTER"]
        nome_bd = st.secrets["MONGO_DB"]
    except KeyError:
        st.error("Configurações de conexão não encontradas")
        return None
    try:
        uri_mongo = f"mongodb+srv://{urllib.parse.quote_plus(username)}:{urllib.parse.quote_plus(password)}@{cluster}/{nome_bd}?retryWrites=true&w=majority"
        client = MongoClient(uri_mongo, 
                           serverSelectionTimeoutMS=5000,
                           maxPoolSize=50)
        db = client[nome_bd]
        client.admin.command('ping')
        
        # Criar índices
        db.xml.create_index("tags")
        db.category.create_index("tags", unique=True)
        
        return db
    except Exception as e:
        st.error(f"Erro ao conectar ao MongoDB: {e}")
        return None

def save_to_category_collection(df_category, batch_size=1000):
    try:
        db = get_database()
        if db is None:
            return False
        
        category_collection = db['category']
        total_ops = 0
        progress_bar = st.progress(0)
        
        # Processar em lotes
        records = df_category.to_dict('records')
        total_records = len(records)
        
        for i in range(0, total_records, batch_size):
            batch = records[i:i + batch_size]
            operations = [
                UpdateMany(
                    {'tags': record['tags']},
                    {'$set': record},
                    upsert=True
                ) for record in batch
            ]
            
            # Executar lote
            category_collection.bulk_write(operations, ordered=False)
            
            # Atualizar progresso
            progress = (i + len(batch)) / total_records
            progress_bar.progress(progress)
            total_ops += len(batch)
        
        progress_bar.progress(1.0)
        st.success(f"Dados salvos com sucesso na collection 'category'! Total: {total_ops} registros")
        return True
        
    except Exception as e:
        st.error(f"Erro ao salvar na collection category: {e}")
        return False

def process_and_save_excel():
    db = get_database()
    if db is None:
        return None
   
    try:
        # Configurar campos a serem retornados
        selected_columns = ["tags", "grupo", "subgrupo", "url_imagens"]
        projection = {col: 1 for col in selected_columns}
        projection["_id"] = 0
        
        with st.spinner("Carregando dados..."):
            # Carregar todos os documentos de uma vez
            data = list(db['xml'].find({}, projection))
            
            if not data:
                st.warning("Nenhum dado encontrado na coleção")
                return None
            
            # Converter para DataFrame
            df = pd.DataFrame(data)
        
        # Mostrar quantidade inicial de registros
        st.info(f"Total de registros antes do processamento: {len(df)}")
       
        # Processar duplicatas
        with st.spinner("Processando duplicatas..."):
            duplicates = df[df.duplicated(subset=['tags'], keep=False)]
            num_duplicates = len(duplicates)
            df_tags = df.drop_duplicates(subset=['tags'], keep='first')
        
        # Salvar na collection category
        with st.spinner("Salvando no MongoDB..."):
            save_success = save_to_category_collection(df_tags)
            if not save_success:
                st.warning("Não foi possível salvar os dados na collection category")
       
        # Gerar Excel em memória
        with st.spinner("Gerando arquivo Excel..."):
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            excel_filename = f"dados_unicos_{timestamp}.xlsx"
            
            # Criar buffer em memória para o Excel
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df_tags.to_excel(writer, index=False)
            excel_data = output.getvalue()
       
        # Estatísticas
        st.success("Processamento concluído com sucesso!")
        st.write("Estatísticas do processamento:")
        st.write(f"- Registros originais: {len(df)}")
        st.write(f"- Duplicatas encontradas: {num_duplicates}")
        st.write(f"- Registros únicos: {len(df_tags)}")
       
        # Download
        st.download_button(
            label="Download Excel com dados únicos",
            data=excel_data,
            file_name=excel_filename,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
       
        # Prévia dos dados
        st.subheader("Prévia dos dados processados")
        st.dataframe(df_tags.head(10))
       
        if num_duplicates > 0:
            st.subheader("Exemplo de registros que eram duplicados")
            st.dataframe(duplicates.head(5))
       
        return df_tags
       
    except Exception as e:
        st.error(f"Erro ao processar dados: {e}")
        return None

# Interface do usuário
if st.button("Processar e Gerar Excel", type="primary"):
    with st.spinner("Processando dados..."):
        df_result = process_and_save_excel()