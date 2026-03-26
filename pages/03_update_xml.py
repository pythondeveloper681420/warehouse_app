import streamlit as st
import pandas as pd
from pymongo import MongoClient, errors
import urllib.parse
import numpy as np
from datetime import datetime, time, timezone
import time as time_module
from contextlib import contextmanager
import itertools
import dns.resolver
dns.resolver.default_resolver = dns.resolver.Resolver(configure=False)
dns.resolver.default_resolver.nameservers = ['8.8.8.8', '8.8.4.4'] 

# Configuração da página
st.set_page_config(
    page_title="Processador MongoDB Pro",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
)

hide_streamlit_style = """
<style>
.main { overflow: auto; }
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
.stApp [data-testid="stToolbar"]{ display:none; }
.reportview-container { margin-top: -2em; }
.stDeployButton {display:none;}
#stDecoration {display:none;}    
</style>
"""
st.markdown(hide_streamlit_style, unsafe_allow_html=True) 

st.markdown("""
    <style>
        .stApp { margin: 0 auto; padding: 1rem; }
        .main > div {
            padding: 2rem;
            border-radius: 10px;
            background: white;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .st-emotion-cache-1y4p8pa { padding: 2rem; border-radius: 10px; }
        .st-emotion-cache-1v0mbdj { margin-top: 1rem; }
    </style>
""", unsafe_allow_html=True)

# Configurações do MongoDB
USERNAME = urllib.parse.quote_plus(st.secrets["MONGO_USERNAME"])
PASSWORD = urllib.parse.quote_plus(st.secrets["MONGO_PASSWORD"])
CLUSTER = st.secrets["MONGO_CLUSTER"]
DB_NAME = st.secrets["MONGO_DB"]
MAX_RETRIES = 5
RETRY_DELAY = 3
CONNECTION_TIMEOUT = 30000
SOCKET_TIMEOUT = 45000

@contextmanager
def mongodb_connection():
    """Context manager para conexão MongoDB com retry e timeouts"""
    client = None
    for attempt in range(MAX_RETRIES):
        try:
            connection_string = (
                f"mongodb+srv://{USERNAME}:{PASSWORD}@{CLUSTER}/"
                f"?retryWrites=true&w=majority"
                f"&connectTimeoutMS={CONNECTION_TIMEOUT}"
                f"&socketTimeoutMS={SOCKET_TIMEOUT}"
                f"&serverSelectionTimeoutMS={CONNECTION_TIMEOUT}"
            )
            client = MongoClient(
                connection_string,
                connect=True,
                serverSelectionTimeoutMS=CONNECTION_TIMEOUT
            )
            client.admin.command('ping')
            db = client[DB_NAME]
            yield db
            break
        except errors.ServerSelectionTimeoutError:
            if attempt == MAX_RETRIES - 1:
                raise Exception("Erro de conexão: Não foi possível conectar ao MongoDB após várias tentativas")
            time_module.sleep(RETRY_DELAY)
        except errors.OperationFailure as e:
            raise Exception(f"Erro de autenticação: {str(e)}")
        except Exception as e:
            raise Exception(f"Erro inesperado: {str(e)}")
        finally:
            if client:
                client.close()

def handle_date(value):
    """Função para tratar datas e horários."""
    if pd.isna(value) or pd.isnull(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.strftime('%Y-%m-%d %H:%M:%S')
    if isinstance(value, time):
        return value.strftime('%H:%M:%S')
    return value

def clean_dataframe(df):
    """Limpa e prepara o DataFrame para inserção no MongoDB."""
    df_clean = df.copy()
    
    columns_to_drop = []
    if '_id' in df_clean.columns:
        columns_to_drop.append('_id')
    if 'creation_date' in df_clean.columns:
        columns_to_drop.append('creation_date')
    
    if columns_to_drop:
        df_clean = df_clean.drop(columns=columns_to_drop)
    
    df_clean['creation_date'] = datetime.now(timezone.utc)
    df_clean['observation'] = ""
    
    for column in df_clean.columns:
        if pd.api.types.is_datetime64_any_dtype(df_clean[column]):
            df_clean[column] = df_clean[column].apply(handle_date)
        else:
            df_clean[column] = df_clean[column].apply(lambda x: 
                None if pd.isna(x) 
                else int(x) if isinstance(x, (np.integer, int))
                else float(x) if isinstance(x, (np.floating, float))
                else bool(x) if isinstance(x, (np.bool_, bool))
                else x.strftime('%H:%M:%S') if isinstance(x, time)
                else str(x) if isinstance(x, (np.datetime64, datetime))
                else x
            )
    
    return df_clean

def upload_to_mongodb(df, collection_name):
    """Upload do DataFrame para MongoDB com melhor gestão de erros"""
    try:
        with mongodb_connection() as db:
            df_clean = clean_dataframe(df)
            records = df_clean.to_dict('records')
            collection = db[collection_name]
            
            batch_size = 500
            inserted_count = 0
            
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                retry_count = 0
                while retry_count < MAX_RETRIES:
                    try:
                        result = collection.insert_many(batch, ordered=False)
                        inserted_count += len(result.inserted_ids)
                        break
                    except errors.BulkWriteError as bwe:
                        if hasattr(bwe, 'details'):
                            inserted_count += bwe.details.get('nInserted', 0)
                        raise
                    except (errors.AutoReconnect, errors.NetworkTimeout):
                        retry_count += 1
                        if retry_count == MAX_RETRIES:
                            raise
                        time_module.sleep(RETRY_DELAY)
            
            return True, inserted_count
            
    except errors.BulkWriteError as bwe:
        return False, f"Erro no upload em lote (alguns documentos podem ter sido inseridos): {str(bwe)}"
    except errors.ServerSelectionTimeoutError:
        return False, "Timeout na conexão com MongoDB. Verifique sua conexão e tente novamente."
    except errors.OperationFailure as e:
        return False, f"Erro de operação MongoDB: {str(e)}"
    except Exception as e:
        return False, f"Erro inesperado: {str(e)}"


def fast_remove_duplicates(collection_name, field_name, keep="oldest"):
    """
    Remove duplicadas usando agregação.
    keep='oldest'  → sort ASC  → $first = creation_date mais antiga
    keep='newest'  → sort DESC → $first = creation_date mais recente
    """
    sort_order = 1 if keep == "oldest" else -1

    try:
        with mongodb_connection() as db:
            collection = db[collection_name]
            collection.create_index([(field_name, 1), ('creation_date', sort_order)])
            
            pipeline = [
                {"$sort": {"creation_date": sort_order}},
                {
                    "$group": {
                        "_id": f"${field_name}",
                        "original_id": {"$first": "$_id"},
                        "count": {"$sum": 1}
                    }
                },
                {"$match": {"count": {"$gt": 1}}}
            ]
            
            duplicates_cursor = collection.aggregate(
                pipeline,
                allowDiskUse=True,
                maxTimeMS=30000
            )
            
            total_deleted = 0
            batch_size = 100
            
            while True:
                try:
                    batch_duplicates = list(itertools.islice(duplicates_cursor, batch_size))
                    if not batch_duplicates:
                        break
                    
                    original_ids = [doc["original_id"] for doc in batch_duplicates]
                    field_values = [doc["_id"]          for doc in batch_duplicates]
                    
                    result = collection.delete_many({
                        field_name: {"$in": field_values},
                        "_id":      {"$nin": original_ids}
                    })
                    total_deleted += result.deleted_count
                    
                except errors.ExecutionTimeout:
                    continue
                except errors.CursorNotFound:
                    duplicates_cursor = collection.aggregate(
                        pipeline, allowDiskUse=True, maxTimeMS=30000
                    )
                    continue
            
            return True, total_deleted, collection.count_documents({})
            
    except Exception as e:
        return False, str(e), 0


def batch_remove_duplicates(collection_name, field_name, keep="oldest", batch_size=100):
    """
    Remove duplicadas em lotes com menor uso de memória.
    keep='oldest'  → sort ASC  → mantém creation_date mais antiga
    keep='newest'  → sort DESC → mantém creation_date mais recente
    """
    sort_order = 1 if keep == "oldest" else -1

    try:
        with mongodb_connection() as db:
            collection = db[collection_name]
            collection.create_index([(field_name, 1), ('creation_date', sort_order)])
            
            duplicates_removed = 0
            keep_ids = {}  # field_value → _id do documento a manter

            cursor = collection.find(
                {},
                {field_name: 1, '_id': 1, 'creation_date': 1}
            ).sort('creation_date', sort_order).batch_size(batch_size)
            
            while True:
                try:
                    batch = list(itertools.islice(cursor, batch_size))
                    if not batch:
                        break
                    
                    batch_ids_to_delete = []
                    for doc in batch:
                        value = doc.get(field_name)
                        if value is not None:
                            if value in keep_ids:
                                batch_ids_to_delete.append(doc['_id'])
                            else:
                                keep_ids[value] = doc['_id']
                    
                    if batch_ids_to_delete:
                        result = collection.delete_many({'_id': {'$in': batch_ids_to_delete}})
                        duplicates_removed += result.deleted_count
                    
                except errors.CursorNotFound:
                    cursor = collection.find(
                        {},
                        {field_name: 1, '_id': 1, 'creation_date': 1}
                    ).sort('creation_date', sort_order).batch_size(batch_size)
                    continue
                except errors.ExecutionTimeout:
                    continue
            
            return True, duplicates_removed, collection.count_documents({})
                
    except Exception as e:
        return False, str(e), 0


def main():
    st.header("🚀 Processador MongoDB Pro")
    st.markdown("Faça upload de seus dados Excel para o MongoDB com facilidade")
    
    with st.container():
        tab1, tab2, tab3 = st.tabs(["📤 Upload de Dados", "🧹 Limpeza de Dados", "❓Como Utilizar"])
        
        # ── TAB 1: UPLOAD ──────────────────────────────────────────────────────
        with tab1:
            col1, col2 = st.columns([2, 1])
            
            with col1:
                uploaded_file = st.file_uploader(
                    "📂 Selecione o Arquivo Excel",
                    type=['xlsx', 'xls'],
                    help="Suporte para arquivos .xlsx e .xls"
                )
            with col2:
                collection_name = st.text_input(
                    "Nome da Coleção",
                    placeholder="Digite o nome da coleção",
                    help="Nome para sua coleção no MongoDB"
                ).strip()

            message_container = st.empty()

            if uploaded_file is not None:
                try:
                    df = pd.read_excel(uploaded_file)
                    
                    if not df.empty:
                        with st.expander("📊 Visualização dos Dados", expanded=False):
                            st.dataframe(df.head(), use_container_width=True)
                        
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Total de Linhas", len(df))
                        with col2:
                            st.metric("Total de Colunas", len(df.columns))
                        with col3:
                            st.metric("Tamanho do Arquivo", f"{uploaded_file.size / 1024:.1f} KB")
                        
                        with st.expander("📋 Tipos de Colunas"):
                            df_types = pd.DataFrame({
                                'Coluna': df.columns,
                                'Tipo': df.dtypes.values.astype(str)
                            })
                            st.dataframe(df_types, use_container_width=True)
                        
                        if collection_name:
                            if st.button("📤 Enviar para MongoDB", type="primary", use_container_width=True):
                                with st.spinner("Processando upload..."):
                                    success, result = upload_to_mongodb(df, collection_name)
                                    if success:
                                        message_container.success(f"""
                                            ✅ Upload Concluído com Sucesso!
                                            • Coleção: {collection_name}
                                            • Registros Inseridos: {result}
                                        """)
                                    else:
                                        message_container.error(result)
                        else:
                            st.info("👆 Por favor, insira um nome para a coleção para prosseguir", icon="ℹ️")
                    else:
                        st.warning("⚠️ O arquivo enviado está vazio!")
                        
                except Exception as e:
                    st.error(f"Erro ao processar arquivo: {str(e)}")
        
        # ── TAB 2: LIMPEZA ─────────────────────────────────────────────────────
        with tab2:
            st.subheader("🧹 Limpeza de Duplicadas")
            
            col_a, col_b = st.columns([2, 1])

            with col_a:
                clean_collection = st.text_input(
                    "Nome da Coleção para Limpeza",
                    placeholder="Digite o nome da coleção",
                    help="Nome da coleção onde as duplicadas serão removidas"
                ).strip()

            with col_b:
                # ✅ Campo de texto com valor padrão "unique"
                field_name = st.text_input(
                    "Campo para identificar duplicadas",
                    value="unique",
                    help="Nome do campo usado para detectar registros duplicados. Padrão: 'unique'"
                ).strip()

            if clean_collection and field_name:

                # ✅ Seleção: manter mais antigos ou mais recentes
                keep_option = st.radio(
                    "Qual registro manter quando houver duplicata?",
                    options=[
                        "🕰️ Mais antigo  —  menor creation_date (primeiro inserido)",
                        "🆕 Mais recente  —  maior creation_date (último inserido)"
                    ],
                    index=0,
                    help=(
                        "Mais antigo → conserva o dado original de entrada.\n"
                        "Mais recente → conserva a versão mais recente do dado."
                    )
                )
                keep = "oldest" if "antigo" in keep_option else "newest"
                keep_label = "mais antigos (menor creation_date)" if keep == "oldest" else "mais recentes (maior creation_date)"

                cleaning_method = st.radio(
                    "Método de Limpeza",
                    ["Rápido (Memória)", "Em Lotes (Menor uso de memória)"],
                    help="Rápido: ideal para coleções menores. Em Lotes: recomendado para grandes volumes."
                )

                st.info(
                    f"⚠️ Serão mantidos os registros **{keep_label}**, "
                    f"identificando duplicatas pelo campo **`{field_name}`**."
                )

                if st.button("🧹 Remover Duplicadas", type="primary", use_container_width=True):
                    with st.spinner("Removendo duplicadas..."):
                        if cleaning_method == "Rápido (Memória)":
                            success, removed_count, remaining_count = fast_remove_duplicates(
                                clean_collection, field_name, keep=keep
                            )
                        else:
                            success, removed_count, remaining_count = batch_remove_duplicates(
                                clean_collection, field_name, keep=keep
                            )
                            
                        if success:
                            st.success(f"""
                                ✅ Limpeza Concluída com Sucesso!
                                • Campo usado: {field_name}
                                • Registros mantidos: {keep_label}
                                • Documentos removidos: {removed_count}
                                • Documentos restantes: {remaining_count}
                            """)
                        else:
                            st.error(f"Erro ao remover duplicadas: {removed_count}")

            elif clean_collection and not field_name:
                st.warning("⚠️ Por favor, informe o nome do campo para identificar duplicadas.")
            else:
                st.info("👆 Por favor, insira o nome da coleção para prosseguir", icon="ℹ️")

        # ── TAB 3: COMO UTILIZAR ───────────────────────────────────────────────
        with tab3:
            st.subheader("📖 Guia de Utilização")
            
            st.markdown("### 📤 Upload de Dados")
            st.markdown("""
            1. **Preparação do Arquivo**:
               - Prepare seu arquivo Excel (.xlsx ou .xls)
               - Certifique-se de que os dados estejam organizados em colunas
               - Verifique se não há caracteres especiais nos cabeçalhos

            2. **Processo de Upload**:
               - Clique em "Browse files" para selecionar seu arquivo
               - Digite um nome para sua coleção no MongoDB
               - Verifique a prévia dos dados exibida
               - Confirme os tipos de dados das colunas
               - Clique em "Enviar para MongoDB" para iniciar o upload

            3. **Data de Criação**:
               - Um campo `creation_date` é automaticamente adicionado a cada registro (UTC)
               - Esta data é usada para controle de duplicadas e versionamento
            """)
            
            st.markdown("### 🧹 Limpeza de Dados")
            st.markdown("""
            1. **Remoção de Duplicadas**:
               - Digite o nome da coleção que deseja limpar
               - Informe o campo para identificar duplicatas (padrão: `unique`)
               - Escolha qual registro manter:
                   * **Mais antigo** → preserva o primeiro registro inserido (menor `creation_date`)
                   * **Mais recente** → preserva o último registro inserido (maior `creation_date`)
               - Escolha o método:
                   * **Rápido**: ideal para coleções menores (usa mais memória)
                   * **Em Lotes**: recomendado para grandes volumes (mais lento, usa menos memória)

            2. **Processo de Limpeza**:
               - Confirme sua seleção e clique em "Remover Duplicadas"
               - Aguarde o processo ser concluído
               - Verifique o resumo: documentos removidos e restantes
            """)
            
            st.markdown("### 💡 Dicas e Boas Práticas")
            with st.expander("Expandir Dicas", expanded=False):
                st.markdown("""
                - **Preparação de Dados**:
                    * Limpe seus dados antes do upload
                    * Padronize os formatos de data
                    * Evite células vazias quando possível

                - **Gestão de Duplicadas**:
                    * Use **mais antigo** para preservar o dado original de entrada
                    * Use **mais recente** para preservar a versão mais atualizada
                    * Faça backups antes de limpar duplicadas

                - **Performance**:
                    * Para arquivos grandes, prefira uploads em horários de menor uso
                    * Use o método em lotes para grandes volumes de dados

                - **Resolução de Problemas**:
                    * Em caso de timeout, tente novamente
                    * Verifique sua conexão com a internet
                    * Para erros persistentes, verifique o formato dos dados
                """)
            
            st.markdown("### ❓ Perguntas Frequentes")
            with st.expander("Expandir FAQ", expanded=False):
                st.markdown("""
                **P: O que significa "mais antigo" vs "mais recente"?**  
                R: "Mais antigo" mantém o registro com o menor `creation_date` (primeiro inserido). "Mais recente" mantém o de maior `creation_date` (último inserido).

                **P: O campo para duplicatas precisa ser `unique`?**  
                R: Não. O padrão é `unique`, mas você pode digitar qualquer nome de campo existente na coleção.

                **P: Quais formatos de arquivo são aceitos?**  
                R: Arquivos Excel (.xlsx e .xls).

                **P: Como sei se meu upload foi bem-sucedido?**  
                R: Uma mensagem de sucesso será exibida com o número de registros inseridos.

                **P: Os dados são preservados antes da limpeza?**  
                R: O sistema não faz backup automático — faça backup manualmente antes de limpar.
                """)
            
            st.markdown("### 📞 Suporte")
            st.info("""
            Para suporte adicional ou relatar problemas:
            - Abra um ticket no sistema de suporte
            - Entre em contato com a equipe de desenvolvimento
            - Consulte a documentação técnica completa
            """)

    st.markdown("---")
    st.markdown(
        """
        <div style='text-align: center'>
            <p style='color: #888;'>Desenvolvido com ❤️ | Processador MongoDB Pro v1.0</p>
        </div>
        """,
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()
