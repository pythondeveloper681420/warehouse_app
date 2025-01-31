import streamlit as st
import pandas as pd
from pymongo import MongoClient
import urllib.parse
import unicodedata
import re
import math
import io
from bson.objectid import ObjectId
import streamlit.components.v1 as components

def normalize_string(text):
    if not isinstance(text, str):
        return str(text)
    text = text.lower()
    text = ''.join(
        char for char in unicodedata.normalize('NFKD', text)
        if unicodedata.category(char) != 'Mn'
    )
    return re.sub(r'[^\w\s]', '', text)

def create_flexible_pattern(text):
    normalized = normalize_string(text)
    fragments = normalized.split()
    pattern = '.*'.join(f'(?=.*{re.escape(fragment)})' for fragment in fragments)
    return pattern + '.*'

@st.cache_resource
def get_mongodb_client():
    username = st.secrets["MONGO_USERNAME"]
    password = st.secrets["MONGO_PASSWORD"]
    cluster = st.secrets["MONGO_CLUSTER"]
    database = st.secrets["MONGO_DB"]
    
    escaped_username = urllib.parse.quote_plus(username)
    escaped_password = urllib.parse.quote_plus(password)
    URI = f"mongodb+srv://{escaped_username}:{escaped_password}@{cluster}/{database}?retryWrites=true&w=majority"
    
    return MongoClient(URI)

@st.cache_data
def get_collection_columns(collection_name):
    client = get_mongodb_client()
    db = client.warehouse
    collection = db[collection_name]
    
    total_docs = collection.count_documents({})
    sample_doc = collection.find_one()
    
    columns = []
    if sample_doc:
        columns = [col for col in sample_doc.keys() if col != '_id']
    
    default_columns = {
        'xml': [
            'url_imagens', 'Nota Fiscal', 'Item Nf', 'Nome Material',
            'Codigo NCM', 'Quantidade', 'Unidade', 'Valor Unitario Produto',
            'Valor Total Produto', 'Valor Total Nota Fiscal', 'Total itens Nf',
            'data nf', 'Data Vencimento', 'Chave NF-e', 'Nome Emitente',
            'CNPJ Emitente', 'CFOP Categoria', 'PO', 'Itens recebidos PO',
            'Valor Recebido PO', 'Codigo Projeto', 'Projeto WBS Andritz',
            'Centro de Custo', 'Codigo Projeto Envio', 'Projeto Envio',
            'grupo', 'subgrupo'
        ],
        'nfspdf': ['Competencia', 'CNPJ Prestador'],
        'po': ['Item', 'Supplier']
    }
    
    fallback = default_columns.get(collection_name, columns[:27])
    final_defaults = [col for col in fallback if col in columns]
    
    if not final_defaults:
        final_defaults = columns[:10]
    
    return total_docs, columns, final_defaults

class CardManager:
    def __init__(self, collection_name):
        self.collection_name = collection_name
        if 'edit_cards' not in st.session_state:
            st.session_state.edit_cards = set()
        if 'delete_cards' not in st.session_state:
            st.session_state.delete_cards = set()

    def get_collection(self):
        client = get_mongodb_client()
        return client.warehouse[self.collection_name]

    def update_document(self, card_id, data):
        try:
            collection = self.get_collection()
            collection.update_one(
                {"_id": ObjectId(card_id)},
                {"$set": data}
            )
            return True, "Record updated successfully!"
        except Exception as e:
            return False, f"Update error: {str(e)}"

    def delete_document(self, card_id):
        try:
            collection = self.get_collection()
            collection.delete_one({"_id": ObjectId(card_id)})
            return True, "Record deleted successfully!"
        except Exception as e:
            return False, f"Delete error: {str(e)}"

    def render_edit_modal(self, card_id, record, visible_cols, image_cols):
        dialog_key = f"edit_dialog_{self.collection_name}_{card_id}"
        
        with st.container():
            edited_data = {}
            for col in visible_cols:
                current_value = record.get(col, "")
                
                # Exibir _id como campo não editável
                if col == '_id':
                    st.text_input(
                        "ID do Documento",
                        value=current_value,
                        key=f"edit_{self.collection_name}_{card_id}_{col}",
                        disabled=True  # Campo desabilitado
                    )
                    continue  # Pula para próxima coluna
                
                # Campos normais (editáveis)
                edited_data[col] = st.text_input(
                    col,
                    current_value,
                    key=f"edit_{self.collection_name}_{card_id}_{col}"
                )
            
            # Botões de ação (mantidos)
            col1, col2 = st.columns(2)
            with col1:
                if st.button("Salvar", type="primary", key=f"save_{self.collection_name}_{card_id}"):
                    success, message = self.update_document(card_id, edited_data)
                    if success:
                        st.success(message)
                        st.session_state.edit_cards.remove(card_id)
                        st.rerun()
                    else:
                        st.error(message)
            with col2:
                if st.button("Cancelar", key=f"cancel_edit_{self.collection_name}_{card_id}"):
                    st.session_state.edit_cards.remove(card_id)
                    st.rerun()

    def render_delete_modal(self, card_id):
        dialog_key = f"delete_dialog_{self.collection_name}_{card_id}"
        
        # Create a placeholder for the dialog
        dialog = st.container()
        
        with dialog:
            st.warning("Are you sure you want to delete this record?")
            col1, col2 = st.columns(2)
            with col1:
                if st.button(
                    "Yes, Delete",
                    type="primary",
                    key=f"confirm_delete_{self.collection_name}_{card_id}"
                ):
                    success, message = self.delete_document(card_id)
                    if success:
                        st.success(message)
                        st.session_state.delete_cards.remove(card_id)
                        st.rerun()
                    else:
                        st.error(message)
            with col2:
                if st.button(
                    "Cancel",
                    key=f"cancel_delete_{self.collection_name}_{card_id}"
                ):
                    st.session_state.delete_cards.remove(card_id)
                    st.rerun()

def get_unique_values(collection_name, column):
    client = get_mongodb_client()
    collection = client.warehouse[collection_name]
    
    pipeline = [
        {"$group": {"_id": f"${column}"}},
        {"$sort": {"_id": 1}},
        {"$limit": 100000}
    ]
    
    try:
        unique_values = [
            doc["_id"] for doc in collection.aggregate(pipeline)
            if doc["_id"] is not None
        ]
        return sorted(unique_values)
    except Exception as e:
        st.error(f"Error getting unique values for {column}: {str(e)}")
        return []

def convert_to_numeric(value):
    clean_value = str(value).strip().replace(',', '.')
    try:
        return int(clean_value)
    except ValueError:
        try:
            return float(clean_value)
        except ValueError:
            return value

def get_column_types(collection_name):
    try:
        client = get_mongodb_client()
        collection = client.warehouse[collection_name]
        sample_doc = collection.find_one()
        
        if not sample_doc:
            st.warning(f"No documents found in collection {collection_name}")
            return {}
        
        def determine_type(value):
            if value is None:
                return 'str'
            if isinstance(value, int):
                return 'int64'
            elif isinstance(value, float):
                return 'float64'
            elif isinstance(value, str):
                try:
                    int(value.replace(',', ''))
                    return 'int64'
                except ValueError:
                    try:
                        float(value.replace(',', '.'))
                        return 'float64'
                    except ValueError:
                        return 'str'
            return 'str'
        
        return {k: determine_type(v) for k, v in sample_doc.items() if k != '_id'}
    except Exception as e:
        st.error(f"Error getting column types: {str(e)}")
        return {}

def build_mongo_query(filters, column_types):
    query = {}
    
    for column, filter_info in filters.items():
        filter_type = filter_info['type']
        filter_value = filter_info['value']
        
        if not filter_value:
            continue
        
        if column_types.get(column, 'str') in ['int64', 'float64']:
            try:
                numeric_value = convert_to_numeric(filter_value)
                if isinstance(numeric_value, (int, float)):
                    query[column] = numeric_value
                    continue
            except:
                pass
        
        if filter_type == 'text':
            pattern = create_flexible_pattern(filter_value)
            query[column] = {'$regex': pattern, '$options': 'i'}
        elif filter_type == 'multi':
            if filter_value:
                query[column] = {'$in': filter_value}
    
    return query

def create_filter_interface(collection_name, columns):
    column_types = get_column_types(collection_name)
    filters = {}
    
    with st.expander("**Filters:**", expanded=False):
        selected_columns = st.multiselect(
            "Select columns to filter:",
            columns,
            key=f"filter_cols_{collection_name}"
        )
        
        if selected_columns:
            cols = st.columns(2)
            for idx, column in enumerate(selected_columns):
                with cols[idx % 2]:
                    st.markdown(f"#### {column}")
                    column_type = column_types.get(column, 'str')
                    filter_type = st.radio(
                        "Filter type:",
                        ["Text", "Multiple Selection"],
                        key=f"radio_{collection_name}_{column}",
                        horizontal=True
                    )
                    
                    if filter_type == "Text":
                        value = st.text_input(
                            f"Search {column}" + (" (numeric)" if column_type in ['int64', 'float64'] else ""),
                            key=f"text_filter_{collection_name}_{column}"
                        )
                        if value:
                            filters[column] = {'type': 'text', 'value': value}
                    else:
                        unique_values = get_unique_values(collection_name, column)
                        if unique_values:
                            selected = st.multiselect(
                                "Select values:",
                                options=unique_values,
                                key=f"multi_filter_{collection_name}_{column}"
                            )
                            if selected:
                                filters[column] = {'type': 'multi', 'value': selected}
                    
                    st.markdown("---")
    
    return filters, column_types

def convert_for_pandas(doc):
    converted = {}
    for key, value in doc.items():
        if isinstance(value, ObjectId):
            converted[key] = str(value)
        elif isinstance(value, dict):
            converted[key] = convert_for_pandas(value)
        elif isinstance(value, list):
            converted[key] = [str(item) if isinstance(item, ObjectId) else item for item in value]
        else:
            converted[key] = value
    return converted

def load_paginated_data(collection_name, page, page_size, filters=None, column_types=None):
    if column_types is None:
        column_types = get_column_types(collection_name)
    
    client = get_mongodb_client()
    collection = client.warehouse[collection_name]
    query = build_mongo_query(filters, column_types) if filters else {}
    skip = (page - 1) * page_size
    
    try:
        sorting = [("Data Emissao", -1)] if collection_name == 'xml' else None
        total_filtered = collection.count_documents(query)
        
        if sorting:
            try:
                cursor = collection.find(query).sort(sorting).skip(skip).limit(page_size)
            except Exception:
                cursor = collection.find(query).skip(skip).limit(page_size)
        else:
            cursor = collection.find(query).skip(skip).limit(page_size)
        
        documents = [convert_for_pandas(doc) for doc in cursor]
        df = pd.DataFrame(documents)

            
        return df, total_filtered
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return pd.DataFrame(), 0

def process_urls(urls):
    if isinstance(urls, pd.Series):
        urls = urls.iloc[0]
    if urls is None or (isinstance(urls, float) and math.isnan(urls)):
        return None
    if isinstance(urls, str):
        return urls
    if isinstance(urls, list):
        valid_urls = [url for url in urls if url and isinstance(url, str)]
        return valid_urls[0] if valid_urls else None
    return None

def format_value(value):
    if isinstance(value, pd.Series):
        value = value.iloc[0]
    if pd.isna(value):
        return '-'
    if isinstance(value, (int, float)):
        if float(value).is_integer():
            return f'{int(value):,}'.replace(',', '')
        return f'{value:.2f}'.replace('.', ',')
    if isinstance(value, str):
        clean_value = value.strip().replace(',', '.')
        try:
            num_value = float(clean_value)
            if float(num_value).is_integer():
                return f'{int(num_value):,}'.replace(',', '')
            return f'{num_value:.2f}'.replace('.', ',')
        except ValueError:
            return str(value)[:35]
    return str(value)[:35]

def render_cards(df, visible_columns, collection_name):
    manager = CardManager(collection_name)
    image_columns = [col for col in df.columns if 'url_imagens' in col.lower()]
    num_columns = 5
    rows = [df.iloc[i:i+num_columns] for i in range(0, len(df), num_columns)]
    
    for row_idx, row in enumerate(rows):
        cols = st.columns(num_columns)
        
        for col_idx, (_, record) in enumerate(row.iterrows()):
            with cols[col_idx]:
                image_url = None
                for img_col in image_columns:
                    if img_col in record:
                        image_url = process_urls(record[img_col])
                        if image_url:
                            break
                
                # Conteúdo do Card
                card_details = ''.join([
                    f'<div style="margin-bottom: 4px; font-size: 0.8rem;">'
                    f'<strong>{col}:</strong> {format_value(record.get(col, "-"))}</div>'
                    for col in visible_columns 
                    if col not in image_columns and col != '_id'
                ])
                
                card_style = (
                    "border: 1px solid #e0e0e0; border-radius: 8px; "
                    "box-shadow: 0 4px 6px rgba(0,0,0,0.1); margin-bottom: 10px; "
                    "overflow: hidden; transition: transform 0.2s; "
                    "height: 400px;"
                )
                
                if image_url:
                    card_html = f"""
                    <div style='{card_style}'>
                        <div style="width:100%; height:180px; display:flex; justify-content:center; align-items:center; overflow:hidden;">
                            <img src="{image_url}" style="width:100%; height:100%; object-fit:contain; object-position:center;">
                        </div>
                        <div style='padding: 8px; font-size: 0.8rem; height: 180px; overflow-y: auto;'>
                            {card_details}
                        </div>
                    </div>
                    """
                else:
                    card_html = f"""
                    <div style='{card_style}'>
                        <div style='padding: 8px; font-size: 0.8rem; height: 360px; overflow-y: auto;'>
                            {card_details}
                        </div>
                    </div>
                    """
                
                st.markdown(card_html, unsafe_allow_html=True)
                
                # Botões ABAIXO do card
                col1, col2 = st.columns(2)
                with col1:
                    if st.button(
                        "✏️ Edit",
                        key=f"edit_btn_{collection_name}_{record['_id']}",
                        use_container_width=True
                    ):
                        st.session_state.edit_cards.add(str(record['_id']))
                with col2:
                    if st.button(
                        "🗑️ Delete",
                        key=f"delete_btn_{collection_name}_{record['_id']}",
                        use_container_width=True
                    ):
                        st.session_state.delete_cards.add(str(record['_id']))
                
                # Modais
                card_id = str(record['_id'])
                if card_id in st.session_state.edit_cards:
                    manager.render_edit_modal(card_id, record, visible_columns + ['_id'], image_columns)
                if card_id in st.session_state.delete_cards:
                    manager.render_delete_modal(card_id)

def display_data_page(collection_name):
    total_documents, columns, default_visible = get_collection_columns(collection_name)
    
    # Adicionar estas 2 linhas ↓
    columns = [col for col in columns if col != '_id']
    default_visible = [col for col in default_visible if col != '_id']
    
    if total_documents == 0:
        st.error(f"No documents found in collection {collection_name}")
        return

    with st.expander("**Visible Columns:**", expanded=False):
        state_key = f'visible_columns_{collection_name}'
        if state_key not in st.session_state:
            st.session_state[state_key] = default_visible
            
        visible_columns = st.multiselect(
            "Select columns to display:",
            options=columns,
            default=st.session_state[state_key],
            key=f'column_selector_{collection_name}'
        )
        st.session_state[state_key] = visible_columns
        
        if st.button("Show All Columns", key=f"show_all_{collection_name}"):
            st.session_state[state_key] = columns
            st.rerun()

    filters, column_types = create_filter_interface(collection_name, columns)
    
    with st.expander("**Settings:**", expanded=False):
        col1, col2, col3 = st.columns([1,1,1], gap='small')
        
        with col1:
            c1, c2 = st.columns([2, 1])
            c1.write('Records per page:')
            page_size = c2.selectbox(
                "Records per page:",
                options=[10, 25, 50, 100],
                index=1,
                key=f"page_size_{collection_name}",
                label_visibility='collapsed'
            )
        
        page_key = f'page_{collection_name}'
        if page_key not in st.session_state:
            st.session_state[page_key] = 1
        current_page = st.session_state[page_key]
        
        df, total_filtered = load_paginated_data(
            collection_name,
            current_page,
            page_size,
            filters,
            column_types
        )
        
        if not df.empty and visible_columns:
             df = df[visible_columns + ['_id']]  # ✅ Adicione esta linha
        
        total_pages = math.ceil(total_filtered / page_size) if total_filtered > 0 else 1
        current_page = min(current_page, total_pages)
        
        with col2:
            st.write(f"Total: {total_filtered} records | Page {current_page} of {total_pages}")
        
        with col3:
            cols = st.columns(4)
            navigation = {
                "⏪": lambda: st.session_state.update({page_key: 1}),
                "◀️": lambda: st.session_state.update({page_key: max(1, current_page - 1)}),
                "▶️": lambda: st.session_state.update({page_key: min(total_pages, current_page + 1)}),
                "⏩": lambda: st.session_state.update({page_key: total_pages})
            }
            
            for idx, (text, callback) in enumerate(navigation.items()):
                if cols[idx].button(text, key=f"{text}_{collection_name}"):
                    callback()
                    st.rerun()

    if not df.empty:
        render_cards(df, visible_columns, collection_name)
        
        if st.button("📥 Download filtered data", key=f"download_{collection_name}"):
            progress_text = "Preparing download..."
            progress_bar = st.progress(0, text=progress_text)
            
            all_data = []
            batch_size = 1000
            total_download_pages = math.ceil(total_filtered / batch_size)
            
            for page in range(1, total_download_pages + 1):
                progress = page / total_download_pages
                progress_bar.progress(progress, text=f"{progress_text} ({page}/{total_download_pages})")
                
                page_df, _ = load_paginated_data(collection_name, page, batch_size, filters)
                if visible_columns:
                    page_df = page_df[visible_columns]
                all_data.append(page_df)
            
            complete_df = pd.concat(all_data, ignore_index=True)
            
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                complete_df.to_excel(writer, index=False, sheet_name='Data')
            
            st.download_button(
                label="💾 Click to download Excel",
                data=buffer.getvalue(),
                file_name=f'{collection_name}_data.xlsx',
                mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
            
            progress_bar.empty()
    else:
        st.warning("No data found with applied filters")

def initialize_session_state():
    if 'user' not in st.session_state:
        st.switch_page("app.py")

def main():
    st.set_page_config(
        page_title="Home",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="collapsed"
    )
    initialize_session_state()

    with st.sidebar:
        user = st.session_state.user
        name = user.get('name')
        email = user.get('email', '')
        phone = user.get('phone', '')
        
        st.markdown(
            f"""
            <div style='
                padding: 1rem;
                border-radius: 0.5rem;
                background-color: #f0f2f6;
                margin-bottom: 1rem;
            '>
                <div style='
                    font-size: 1.1rem;
                    font-weight: bold;
                    margin-bottom: 0.5rem;
                    display: flex;
                    justify-content: center;
                '>
                    {name}
                </div>
                <div style='
                    font-size: 0.9rem;
                    color: #666;
                    display: flex;
                    justify-content: center;
                '>
                    {email}
                </div>
                <div style='
                    font-size: 0.9rem;
                    color: #666;
                    display: flex;
                    justify-content: center;
                '>
                    {phone}
                </div>
            </div>
            """,
            unsafe_allow_html=True
        )

        initials = st.session_state.user['initials']
        st.markdown(
            f"""
            <div style='display: flex; justify-content: center; margin-bottom: 1.5rem;'>
                <div style='
                    display: flex;
                    justify-content: center;
                    background: linear-gradient(135deg, #0075be, #00a3e0);
                    color: white;
                    border-radius: 50%;
                    width: 48px;
                    height: 48px;
                    align-items: center;
                    font-size: 20px;
                    font-weight: 600;
                    box-shadow: 0 3px 6px rgba(0,0,0,0.16);
                    transition: transform 0.2s ease;
                    cursor: pointer;
                ' onmouseover="this.style.transform='scale(1.05)'"
                onmouseout="this.style.transform='scale(1)'">
                    {initials}
                </div>
            </div>
            """,
            unsafe_allow_html=True
        )
        
        if st.button(
            "Logout",
            key="logout_button",
            type="primary",
            use_container_width=True
        ):
            for key in st.session_state.keys():
                del st.session_state[key]
            st.switch_page("app.py")

    col1, col2 = st.columns([3, 1], gap="large")
    with col1:
        st.markdown('## **📊 :rainbow[Home]**')

    collections = ['xml', 'nfspdf', 'po']
    tabs = st.tabs([collection.upper() for collection in collections])
    
    for tab, collection_name in zip(tabs, collections):
        with tab:
            display_data_page(collection_name)
    
    st.divider()
    st.caption("MongoDB Data Dashboard v1.0")

if __name__ == "__main__":
    main()