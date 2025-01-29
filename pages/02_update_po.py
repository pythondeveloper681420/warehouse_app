import pandas as pd
import streamlit as st
import time
from datetime import datetime
import io
import gc
import logging
from typing import List, Tuple, Optional, Dict, Any
import numpy as np
import base64
import re

# Desabilitar a exibição de separadores de milhar
pd.options.display.float_format = '{:,.0f}'.format  # Para números decimais
pd.options.display.max_columns = None  # Exibe todas as colunas sem truncar


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
MAX_UPLOAD_SIZE_MB = 200
BYTES_PER_MB = 1024 * 1024
CHUNK_SIZE = 10000  # Number of rows to process at once

# Colunas selecionadas para salvar no arquivo final
SELECTED_COLUMNS = [
    'Purchasing Document', 
    'Item',
    
    'Supplier',
    
    'Vendor Name',
    'Material',
    'Material Description',
    'Order Quantity',
    'total_itens_po',
    'Order Unit',
    'Control Code (NCM)',
    'Project Code',
    'Andritz WBS Element',
    'codigo_projeto',
    'Cost Center',
    'Document Date', 
    'PO Creation Date',
    'PO Created by',
    'Purchase Requisition',
    'PR Created by',
    'Price unit',
    'Gross Price',
    'PBXX Condition Amount',
    'valor_unitario',
    'valor_item_com_impostos',
    'Net order value',
    'total_valor_po_liquido',
    'total_valor_po_com_impostos', 
    'valor_unitario_formatted',
    'valor_item_com_impostos_formatted',
    'Net order value_formatted',
    'total_valor_po_liquido_formatted',
    'total_valor_po_com_impostos_formatted',    
    'Purchasing Group',
    'Plant',               
    'unique'
]

#Document Date

class DataProcessor:
    """Class to handle all data processing operations"""    
    @staticmethod
    def format_currency(value: float) -> str:
        """Format value as Brazilian currency"""
        try:
            if pd.isna(value) or value == '':
                return "R$ 0,00"
            
            if isinstance(value, str):
                value = float(value.replace('.', '').replace(',', '.'))
            
            value = float(value)
            integer_part = int(value)
            decimal_part = int((value - integer_part) * 100)
            
            formatted_integer = '{:,}'.format(integer_part).replace(',', '.')
            return f"R$ {formatted_integer},{decimal_part:02d}"
        except Exception as e:
            logger.warning(f"Error formatting currency value {value}: {str(e)}")
            return "R$ 0,00"

    @staticmethod
    def safe_division(x: float, y: float) -> float:
        """Safely perform division handling zero division"""
        try:
            return x / y if y != 0 else 0
        except:
            return 0

    @staticmethod
    def process_chunk(df: pd.DataFrame) -> pd.DataFrame:
        """Process a chunk of data"""
        try:
            chunk_processed = df.copy()
            
            numeric_columns = ['Net order value', 'Order Quantity', 'PBXX Condition Amount']
            for col in numeric_columns:
                if col in chunk_processed.columns:
                    chunk_processed[col] = pd.to_numeric(chunk_processed[col], errors='coerce')
                    chunk_processed[col] = chunk_processed[col].fillna(0)
            
            chunk_processed['valor_unitario'] = chunk_processed.apply(
                lambda row: DataProcessor.safe_division(row['Net order value'], row['Order Quantity']),
                axis=1
            )
            
            chunk_processed['valor_item_com_impostos'] = (
                chunk_processed['PBXX Condition Amount'] * chunk_processed['Order Quantity']
            )
            
            return chunk_processed
            
        except Exception as e:
            logger.error(f"Error processing chunk: {str(e)}")
            raise
                   
    @staticmethod
    def process_dataframe(df: pd.DataFrame, progress_bar: Any) -> pd.DataFrame:
        """Process the complete DataFrame with progress tracking"""
        try:
            chunk_size = CHUNK_SIZE
            num_chunks = len(df) // chunk_size + 1
            processed_chunks = []
            
            for i in range(num_chunks):
                start_idx = i * chunk_size
                end_idx = min((i + 1) * chunk_size, len(df))
                chunk = df.iloc[start_idx:end_idx]
                
                processed_chunk = DataProcessor.process_chunk(chunk)
                processed_chunks.append(processed_chunk)
                
                progress = (i + 1) / num_chunks
                progress_bar.progress(progress)
                
            df_processed = pd.concat(processed_chunks, ignore_index=True)
            
            # Eliminar as linhas onde os valores na coluna 'coluna' são strings
            df_processed = df_processed[~df_processed['Purchasing Document'].apply(lambda x: isinstance(x, str))]
                       
            df_processed['unique'] = (
                df_processed['Purchasing Document'].astype(str) + 
                df_processed['Item'].astype(str)
            )
            
            
            
            # Evitar SettingWithCopyWarning e garantir a conversão correta
            # df_processed.loc[:, 'unique'] = df_processed['Purchasing Document'].astype(str) + df_processed['Item'].astype(str)

            # Garantir que a coluna 'Supplier' seja tratada como string (para evitar problemas com valores não numéricos)
            df_processed['Supplier'] = df_processed['Supplier'].astype(str)
            
            df_processed = df_processed.drop_duplicates(subset=['unique'])
            
            groupby_cols = ['Purchasing Document']
            df_processed['total_valor_po_liquido'] = df_processed.groupby(groupby_cols)['Net order value'].transform('sum')
            df_processed['total_valor_po_com_impostos'] = df_processed.groupby(groupby_cols)['valor_item_com_impostos'].transform('sum')
            df_processed['total_itens_po'] = df_processed.groupby(groupby_cols)['Order Quantity'].transform('sum')
                        
            # df_processed['Purchasing Document'] = pd.to_numeric(df_processed['Purchasing Document'], errors='coerce')
            # df_processed = df_processed.dropna(subset=['Purchasing Document'])
            # df_processed['Purchasing Document'] = df_processed['Purchasing Document'].astype(int)
            
            df_processed['PO Creation Date'] = pd.to_datetime(df_processed['Document Date'], dayfirst=True)
            df_processed = df_processed.sort_values(by='PO Creation Date', ascending=False)
            

            
            currency_columns = [
                'valor_unitario', 'valor_item_com_impostos', 'Net order value',
                'total_valor_po_liquido', 'total_valor_po_com_impostos'
            ]
            
            for col in currency_columns:
                df_processed[f'{col}_formatted'] = df_processed[col].apply(DataProcessor.format_currency)
            
            date_columns = [
                'Document Date', 'Delivery date', 'Last FUP', 
                'Stat.-Rel. Del. Date', 'Delivery Date', 
                'Requisition Date', 'Inspection Request Date',
                'First Delivery Date', 'Purchase Requisition Delivery Date'
            ]
            
            for col in date_columns:
                if col in df_processed.columns:
                    df_processed[col] = pd.to_datetime(
                        df_processed[col],
                        format='%d/%m/%Y',
                        dayfirst=True,
                        errors='coerce'
                    )
                    df_processed[col] = df_processed[col].dt.strftime('%d/%m/%Y')
                    
            @staticmethod
            def extract_code(text):
                """
                Extrai apenas os 6 dígitos do padrão X-XX-XXXXXX-XXX-XXXX-XXX, onde X pode ser letra ou número.
                
                Parameters:
                text (str): O texto onde procurar os códigos
                
                Returns:
                str: String apenas com os 6 dígitos ou vazio se não encontrar
                """
                
                
                if not text or not isinstance(text, str):
                    return ""
                
                # Padrão para capturar 6 dígitos após qualquer letra/número e traço
                pattern = r'[A-Z0-9]-[A-Z0-9]{2}-(\d{6})-\d{3}-\d{4}-\d{3}'
                
                # Encontra o match no texto
                match = re.search(pattern, text)
                
                # Retorna apenas os 6 dígitos se encontrar
                return match.group(1) if match else ""
            df_processed['codigo_projeto'] = df_processed['Andritz WBS Element'].apply(extract_code)
            df_processed['codigo_projeto'] = df_processed['codigo_projeto'].apply(
                lambda x: int(x) if x != "" else ""
            )     
            
            
                     
            # Lista de colunas que você quer limpar
            columns_to_clean = [ 'Purchasing Document', 'Item', 'Material']

            for col in columns_to_clean:
                df[col] = (
                    df[col]                          # Seleciona a coluna
                    .astype(str)                     # Converte para string
                    .str.replace(r'\D', '', regex=True)  # Remove caracteres não numéricos
                    .replace('', pd.NA)             # Substitui strings vazias por NaN
                    .astype(pd.Int64Dtype())         # Converte para Int64 (com suporte a NaN)
                )
            
            df_processed = df_processed[SELECTED_COLUMNS] 
                    
            return df_processed
        
                         
        except Exception as e:
            logger.error(f"Error in process_dataframe: {str(e)}")
            raise
             
class FileHandler:
    """Class to handle file operations"""
    
    @staticmethod
    def calculate_total_size(files: List[Any]) -> float:
        """Calculate total size of uploaded files in MB"""
        return sum(file.size for file in files) / BYTES_PER_MB

    @staticmethod
    def to_excel(df: pd.DataFrame) -> str:
        """Convert DataFrame to Excel file and return as base64 string"""
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False)
        excel_data = output.getvalue()
        b64 = base64.b64encode(excel_data).decode()
        return b64

    @staticmethod
    def read_excel_file(file: Any) -> Optional[pd.DataFrame]:
        """Safely read Excel file"""
        try:
            return pd.read_excel(file, engine='openpyxl')
        except Exception as e:
            logger.error(f"Error reading file {file.name}: {str(e)}")
            return None

def clear_session_state():
    """Clear all session state variables"""
    for key in list(st.session_state.keys()):
        del st.session_state[key]
    gc.collect()

def get_download_link(b64_data: str, filename: str) -> str:
    """Generate HTML download link for Excel file with callback"""
    href = f'data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64_data}'
    return f'''
        <a href="{href}" 
           download="{filename}" 
           class="downloadButton"
           onclick="setTimeout(function(){{ window.location.href = window.location.pathname; }}, 1000);">
           📥 Baixar Arquivo Excel Processado
        </a>
        <script>
            window.addEventListener('load', function() {{
                document.querySelector('.downloadButton').addEventListener('click', function() {{
                    setTimeout(function() {{
                        window.location.reload();
                    }}, 1000);
                }});
            }});
        </script>
    '''

def main():
    """Main application function"""
    st.set_page_config(
        page_title="Sistema de Processamento de PO",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="collapsed"
    )
    
    # Add custom CSS for download button
    st.markdown("""
        <style>
        .downloadButton {
            background-color: #0075be;
            color: white !important;
            padding: 0.5em 1em;
            text-decoration: none;
            border-radius: 5px;
            border: none;
            display: inline-block;
            width: 100%;
            text-align: center;
            margin: 1em 0;
            font-weight: 500;
        }
        .downloadButton:hover {
            background-color: #4098ce;
            color: white !important;
            text-decoration: none;
        }
        </style>
    """, unsafe_allow_html=True)
    
    # Initialize session state
    if 'initialized' not in st.session_state:
        clear_session_state()
        st.session_state.initialized = True
        st.session_state.processed_data = None
        st.session_state.download_filename = None
        st.session_state.excel_data = None
        st.session_state.download_triggered = False
    
    st.header("📑 Sistema de Processamento de Pedidos de Compra")
    #st.subheader("📁 Seleção de Arquivos")
    tab1, tab2, tab3 = st.tabs(["📤 Upload e Extração", "📊 Visualização de Dados", "❓ Como Utilizar"])

    with tab1:   
        col1, col2 = st.columns([3, 1])
        
        with col1:
            uploaded_files = st.file_uploader(
                "Selecione os arquivos Excel para processar",
                type=['xlsx'],
                accept_multiple_files=True,
                help="Você pode selecionar múltiplos arquivos Excel (.xlsx)"
            )
        
        with col2:
            if uploaded_files:
                total_size = FileHandler.calculate_total_size(uploaded_files)
                remaining_size = MAX_UPLOAD_SIZE_MB - total_size
                
                st.metric(
                    label="📦 Espaço utilizado",
                    value=f"{total_size:.1f}MB"
                )
                st.metric(
                    label="⚡ Espaço disponível",
                    value=f"{remaining_size:.1f}MB"
                )
        if uploaded_files:
            if st.button("🚀 Iniciar Processamento", use_container_width=True, type="primary"):
                try:
                    randon = datetime.now().strftime("%d%m%Y%H%M%S") + str(datetime.now().microsecond)[:3]
                    with st.spinner("Processando arquivos..."):
                        progress_bar = st.progress(0)
                        status_placeholder = st.empty()
                        
                        start_time = time.time()
                        all_dfs = []
                        
                        for idx, uploaded_file in enumerate(uploaded_files):
                            status_placeholder.info(f"Processando: {uploaded_file.name}")
                            df_temp = FileHandler.read_excel_file(uploaded_file)
                            
                            if df_temp is not None and not df_temp.empty:
                                all_dfs.append(df_temp)
                            
                            progress_bar.progress((idx + 1) / len(uploaded_files))
                        
                        if all_dfs:
                            df_final = pd.concat(all_dfs, ignore_index=True)
                            
                            df = df_final 
                            
                            df_processed = DataProcessor.process_dataframe(df_final, progress_bar)
                            
                            st.session_state.processed_data = df_processed
                            
                            st.session_state.download_filename = f"PO_{randon}.xlsx"
                            
                            # Convert to base64 and store in session state
                            st.session_state.excel_data = FileHandler.to_excel(df_processed)
                            
                            elapsed_time = time.time() - start_time
                            
                            st.success("✅ Processamento concluído com sucesso!")
                            
                            col1, col2, col3 = st.columns(3)
                            col1.metric("Tempo de processamento", f"{elapsed_time:.2f}s")
                            col2.metric("Arquivos processados", len(uploaded_files))
                            col3.metric("Registros processados", len(df_processed))
                        else:
                            st.warning("⚠️ Nenhum dado encontrado para processar!")
                        
                        gc.collect()
                
                except Exception as e:
                    logger.error(f"Error during processing: {str(e)}")
                    st.error(f"❌ Erro durante o processamento: {str(e)}")
        
        if st.session_state.excel_data is not None:
            st.subheader("📥 Download do Arquivo Processado")
            download_link = get_download_link(
                st.session_state.excel_data,
                st.session_state.download_filename
            )
            st.markdown(download_link, unsafe_allow_html=True)

            # Add a button to manually clear the cache and return to initial state
            if st.button("🔄 Limpar e Voltar ao Início", use_container_width=True):
                clear_session_state()
                st.rerun()
                
    with tab2:
       if st.session_state.excel_data is not None:
                 
            df=df[['Purchasing Document',
                'Item',
                #'Supplier',
                'Vendor Name',
                'Material',
                'Material Description',
                'Order Quantity',
                'Order Unit',
                'Control Code (NCM)',
                'Project Code',
                'Andritz WBS Element',
                #'codigo_projeto',
                'Cost Center',
                'Document Date', 
                # 'PO Creation Date',
                'PO Created by',
                'Purchase Requisition',
                # 'total_itens_po',
                # 'valor_unitario_formatted', 
                # 'total_valor_po_liquido_formatted', 
                # 'total_valor_po_com_impostos_formatted',
                # 'Order Quantity',
                # 'total_itens_po',
                # 'unique'
                ]]
            # df['unique'] = (
            #     df['Purchasing Document'].astype(str) + 
            #     df['Item'].astype(str)
            # )
                        # Evitar SettingWithCopyWarning e garantir a conversão correta
            # Crie uma cópia explícita do DataFrame se necessário
            df = df.copy() 
                       
            df.loc[:, 'unique'] = df['Purchasing Document'].astype(str) + df['Item'].astype(str)

            # Garantir que a coluna 'Supplier' seja tratada como string (para evitar problemas com valores não numéricos)
            
            
            df = df.drop_duplicates(subset=['unique'])
            
                        # Lista de colunas que você quer limpar
            columns_to_clean = ['unique', 'Purchasing Document', 'Item', 'Material']

            for col in columns_to_clean:
                df[col] = (
                    df[col]                          # Seleciona a coluna
                    .astype(str)                     # Converte para string
                    .str.replace(r'\D', '', regex=True)  # Remove caracteres não numéricos
                    .replace('', pd.NA)             # Substitui strings vazias por NaN
                    .astype(pd.Int64Dtype())         # Converte para Int64 (com suporte a NaN)
                )
            
            
            
            #st.dataframe(df)
            st.header("Visualização de Dados")
                    # Key Metrics
            col1, col2, col3 = st.columns(3)
                    
            with col1:
                total_invoices = len(df)
                st.metric(label="Total de Linhas", value=total_invoices)
        
            with col2:
                 unique_issuers = df['Vendor Name'].nunique()
                 st.metric(label="Número de Fornecedores", value=unique_issuers)
        
            with col3:
                 unique_issuers = df['Purchasing Document'].nunique()
                 st.metric(label="Número de PO'S", value=unique_issuers)
            
            # # Global Search Filter
            # st.subheader("Filtrar Dados")
            # search_term = st.text_input("Busca Global (filtra em todas as colunas)")
        
            # if search_term:
            #     # Create a boolean mask that checks if the search term is in any column
            #         mask = df.apply(lambda row: row.astype(str).str.contains(search_term, case=False).any(), axis=1)
            #         filtered_df = df[mask]
            # else:
            #         filtered_df = df
            
                # Display filtered DataFrame without index
            st.dataframe(df, hide_index=True)  
       else:
            st.info("Faça o upload dos arquivos na aba 'Upload e Extração' para visualizar os dados.")
           
    
    with tab3:
        st.subheader("📖 Guia de Utilização")
        st.markdown("""
        ### Como usar o Sistema de Processamento de PO
        
        1. **Upload de Arquivos**
           - Acesse a aba "Upload e Extração"
           - Selecione um ou mais arquivos Excel (.xlsx)
           - O sistema aceita arquivos até 200MB no total
        
        2. **Processamento**
           - Clique em "Iniciar Processamento"
           - Aguarde o processamento ser concluído
           - Faça o download do arquivo processado
        
        3. **Visualização**
           - Acesse a aba "Visualização de Dados"
           - Explore os gráficos e métricas
           - Utilize a tabela de dados para análises detalhadas
        
        ### Colunas Processadas
        O sistema processa as seguintes informações:
        - Número do Pedido de Compra
        - Informações do Fornecedor
        - Detalhes dos Materiais
        - Valores e Quantidades
        - Datas e Informações Adicionais
        
        ### Dúvidas Frequentes
        1. **Tipos de arquivo aceitos?**
           - Apenas arquivos Excel (.xlsx)
        
        2. **Limite de tamanho?**
           - 200MB no total
        
        3. **Dados processados são salvos?**
           - Não, os dados são processados apenas durante a sessão atual
        """)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Application error: {str(e)}")
        st.error("Ocorreu um erro inesperado. Por favor, tente novamente.")
        
# Footer
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center'>
        <p>Desenvolvido com ❤️ | PO Processor Pro v1.0</p>
    </div>
    """,
    unsafe_allow_html=True
)        