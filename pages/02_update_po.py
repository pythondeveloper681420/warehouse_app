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
pd.options.display.float_format = '{:,.0f}'.format
pd.options.display.max_columns = None

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
MAX_UPLOAD_SIZE_MB = 200
BYTES_PER_MB = 1024 * 1024
CHUNK_SIZE = 10000

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


def extract_code(text: str) -> str:
    """
    Extrai apenas os 6 dígitos do padrão X-XX-XXXXXX-XXX-XXXX-XXX.
    """
    if not text or not isinstance(text, str):
        return ""
    pattern = r'[A-Z0-9]-[A-Z0-9]{2}-(\d{6})-\d{3}-\d{4}-\d{3}'
    match = re.search(pattern, text)
    return match.group(1) if match else ""


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
            decimal_part = int(round((value - integer_part) * 100))
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

            # Eliminar linhas onde 'Purchasing Document' é string não numérica
            df_processed = df_processed[
                ~df_processed['Purchasing Document'].apply(lambda x: isinstance(x, str))
            ]

            df_processed['unique'] = (
                df_processed['Purchasing Document'].astype(str) +
                df_processed['Item'].astype(str)
            )

            # Tratar coluna 'Supplier' com segurança — criar se não existir
            if 'Supplier' in df_processed.columns:
                df_processed['Supplier'] = df_processed['Supplier'].astype(str)
            else:
                logger.warning("Coluna 'Supplier' não encontrada. Criando coluna vazia.")
                df_processed['Supplier'] = ''

            df_processed = df_processed.drop_duplicates(subset=['unique'])

            groupby_cols = ['Purchasing Document']
            df_processed['total_valor_po_liquido'] = df_processed.groupby(groupby_cols)['Net order value'].transform('sum')
            df_processed['total_valor_po_com_impostos'] = df_processed.groupby(groupby_cols)['valor_item_com_impostos'].transform('sum')
            df_processed['total_itens_po'] = df_processed.groupby(groupby_cols)['Order Quantity'].transform('sum')

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

            # Usar a função extract_code definida no módulo (fora da classe)
            if 'Andritz WBS Element' in df_processed.columns:
                df_processed['codigo_projeto'] = df_processed['Andritz WBS Element'].apply(extract_code)
                df_processed['codigo_projeto'] = df_processed['codigo_projeto'].apply(
                    lambda x: int(x) if x != "" else ""
                )
            else:
                df_processed['codigo_projeto'] = ""

            # Limpar colunas numéricas
            columns_to_clean = ['Purchasing Document', 'Item', 'Material']
            for col in columns_to_clean:
                if col in df_processed.columns:
                    df_processed[col] = (
                        df_processed[col]
                        .astype(str)
                        .str.replace(r'\D', '', regex=True)
                        .replace('', pd.NA)
                        .astype(pd.Int64Dtype())
                    )

            # Selecionar apenas colunas existentes no DataFrame
            cols_to_select = [c for c in SELECTED_COLUMNS if c in df_processed.columns]
            df_processed = df_processed[cols_to_select]

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
    """Generate HTML download link for Excel file"""
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

    if 'initialized' not in st.session_state:
        clear_session_state()
        st.session_state.initialized = True
        st.session_state.processed_data = None
        st.session_state.download_filename = None
        st.session_state.excel_data = None
        st.session_state.download_triggered = False
        st.session_state.df_view = None  # DataFrame para visualização

    st.header("📑 Sistema de Processamento de Pedidos de Compra")
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
                st.metric(label="📦 Espaço utilizado", value=f"{total_size:.1f}MB")
                st.metric(label="⚡ Espaço disponível", value=f"{remaining_size:.1f}MB")

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
                            df_processed = DataProcessor.process_dataframe(df_final, progress_bar)

                            st.session_state.processed_data = df_processed
                            st.session_state.download_filename = f"PO_{randon}.xlsx"
                            st.session_state.excel_data = FileHandler.to_excel(df_processed)

                            # Preparar DataFrame para visualização
                            view_cols = [
                                'Purchasing Document', 'Item', 'Vendor Name', 'Material',
                                'Material Description', 'Order Quantity', 'Order Unit',
                                'Control Code (NCM)', 'Project Code', 'Andritz WBS Element',
                                'Cost Center', 'Document Date', 'PO Created by',
                                'Purchase Requisition'
                            ]
                            available_view_cols = [c for c in view_cols if c in df_processed.columns]
                            df_view = df_processed[available_view_cols].copy()
                            df_view['unique'] = (
                                df_view['Purchasing Document'].astype(str) +
                                df_view['Item'].astype(str)
                            )
                            df_view = df_view.drop_duplicates(subset=['unique'])

                            clean_cols = ['unique', 'Purchasing Document', 'Item', 'Material']
                            for col in clean_cols:
                                if col in df_view.columns:
                                    df_view[col] = (
                                        df_view[col]
                                        .astype(str)
                                        .str.replace(r'\D', '', regex=True)
                                        .replace('', pd.NA)
                                        .astype(pd.Int64Dtype())
                                    )

                            st.session_state.df_view = df_view

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

            if st.button("🔄 Limpar e Voltar ao Início", use_container_width=True):
                clear_session_state()
                st.rerun()

    with tab2:
        if st.session_state.get('df_view') is not None:
            df_view = st.session_state.df_view

            st.header("Visualização de Dados")

            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric(label="Total de Linhas", value=len(df_view))
            with col2:
                if 'Vendor Name' in df_view.columns:
                    st.metric(label="Número de Fornecedores", value=df_view['Vendor Name'].nunique())
            with col3:
                if 'Purchasing Document' in df_view.columns:
                    st.metric(label="Número de PO'S", value=df_view['Purchasing Document'].nunique())

            st.dataframe(df_view, hide_index=True)
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
           - Explore as métricas e a tabela de dados

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
