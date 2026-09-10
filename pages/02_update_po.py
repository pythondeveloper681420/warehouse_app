# =============================================================================
# IMPORTANTE SOBRE O LIMITE DE UPLOAD (500MB)
# -----------------------------------------------------------------------------
# O limite de tamanho de upload NÃO é controlado pelo código Python — ele é
# imposto pelo próprio servidor do Streamlit antes do seu script rodar.
# Para permitir uploads maiores (ex: 500MB), garanta que exista o arquivo
# ".streamlit/config.toml" na mesma pasta deste script com o conteúdo:
#
#   [server]
#   maxUploadSize = 550
#   maxMessageSize = 550
#
# Esse arquivo já foi gerado junto com este script. Se preferir, você também
# pode rodar o app assim, sem precisar do config.toml:
#
#   streamlit run po_processor_streamlit.py --server.maxUploadSize=550
#
# Sem uma dessas duas opções, o Streamlit vai barrar o upload em 200MB mesmo
# que o código aqui já esteja preparado para tamanhos maiores.
# =============================================================================

import pandas as pd
import streamlit as st
import time
from datetime import datetime
import io
import gc
import logging
from typing import List, Optional, Any, Dict
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
MAX_UPLOAD_SIZE_MB = 550  # alinhado ao config.toml (server.maxUploadSize)
BYTES_PER_MB = 1024 * 1024
CHUNK_SIZE = 10000

# -----------------------------------------------------------------------------
# Valores padrão para TODAS as colunas usadas no processamento.
# Se uma coluna não existir no arquivo enviado, ela é criada automaticamente
# com este valor padrão (e um aviso é registrado no log), então o
# processamento NUNCA quebra por causa de coluna ausente.
# -----------------------------------------------------------------------------
DEFAULT_COLUMN_VALUES: Dict[str, Any] = {
    # Identificadores — usar NaN (não string vazia) para não conflitar com o
    # filtro que remove linhas cujo 'Purchasing Document' é uma string.
    'Purchasing Document': np.nan,
    'Item': np.nan,

    # Numéricas
    'Order Quantity': 0,
    'Net order value': 0,
    'PBXX Condition Amount': 0,
    'Price unit': 0,
    'Gross Price': 0,

    # Texto
    'Supplier': '',
    'Vendor Name': '',
    'Material': '',
    'Material Description': '',
    'Order Unit': '',
    'Control Code (NCM)': '',
    'Project Code': '',
    'Andritz WBS Element': '',
    'Cost Center': '',
    'Document Date': '',
    'PO Created by': '',
    'Purchase Requisition': '',
    'PR Created by': '',
    'Purchasing Group': '',
    'Plant': '',
    'Delivery date': '',
    'Last FUP': '',
    'Stat.-Rel. Del. Date': '',
    'Delivery Date': '',
    'Requisition Date': '',
    'Inspection Request Date': '',
    'First Delivery Date': '',
    'Purchase Requisition Delivery Date': '',
}

NUMERIC_COLUMNS = ['Order Quantity', 'Net order value', 'PBXX Condition Amount', 'Price unit', 'Gross Price']

DATE_COLUMNS = [
    'Delivery date', 'Last FUP',
    'Stat.-Rel. Del. Date', 'Delivery Date',
    'Requisition Date', 'Inspection Request Date',
    'First Delivery Date', 'Purchase Requisition Delivery Date'
]

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
    Nunca lança exceção: em caso de entrada inesperada, retorna "".
    """
    try:
        if not text or not isinstance(text, str):
            return ""
        pattern = r'[A-Z0-9]-[A-Z0-9]{2}-(\d{6})-\d{3}-\d{4}-\d{3}'
        match = re.search(pattern, text)
        return match.group(1) if match else ""
    except Exception as e:
        logger.warning(f"extract_code falhou para o valor '{text}': {e}")
        return ""


class DataProcessor:
    """Class to handle all data processing operations"""

    @staticmethod
    def format_currency(value: float) -> str:
        """Format value as Brazilian currency. Nunca lança exceção."""
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
        except Exception:
            return 0

    @staticmethod
    def safe_int_id_convert(series: pd.Series) -> pd.Series:
        """
        Converte uma coluna para inteiro (removendo caracteres não numéricos).
        Se a conversão falhar por qualquer motivo, registra um aviso e
        devolve a coluna original sem quebrar o processamento.
        """
        try:
            return (
                series.astype(str)
                .str.replace(r'\D', '', regex=True)
                .replace('', pd.NA)
                .astype(pd.Int64Dtype())
            )
        except Exception as e:
            logger.warning(f"Não foi possível converter coluna para inteiro: {e}")
            return series

    @staticmethod
    def ensure_all_columns(df: pd.DataFrame) -> pd.DataFrame:
        """
        Garante que TODAS as colunas esperadas existam no DataFrame.
        Colunas ausentes são criadas com um valor padrão seguro e um aviso
        é registrado no log — o processamento nunca é interrompido por isso.
        """
        df = df.copy()
        for col, default_value in DEFAULT_COLUMN_VALUES.items():
            if col not in df.columns:
                logger.warning(
                    f"Coluna '{col}' não encontrada no arquivo enviado. "
                    f"Criando coluna com valor padrão para permitir o processamento."
                )
                df[col] = default_value

        for col in NUMERIC_COLUMNS:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        return df

    @staticmethod
    def process_chunk(df: pd.DataFrame) -> pd.DataFrame:
        """Process a chunk of data. Assume que ensure_all_columns já rodou."""
        try:
            chunk_processed = df.copy()

            # Segurança extra (dupla proteção), caso o chunk venha sem alguma coluna
            chunk_processed = DataProcessor.ensure_all_columns(chunk_processed)

            chunk_processed['valor_unitario'] = chunk_processed.apply(
                lambda row: DataProcessor.safe_division(row['Net order value'], row['Order Quantity']),
                axis=1
            )

            chunk_processed['valor_item_com_impostos'] = (
                chunk_processed['PBXX Condition Amount'] * chunk_processed['Order Quantity']
            )

            return chunk_processed

        except Exception as e:
            # Nunca propaga: em último caso, devolve o chunk com colunas zeradas
            logger.error(f"Error processing chunk (recuperando com valores padrão): {str(e)}")
            chunk_processed = df.copy()
            chunk_processed = DataProcessor.ensure_all_columns(chunk_processed)
            chunk_processed['valor_unitario'] = 0
            chunk_processed['valor_item_com_impostos'] = 0
            return chunk_processed

    @staticmethod
    def process_dataframe(df: pd.DataFrame, progress_bar: Any) -> pd.DataFrame:
        """Process the complete DataFrame with progress tracking. Tolerante a colunas ausentes."""
        try:
            # Garante todas as colunas ANTES de qualquer processamento
            df = DataProcessor.ensure_all_columns(df)

            chunk_size = CHUNK_SIZE
            num_chunks = max(1, len(df) // chunk_size + 1)
            processed_chunks = []

            for i in range(num_chunks):
                start_idx = i * chunk_size
                end_idx = min((i + 1) * chunk_size, len(df))
                chunk = df.iloc[start_idx:end_idx]
                if chunk.empty:
                    continue
                processed_chunk = DataProcessor.process_chunk(chunk)
                processed_chunks.append(processed_chunk)
                progress = (i + 1) / num_chunks
                progress_bar.progress(min(progress, 1.0))

            if not processed_chunks:
                return pd.DataFrame(columns=SELECTED_COLUMNS)

            df_processed = pd.concat(processed_chunks, ignore_index=True)

            # Eliminar linhas onde 'Purchasing Document' é string não numérica
            # (se a coluna não existir de verdade, ela já é NaN e não é filtrada)
            df_processed = df_processed[
                ~df_processed['Purchasing Document'].apply(lambda x: isinstance(x, str))
            ]

            df_processed['unique'] = (
                df_processed['Purchasing Document'].astype(str) +
                df_processed['Item'].astype(str)
            )

            df_processed['Supplier'] = df_processed['Supplier'].astype(str)

            df_processed = df_processed.drop_duplicates(subset=['unique'])

            if df_processed.empty:
                logger.warning("Nenhuma linha válida restou após a limpeza inicial.")
                return pd.DataFrame(columns=SELECTED_COLUMNS)

            groupby_cols = ['Purchasing Document']
            try:
                df_processed['total_valor_po_liquido'] = df_processed.groupby(groupby_cols)['Net order value'].transform('sum')
                df_processed['total_valor_po_com_impostos'] = df_processed.groupby(groupby_cols)['valor_item_com_impostos'].transform('sum')
                df_processed['total_itens_po'] = df_processed.groupby(groupby_cols)['Order Quantity'].transform('sum')
            except Exception as e:
                logger.warning(f"Falha ao agrupar por '{groupby_cols}', usando valores individuais: {e}")
                df_processed['total_valor_po_liquido'] = df_processed['Net order value']
                df_processed['total_valor_po_com_impostos'] = df_processed['valor_item_com_impostos']
                df_processed['total_itens_po'] = df_processed['Order Quantity']

            # Coluna de data
            try:
                df_processed['PO Creation Date'] = pd.to_datetime(
                    df_processed['Document Date'], dayfirst=True, errors='coerce'
                )
                df_processed = df_processed.sort_values(by='PO Creation Date', ascending=False)
            except Exception as e:
                logger.warning(f"Não foi possível processar 'Document Date': {e}")
                df_processed['PO Creation Date'] = pd.NaT

            currency_columns = [
                'valor_unitario', 'valor_item_com_impostos', 'Net order value',
                'total_valor_po_liquido', 'total_valor_po_com_impostos'
            ]
            for col in currency_columns:
                df_processed[f'{col}_formatted'] = df_processed[col].apply(DataProcessor.format_currency)

            for col in DATE_COLUMNS:
                try:
                    df_processed[col] = pd.to_datetime(
                        df_processed[col],
                        format='%d/%m/%Y',
                        dayfirst=True,
                        errors='coerce'
                    )
                    df_processed[col] = df_processed[col].dt.strftime('%d/%m/%Y')
                except Exception as e:
                    logger.warning(f"Não foi possível formatar a coluna de data '{col}': {e}")

            # codigo_projeto a partir de 'Andritz WBS Element'
            try:
                df_processed['codigo_projeto'] = df_processed['Andritz WBS Element'].apply(extract_code)
                df_processed['codigo_projeto'] = df_processed['codigo_projeto'].apply(
                    lambda x: int(x) if x not in ("", None) else ""
                )
            except Exception as e:
                logger.warning(f"Falha ao extrair 'codigo_projeto': {e}")
                df_processed['codigo_projeto'] = ""

            # Limpar colunas numéricas (IDs)
            for col in ['Purchasing Document', 'Item', 'Material']:
                df_processed[col] = DataProcessor.safe_int_id_convert(df_processed[col])

            # Selecionar apenas colunas existentes no DataFrame (garantidas por ensure_all_columns
            # + colunas calculadas), preenchendo qualquer uma que ainda falte por segurança.
            for col in SELECTED_COLUMNS:
                if col not in df_processed.columns:
                    df_processed[col] = ''
            df_processed = df_processed[SELECTED_COLUMNS]

            return df_processed

        except Exception as e:
            # Última rede de segurança: nunca deixa o processamento explodir.
            logger.error(f"Error in process_dataframe (retornando dataframe vazio): {str(e)}")
            return pd.DataFrame(columns=SELECTED_COLUMNS)


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
        """
        Lê um arquivo Excel com segurança. Nunca lança exceção — em caso de
        falha, registra o erro no log e retorna None (o arquivo é apenas
        ignorado, sem travar o processamento dos demais).
        """
        try:
            df = pd.read_excel(file, engine='openpyxl')
            # Normaliza os nomes das colunas (remove espaços nas pontas), o que
            # evita erros de "coluna não encontrada" por diferenças invisíveis
            # de formatação no cabeçalho do Excel.
            df.columns = [str(c).strip() for c in df.columns]
            return df
        except Exception as e:
            logger.error(f"Error reading file {getattr(file, 'name', 'desconhecido')}: {str(e)}")
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
                help=f"Você pode selecionar múltiplos arquivos Excel (.xlsx) — até {MAX_UPLOAD_SIZE_MB}MB no total"
            )

        with col2:
            if uploaded_files:
                total_size = FileHandler.calculate_total_size(uploaded_files)
                remaining_size = MAX_UPLOAD_SIZE_MB - total_size
                st.metric(label="📦 Espaço utilizado", value=f"{total_size:.1f}MB")
                st.metric(label="⚡ Espaço disponível", value=f"{remaining_size:.1f}MB")
                if remaining_size < 0:
                    st.warning(
                        "⚠️ O total enviado passou do limite configurado no servidor "
                        f"({MAX_UPLOAD_SIZE_MB}MB). Se o Streamlit não aceitar o upload, "
                        "aumente 'server.maxUploadSize' no config.toml."
                    )

        if uploaded_files:
            if st.button("🚀 Iniciar Processamento", use_container_width=True, type="primary"):
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
                        else:
                            st.warning(f"⚠️ Não foi possível ler ou o arquivo está vazio: {uploaded_file.name}")
                        progress_bar.progress((idx + 1) / len(uploaded_files))

                    if all_dfs:
                        try:
                            df_final = pd.concat(all_dfs, ignore_index=True)
                            del all_dfs
                            gc.collect()

                            df_processed = DataProcessor.process_dataframe(df_final, progress_bar)
                            del df_final
                            gc.collect()

                            if df_processed.empty:
                                st.warning("⚠️ O processamento não gerou nenhum registro válido.")
                            else:
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

                                if 'Purchasing Document' in df_view.columns and 'Item' in df_view.columns:
                                    df_view['unique'] = (
                                        df_view['Purchasing Document'].astype(str) +
                                        df_view['Item'].astype(str)
                                    )
                                    df_view = df_view.drop_duplicates(subset=['unique'])

                                for col in ['unique', 'Purchasing Document', 'Item', 'Material']:
                                    if col in df_view.columns:
                                        df_view[col] = DataProcessor.safe_int_id_convert(df_view[col])

                                st.session_state.df_view = df_view

                                elapsed_time = time.time() - start_time
                                st.success("✅ Processamento concluído com sucesso!")

                                col1, col2, col3 = st.columns(3)
                                col1.metric("Tempo de processamento", f"{elapsed_time:.2f}s")
                                col2.metric("Arquivos processados", len(uploaded_files))
                                col3.metric("Registros processados", len(df_processed))
                        except Exception as e:
                            # Rede de segurança final: mostra um aviso, não um erro travante.
                            logger.error(f"Falha inesperada no processamento: {str(e)}")
                            st.warning(
                                "⚠️ Ocorreu um problema durante o processamento e alguns dados "
                                "podem não ter sido incluídos. Verifique o resultado antes de usar."
                            )
                    else:
                        st.warning("⚠️ Nenhum dado encontrado para processar!")

                    gc.collect()

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
        st.markdown(f"""
        ### Como usar o Sistema de Processamento de PO

        1. **Upload de Arquivos**
           - Acesse a aba "Upload e Extração"
           - Selecione um ou mais arquivos Excel (.xlsx)
           - O sistema aceita arquivos até {MAX_UPLOAD_SIZE_MB}MB no total (requer config.toml ajustado)

        2. **Processamento**
           - Clique em "Iniciar Processamento"
           - Aguarde o processamento ser concluído
           - Faça o download do arquivo processado
           - Colunas ausentes no arquivo original são preenchidas automaticamente
             com valores padrão, sem interromper o processamento

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
           - {MAX_UPLOAD_SIZE_MB}MB no total (ajustável em `.streamlit/config.toml`)

        3. **O que acontece se faltar uma coluna no arquivo?**
           - O sistema cria a coluna automaticamente com um valor padrão e
             continua o processamento normalmente, sem travar

        4. **Dados processados são salvos?**
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
