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
# Ou rode o app assim, sem precisar do config.toml:
#
#   streamlit run po_processor_streamlit.py --server.maxUploadSize=550
#
# Sem uma dessas duas opções, o Streamlit vai barrar o upload em 200MB mesmo
# que o código aqui já esteja preparado para tamanhos maiores.
# =============================================================================
#
# =============================================================================
# ARQUITETURA DESTE SCRIPT (reescrito para consumir pouca memória)
# -----------------------------------------------------------------------------
# A versão anterior lia todos os arquivos com pandas, concatenava tudo em um
# único DataFrame gigante na memória e só então processava. Isso significa
# que o pico de memória era proporcional à soma de TODOS os arquivos juntos.
#
# Esta versão processa cada arquivo, um de cada vez, usando o openpyxl em
# modo "read_only" (leitura linha a linha, sem carregar a planilha inteira
# na memória) e grava o resultado direto em disco em modo "write_only"
# (também linha a linha, sem acumular o resultado inteiro na memória).
#
# Como o cálculo de totais por Pedido de Compra (PO) depende de somar todas
# as linhas daquele PO — que podem estar espalhadas em posições diferentes
# do arquivo, ou até em arquivos diferentes — não dá pra gerar o resultado
# final em uma única leitura sequencial. A solução é fazer 3 passadas curtas,
# cada uma lendo os arquivos um por vez (sem empilhar todos na RAM):
#
#   PASSADA 0 — Mapear colunas:
#       Lê só a linha de cabeçalho de cada arquivo (custo desprezível) para
#       montar a lista de TODAS as colunas que apareceram em qualquer
#       arquivo, na ordem em que foram vistas. Nenhuma coluna é descartada.
#
#   PASSADA 1 — Agregar totais por PO:
#       Percorre linha a linha cada arquivo (um de cada vez, fechando antes
#       de abrir o próximo) e acumula, em um dicionário pequeno (uma entrada
#       por PO, não por linha), os totais de valor líquido, valor com
#       impostos e quantidade. Esse dicionário é a única coisa "grande" que
#       fica na memória inteira do processamento — e ele é muito menor que
#       os dados brutos, pois tem 1 linha por PO, não 1 linha por item.
#
#   PASSADA 2 — Gravar o arquivo final:
#       Percorre novamente cada arquivo, linha a linha, calcula as colunas
#       derivadas (valor unitário, totais por PO já calculados na Passada 1,
#       código do projeto, datas formatadas, etc.) e grava CADA LINHA direto
#       no arquivo Excel de saída em disco (modo write_only do openpyxl, que
#       transmite cada linha para o arquivo assim que ela é adicionada, sem
#       guardar tudo em memória).
#
# TRADE-OFF ASSUMIDO (para caber em pouca RAM):
#   - Fazemos 3 leituras de cada arquivo em vez de 1. Isso custa mais tempo
#     de processamento (mais I/O), mas mantém o pico de memória baixo e
#     estável, independente do tamanho total dos arquivos.
#   - O arquivo final NÃO é mais ordenado por data de criação do PO (ordenar
#     exigiria ter todas as linhas em memória ao mesmo tempo, o que
#     contradiz o objetivo de economizar RAM). As linhas saem na mesma ordem
#     em que aparecem nos arquivos de origem. Se precisar ordenado, use
#     "Dados > Classificar" no Excel/Google Sheets depois de baixar — é
#     rápido porque o arquivo final já está pronto.
#   - Todas as colunas originais de cada arquivo são preservadas no arquivo
#     final (união das colunas de todos os arquivos enviados), além das
#     colunas calculadas (valor_unitario, totais por PO, código do projeto,
#     etc.), que são adicionadas ao final.
# =============================================================================

import streamlit as st
import pandas as pd
from datetime import datetime
import time
import os
import gc
import re
import tempfile
import logging
from typing import List, Optional, Any, Dict, Set, Tuple

from openpyxl import Workbook, load_workbook

# Desabilitar a exibição de separadores de milhar
pd.options.display.float_format = '{:,.0f}'.format
pd.options.display.max_columns = None

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Constantes
# -----------------------------------------------------------------------------
MAX_UPLOAD_SIZE_MB = 550
BYTES_PER_MB = 1024 * 1024
PREVIEW_ROWS = 200  # quantas linhas mostrar na aba de Visualização

# Colunas numéricas "de origem" (existem no arquivo enviado e são somente
# convertidas/limpas, não calculadas)
NUMERIC_SOURCE_COLUMNS = [
    'Order Quantity', 'Net order value', 'PBXX Condition Amount',
    'Price unit', 'Gross Price'
]

# Colunas de data "de origem" que devem ser reformatadas para dd/mm/aaaa
DATE_COLUMNS = [
    'Delivery date', 'Last FUP',
    'Stat.-Rel. Del. Date', 'Delivery Date',
    'Requisition Date', 'Inspection Request Date',
    'First Delivery Date', 'Purchase Requisition Delivery Date'
]

# Colunas de identificador que são normalizadas para inteiro
ID_COLUMNS = ['Purchasing Document', 'Item', 'Material']

# Colunas calculadas que são adicionadas ao final do arquivo, na ordem abaixo
# (qualquer uma que já exista como coluna original não é duplicada)
COMPUTED_COLUMNS_ORDER = [
    'total_itens_po',
    'valor_unitario',
    'valor_item_com_impostos',
    'total_valor_po_liquido',
    'total_valor_po_com_impostos',
    'valor_unitario_formatted',
    'valor_item_com_impostos_formatted',
    'Net order value_formatted',
    'total_valor_po_liquido_formatted',
    'total_valor_po_com_impostos_formatted',
    'PO Creation Date',
    'codigo_projeto',
    'unique',
]


# =============================================================================
# Funções utilitárias de conversão (operam em UM valor por vez, nunca em um
# DataFrame inteiro — por isso o custo de memória é sempre desprezível)
# =============================================================================

def extract_code(text: Optional[str]) -> str:
    """Extrai os 6 dígitos do padrão X-XX-XXXXXX-XXX-XXXX-XXX. Nunca lança exceção."""
    try:
        if not text or not isinstance(text, str):
            return ""
        pattern = r'[A-Z0-9]-[A-Z0-9]{2}-(\d{6})-\d{3}-\d{4}-\d{3}'
        match = re.search(pattern, text)
        return match.group(1) if match else ""
    except Exception as e:
        logger.warning(f"extract_code falhou para o valor '{text}': {e}")
        return ""


def format_currency(value: float) -> str:
    """Formata um número como moeda brasileira. Nunca lança exceção."""
    try:
        if value is None:
            return "R$ 0,00"
        if isinstance(value, float) and value != value:  # NaN
            return "R$ 0,00"
        value = float(value)
        integer_part = int(value)
        decimal_part = int(round((value - integer_part) * 100))
        formatted_integer = '{:,}'.format(integer_part).replace(',', '.')
        return f"R$ {formatted_integer},{decimal_part:02d}"
    except Exception as e:
        logger.warning(f"Error formatting currency value {value}: {str(e)}")
        return "R$ 0,00"


def safe_division(x: float, y: float) -> float:
    """Divisão segura, evita ZeroDivisionError."""
    try:
        return x / y if y else 0
    except Exception:
        return 0


def to_number(value: Any, default: float = 0.0) -> float:
    """Converte qualquer valor de célula para float, de forma tolerante."""
    try:
        if value is None:
            return default
        if isinstance(value, bool):
            return default
        if isinstance(value, (int, float)):
            if isinstance(value, float) and value != value:  # NaN
                return default
            return float(value)
        if isinstance(value, str):
            s = value.strip()
            if s == '':
                return default
            # Trata tanto "1234,56" (BR) quanto "1234.56" (US)
            if ',' in s and '.' in s:
                s = s.replace('.', '').replace(',', '.')
            elif ',' in s:
                s = s.replace(',', '.')
            return float(s)
        return default
    except Exception:
        return default


def parse_id(value: Any) -> Optional[int]:
    """
    Extrai um identificador inteiro de uma célula (Purchasing Document, Item,
    Material). Remove qualquer caractere não numérico. Retorna None se não
    houver dígitos.
    """
    try:
        if value is None:
            return None
        if isinstance(value, bool):
            return None
        if isinstance(value, int):
            return int(value)
        if isinstance(value, float):
            if value != value:  # NaN
                return None
            return int(value)
        s = str(value).strip()
        digits = re.sub(r'\D', '', s)
        if digits == '':
            return None
        return int(digits)
    except Exception:
        return None


def parse_date_value(value: Any) -> Optional[datetime]:
    """Converte um valor de célula (datetime nativo ou texto) em datetime. Tolerante a falhas."""
    try:
        if value is None or value == '':
            return None
        if isinstance(value, datetime):
            return value
        s = str(value).strip()
        if s == '':
            return None
        dt = pd.to_datetime(s, dayfirst=True, errors='coerce')
        if pd.isna(dt):
            return None
        return dt.to_pydatetime()
    except Exception:
        return None


def normalize_header(header_row: Tuple[Any, ...]) -> List[str]:
    """Normaliza a linha de cabeçalho (remove espaços nas pontas, trata células vazias)."""
    result = []
    for c in header_row:
        if c is None:
            result.append('')
        else:
            result.append(str(c).strip())
    return result


def build_col_index(header: List[str]) -> Dict[str, int]:
    """Mapa nome-da-coluna -> índice, ignorando colunas sem nome."""
    idx = {}
    for i, name in enumerate(header):
        if name and name not in idx:  # mantém a primeira ocorrência em caso de nome duplicado
            idx[name] = i
    return idx


def get_cell(row: Tuple[Any, ...], col_idx: Dict[str, int], name: str, default: Any = None) -> Any:
    i = col_idx.get(name)
    if i is None or i >= len(row):
        return default
    return row[i]


# =============================================================================
# PASSADA 0 — mapear todas as colunas presentes em todos os arquivos
# =============================================================================

def scan_header(uploaded_file: Any) -> List[str]:
    """Lê apenas a primeira linha do arquivo (custo desprezível de memória/tempo)."""
    uploaded_file.seek(0)
    wb = load_workbook(uploaded_file, read_only=True, data_only=True)
    try:
        ws = wb.worksheets[0]
        first_row = next(ws.iter_rows(min_row=1, max_row=1, values_only=True), ())
        return normalize_header(first_row)
    finally:
        wb.close()
        uploaded_file.seek(0)


def build_master_columns(uploaded_files: List[Any]) -> List[str]:
    """União de todas as colunas de todos os arquivos, na ordem em que aparecem."""
    master_columns: List[str] = []
    seen: Set[str] = set()
    for f in uploaded_files:
        header = scan_header(f)
        for col in header:
            if col and col not in seen:
                seen.add(col)
                master_columns.append(col)
    return master_columns


# =============================================================================
# PASSADA 1 — agregar totais por Pedido de Compra (PO), um arquivo de cada vez
# =============================================================================

def aggregate_file(uploaded_file: Any, po_totals: Dict[int, Dict[str, float]],
                    seen_keys: Set[Tuple[Optional[int], Optional[int]]]) -> int:
    """
    Percorre um arquivo linha a linha e acumula os totais por PO em `po_totals`.
    `seen_keys` evita contar a mesma linha (mesmo PO+Item) duas vezes, da
    mesma forma que o processamento original removia duplicatas antes de somar.
    Retorna o número de linhas válidas processadas (para métricas/log).
    """
    uploaded_file.seek(0)
    wb = load_workbook(uploaded_file, read_only=True, data_only=True)
    processed = 0
    try:
        ws = wb.worksheets[0]
        rows_iter = ws.iter_rows(values_only=True)
        header = normalize_header(next(rows_iter, ()))
        col_idx = build_col_index(header)

        for row in rows_iter:
            raw_po = get_cell(row, col_idx, 'Purchasing Document')
            # Replica o filtro original: descarta a linha se o Purchasing
            # Document veio como texto (ex: linhas de rodapé/total).
            if isinstance(raw_po, str):
                continue
            po_id = parse_id(raw_po)
            if po_id is None:
                continue

            item_id = parse_id(get_cell(row, col_idx, 'Item'))
            key = (po_id, item_id)
            if key in seen_keys:
                continue
            seen_keys.add(key)

            qty = to_number(get_cell(row, col_idx, 'Order Quantity', 0))
            net_value = to_number(get_cell(row, col_idx, 'Net order value', 0))
            pbxx = to_number(get_cell(row, col_idx, 'PBXX Condition Amount', 0))
            valor_item_com_impostos = pbxx * qty

            entry = po_totals.setdefault(po_id, {'net': 0.0, 'com_impostos': 0.0, 'qty': 0.0})
            entry['net'] += net_value
            entry['com_impostos'] += valor_item_com_impostos
            entry['qty'] += qty
            processed += 1
    finally:
        wb.close()
        uploaded_file.seek(0)
        gc.collect()
    return processed


# =============================================================================
# PASSADA 2 — gravar o arquivo final, linha a linha, direto em disco
# =============================================================================

def write_file_rows(uploaded_file: Any, ws_out: Any, master_columns: List[str],
                     final_header: List[str], po_totals: Dict[int, Dict[str, float]],
                     seen_keys: Set[Tuple[Optional[int], Optional[int]]],
                     vendor_names_seen: Set[str]) -> int:
    """
    Percorre um arquivo linha a linha, calcula as colunas derivadas e grava
    cada linha diretamente na planilha de saída (write_only), sem acumular
    o resultado em memória. Retorna o número de linhas gravadas.
    """
    uploaded_file.seek(0)
    wb = load_workbook(uploaded_file, read_only=True, data_only=True)
    written = 0
    try:
        ws = wb.worksheets[0]
        rows_iter = ws.iter_rows(values_only=True)
        header = normalize_header(next(rows_iter, ()))
        col_idx = build_col_index(header)

        for row in rows_iter:
            raw_po = get_cell(row, col_idx, 'Purchasing Document')
            if isinstance(raw_po, str):
                continue
            po_id = parse_id(raw_po)
            if po_id is None:
                continue

            item_id = parse_id(get_cell(row, col_idx, 'Item'))
            key = (po_id, item_id)
            if key in seen_keys:
                continue
            seen_keys.add(key)

            material_id = parse_id(get_cell(row, col_idx, 'Material'))

            qty = to_number(get_cell(row, col_idx, 'Order Quantity', 0))
            net_value = to_number(get_cell(row, col_idx, 'Net order value', 0))
            pbxx = to_number(get_cell(row, col_idx, 'PBXX Condition Amount', 0))
            valor_unitario = safe_division(net_value, qty)
            valor_item_com_impostos = pbxx * qty

            totals = po_totals.get(po_id, {'net': 0.0, 'com_impostos': 0.0, 'qty': 0.0})

            doc_date = parse_date_value(get_cell(row, col_idx, 'Document Date'))
            wbs_raw = get_cell(row, col_idx, 'Andritz WBS Element')
            codigo_projeto_str = extract_code(wbs_raw) if isinstance(wbs_raw, str) else ''
            codigo_projeto = int(codigo_projeto_str) if codigo_projeto_str else ''

            vendor_name = get_cell(row, col_idx, 'Vendor Name')
            if vendor_name:
                vendor_names_seen.add(str(vendor_name))

            # Monta a linha de saída com TODAS as colunas originais + calculadas
            out_row: Dict[str, Any] = {}
            for col in master_columns:
                val = get_cell(row, col_idx, col)
                if col in DATE_COLUMNS:
                    dt = parse_date_value(val)
                    val = dt.strftime('%d/%m/%Y') if dt else ''
                elif col in NUMERIC_SOURCE_COLUMNS:
                    val = to_number(val)
                elif col == 'Purchasing Document':
                    val = po_id
                elif col == 'Item':
                    val = item_id
                elif col == 'Material':
                    val = material_id
                out_row[col] = val

            out_row['total_itens_po'] = totals['qty']
            out_row['valor_unitario'] = valor_unitario
            out_row['valor_item_com_impostos'] = valor_item_com_impostos
            out_row['total_valor_po_liquido'] = totals['net']
            out_row['total_valor_po_com_impostos'] = totals['com_impostos']
            out_row['valor_unitario_formatted'] = format_currency(valor_unitario)
            out_row['valor_item_com_impostos_formatted'] = format_currency(valor_item_com_impostos)
            out_row['Net order value_formatted'] = format_currency(net_value)
            out_row['total_valor_po_liquido_formatted'] = format_currency(totals['net'])
            out_row['total_valor_po_com_impostos_formatted'] = format_currency(totals['com_impostos'])
            out_row['PO Creation Date'] = doc_date.strftime('%d/%m/%Y') if doc_date else ''
            out_row['codigo_projeto'] = codigo_projeto
            out_row['unique'] = f"{po_id}{item_id if item_id is not None else ''}"

            ws_out.append([out_row.get(col, '') for col in final_header])
            written += 1
    finally:
        wb.close()
        uploaded_file.seek(0)
        gc.collect()
    return written


# =============================================================================
# Leitura de uma prévia leve do arquivo final (para a aba de Visualização)
# =============================================================================

def read_preview_rows(path: str, n: int = PREVIEW_ROWS) -> pd.DataFrame:
    """Lê apenas as primeiras `n` linhas de dados do arquivo final gerado, sem
    carregar o arquivo inteiro — usado só para a prévia na tela."""
    wb = load_workbook(path, read_only=True, data_only=True)
    try:
        ws = wb.worksheets[0]
        rows_iter = ws.iter_rows(values_only=True)
        header = normalize_header(next(rows_iter, ()))
        preview_rows = []
        for i, row in enumerate(rows_iter):
            if i >= n:
                break
            preview_rows.append(row)
        return pd.DataFrame(preview_rows, columns=header)
    finally:
        wb.close()


# =============================================================================
# Orquestração do pipeline completo (as 3 passadas)
# =============================================================================

def process_files(uploaded_files: List[Any], progress_bar: Any, status_placeholder: Any) -> Dict[str, Any]:
    """
    Executa as 3 passadas sobre a lista de arquivos e grava o resultado em um
    arquivo temporário em disco. Retorna um dicionário com o caminho do
    arquivo final e as métricas calculadas.
    """
    n_files = len(uploaded_files)

    # --- Passada 0: mapear colunas -----------------------------------------
    status_placeholder.info("🔎 Etapa 1/3 — Mapeando colunas dos arquivos...")
    master_columns = build_master_columns(uploaded_files)
    final_header = master_columns + [c for c in COMPUTED_COLUMNS_ORDER if c not in master_columns]
    progress_bar.progress(0.05)

    # --- Passada 1: agregar totais por PO -----------------------------------
    po_totals: Dict[int, Dict[str, float]] = {}
    seen_keys_agg: Set[Tuple[Optional[int], Optional[int]]] = set()
    total_rows_agg = 0
    for idx, f in enumerate(uploaded_files):
        status_placeholder.info(
            f"➕ Etapa 2/3 — Calculando totais por PO... arquivo {idx + 1}/{n_files}: {f.name}"
        )
        total_rows_agg += aggregate_file(f, po_totals, seen_keys_agg)
        progress_bar.progress(0.05 + 0.45 * ((idx + 1) / n_files))
    del seen_keys_agg
    gc.collect()

    # --- Passada 2: gravar o arquivo final direto em disco -------------------
    out_fd, out_path = tempfile.mkstemp(suffix='.xlsx', prefix='po_processado_')
    os.close(out_fd)

    wb_out = Workbook(write_only=True)
    ws_out = wb_out.create_sheet('PO_Processado')
    ws_out.append(final_header)

    seen_keys_write: Set[Tuple[Optional[int], Optional[int]]] = set()
    vendor_names_seen: Set[str] = set()
    total_rows_written = 0
    for idx, f in enumerate(uploaded_files):
        status_placeholder.info(
            f"💾 Etapa 3/3 — Gerando arquivo final... arquivo {idx + 1}/{n_files}: {f.name}"
        )
        total_rows_written += write_file_rows(
            f, ws_out, master_columns, final_header, po_totals, seen_keys_write, vendor_names_seen
        )
        progress_bar.progress(0.5 + 0.45 * ((idx + 1) / n_files))

    wb_out.save(out_path)
    progress_bar.progress(1.0)

    return {
        'output_path': out_path,
        'total_rows': total_rows_written,
        'total_pos': len(po_totals),
        'total_vendors': len(vendor_names_seen),
        'total_columns': len(final_header),
    }


# =============================================================================
# Utilidades de arquivo / sessão
# =============================================================================

def calculate_total_size_mb(files: List[Any]) -> float:
    return sum(file.size for file in files) / BYTES_PER_MB


def cleanup_output_file():
    path = st.session_state.get('output_path')
    if path and os.path.exists(path):
        try:
            os.remove(path)
        except Exception as e:
            logger.warning(f"Não foi possível remover arquivo temporário {path}: {e}")


def clear_session_state():
    cleanup_output_file()
    for key in list(st.session_state.keys()):
        del st.session_state[key]
    gc.collect()


# =============================================================================
# Interface Streamlit
# =============================================================================

def main():
    st.set_page_config(
        page_title="Sistema de Processamento de PO",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="collapsed"
    )

    if 'initialized' not in st.session_state:
        st.session_state.initialized = True
        st.session_state.output_path = None
        st.session_state.download_filename = None
        st.session_state.metrics = None
        st.session_state.preview_df = None

    st.header("📑 Sistema de Processamento de Pedidos de Compra")
    st.caption(
        "Processamento em lote: cada arquivo é lido e liberado da memória um de "
        "cada vez, e o resultado é gravado em disco linha a linha — sem manter "
        "todos os dados na RAM de uma vez. Todas as colunas originais são preservadas."
    )
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
                total_size = calculate_total_size_mb(uploaded_files)
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
                cleanup_output_file()
                randon = datetime.now().strftime("%d%m%Y%H%M%S") + str(datetime.now().microsecond)[:3]

                with st.spinner("Processando arquivos..."):
                    progress_bar = st.progress(0)
                    status_placeholder = st.empty()
                    start_time = time.time()

                    try:
                        result = process_files(uploaded_files, progress_bar, status_placeholder)

                        if result['total_rows'] == 0:
                            st.warning("⚠️ O processamento não gerou nenhum registro válido.")
                            cleanup_output_file()
                        else:
                            st.session_state.output_path = result['output_path']
                            st.session_state.download_filename = f"PO_{randon}.xlsx"
                            st.session_state.metrics = result

                            status_placeholder.empty()
                            st.session_state.preview_df = read_preview_rows(result['output_path'])

                            elapsed_time = time.time() - start_time
                            st.success("✅ Processamento concluído com sucesso!")

                            m1, m2, m3, m4 = st.columns(4)
                            m1.metric("Tempo de processamento", f"{elapsed_time:.2f}s")
                            m2.metric("Arquivos processados", len(uploaded_files))
                            m3.metric("Registros processados", result['total_rows'])
                            m4.metric("PO's distintas", result['total_pos'])
                    except Exception as e:
                        logger.error(f"Falha inesperada no processamento: {str(e)}")
                        st.warning(
                            "⚠️ Ocorreu um problema durante o processamento e alguns dados "
                            "podem não ter sido incluídos. Verifique o resultado antes de usar."
                        )
                        cleanup_output_file()

                    gc.collect()

        if st.session_state.output_path and os.path.exists(st.session_state.output_path):
            st.subheader("📥 Download do Arquivo Processado")
            with open(st.session_state.output_path, 'rb') as fh:
                st.download_button(
                    label="📥 Baixar Arquivo Excel Processado",
                    data=fh.read(),
                    file_name=st.session_state.download_filename,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                    type="primary",
                )

            if st.button("🔄 Limpar e Voltar ao Início", use_container_width=True):
                clear_session_state()
                st.rerun()

    with tab2:
        if st.session_state.get('preview_df') is not None and st.session_state.get('metrics'):
            metrics = st.session_state.metrics
            preview_df = st.session_state.preview_df

            st.header("Visualização de Dados")

            c1, c2, c3, c4 = st.columns(4)
            c1.metric(label="Total de Linhas", value=metrics['total_rows'])
            c2.metric(label="Número de Fornecedores", value=metrics['total_vendors'])
            c3.metric(label="Número de PO'S", value=metrics['total_pos'])
            c4.metric(label="Total de Colunas", value=metrics['total_columns'])

            st.caption(
                f"Mostrando as primeiras {min(PREVIEW_ROWS, len(preview_df))} linhas do arquivo final "
                "(apenas para conferência — o arquivo baixado contém todos os registros)."
            )
            st.dataframe(preview_df, hide_index=True)
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

        2. **Processamento (em 3 etapas, por arquivo)**
           - Etapa 1: mapeia todas as colunas presentes nos arquivos
           - Etapa 2: calcula os totais por Pedido de Compra (PO)
           - Etapa 3: grava o arquivo final direto em disco, linha a linha
           - Cada arquivo é aberto, processado e liberado da memória antes do próximo
           - Colunas ausentes em um arquivo específico ficam em branco para aquele arquivo,
             sem interromper o processamento

        3. **Visualização**
           - Acesse a aba "Visualização de Dados"
           - Veja as métricas gerais e uma prévia das primeiras {PREVIEW_ROWS} linhas

        ### O que muda em relação à versão anterior
        - **Todas as colunas originais** de todos os arquivos são mantidas no resultado
          (união das colunas de cada arquivo), além das colunas calculadas.
        - O processamento não carrega todos os arquivos na memória de uma vez — cada
          arquivo é lido e liberado individualmente, em até 3 passagens leves.
        - O arquivo final **não vem mais ordenado por data** (ordenar exigiria manter
          tudo em memória). Se precisar, ordene no Excel/Sheets após o download —
          é rápido, pois o arquivo já está pronto.

        ### Dúvidas Frequentes
        1. **Tipos de arquivo aceitos?**
           - Apenas arquivos Excel (.xlsx)

        2. **Limite de tamanho?**
           - {MAX_UPLOAD_SIZE_MB}MB no total (ajustável em `.streamlit/config.toml`)

        3. **O que acontece se faltar uma coluna em um dos arquivos?**
           - A coluna aparece em branco apenas para as linhas daquele arquivo;
             o processamento continua normalmente para todos os arquivos.

        4. **Dados processados são salvos?**
           - O arquivo final fica em um arquivo temporário no servidor durante a sessão
             e é removido ao clicar em "Limpar e Voltar ao Início" (ou ao reiniciar a sessão).
        """)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Application error: {str(e)}")
        st.error("Ocorreu um erro inesperado. Por favor, tente novamente.")

    st.markdown("---")
    st.markdown(
        """
        <div style='text-align: center'>
            <p>Desenvolvido com ❤️ | PO Processor Pro v2.0 (processamento em lote)</p>
        </div>
        """,
        unsafe_allow_html=True
    )
