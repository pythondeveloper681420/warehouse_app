# =============================================================================
# IMPORTANTE SOBRE O LIMITE DE UPLOAD (500MB)
# -----------------------------------------------------------------------------
# O limite de tamanho de upload NÃO é controlado pelo código Python — ele é
# imposto pelo próprio servidor do Streamlit antes do seu script rodar.
# Para permitir uploads maiores (ex: 500MB), garanta que exista o arquivo
# ".streamlit/config.toml" na MESMA PASTA deste script com o conteúdo:
#
#   [server]
#   maxUploadSize = 550
#   maxMessageSize = 550
#
# Ou rode o app assim, sem precisar do config.toml:
#
#   streamlit run po_processor_streamlit.py --server.maxUploadSize=550
#
# Se o uploader na tela mostrar "Limit 200MB per file", o config.toml NÃO
# está sendo lido (pasta errada, ou o processo não foi reiniciado depois de
# criar o arquivo). Sem isso, o Streamlit barra o upload em 200MB mesmo que
# o código aqui já esteja preparado para tamanhos maiores.
# =============================================================================
#
# =============================================================================
# ARQUITETURA DESTE SCRIPT (memória baixa + isolamento de falhas por arquivo)
# -----------------------------------------------------------------------------
# Esta versão processa cada arquivo, um de cada vez, usando openpyxl em modo
# "read_only" (leitura linha a linha) e grava o resultado direto em disco em
# modo "write_only" (também linha a linha) — sem nunca carregar tudo na RAM.
#
# Isolamento de falhas por arquivo:
#   - Cada etapa (mapear colunas / agregar totais / gravar linhas) roda por
#     arquivo dentro de um try/except próprio.
#   - Se um arquivo falhar, ele é marcado como "com erro", pulado, e o
#     processamento CONTINUA normalmente para os demais arquivos do lote.
#   - Todos os erros (arquivo, etapa, mensagem, traceback) são guardados em
#     st.session_state.file_diagnostics e exibidos na aba "🛠️ Diagnóstico",
#     junto com avisos de colunas obrigatórias ausentes por arquivo.
#
# NOVIDADE NESTA VERSÃO: ordem FIXA de colunas no arquivo final.
#   - Em vez de simplesmente unir as colunas na ordem em que aparecem nos
#     arquivos, o arquivo final agora segue a ordem definida em
#     BASE_COLUMN_ORDER (colunas de origem) + COMPUTED_COLUMNS_ORDER
#     (colunas calculadas), sempre nessa ordem.
#   - Se algum arquivo do lote NÃO tiver uma dessas colunas, ela ainda
#     aparece no resultado final, só que em branco para as linhas daquele
#     arquivo.
#   - Se algum arquivo tiver uma coluna com nome DIFERENTE de tudo que está
#     na ordem padrão (uma coluna "extra"/desconhecida), ela é preservada e
#     posicionada logo ANTES da coluna 'unique' (que continua sendo sempre
#     a última coluna do arquivo).
#
# Continuam válidas as 3 passadas (mapear colunas -> agregar totais por PO
# -> gravar arquivo final linha a linha) e o trade-off de não ordenar por
# data (ordene no Excel/Sheets depois de baixar, se precisar).
# =============================================================================

import streamlit as st
import pandas as pd
from datetime import datetime
import time
import os
import gc
import re
import traceback
import tempfile
import zipfile
import logging
from typing import List, Optional, Any, Dict, Set, Tuple

from openpyxl import Workbook, load_workbook
from openpyxl.utils.exceptions import InvalidFileException

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

# Quantas linhas no topo de cada arquivo podem ser escaneadas em busca do
# cabeçalho real, caso as primeiras linhas estejam em branco ou sejam só
# título/logo (comum em relatórios exportados do SAP GUI).
HEADER_SCAN_MAX_ROWS = 20

# =============================================================================
# ORDEM FIXA DAS COLUNAS DE ORIGEM (SAP) NO ARQUIVO FINAL
# -----------------------------------------------------------------------------
# Essas colunas SEMPRE aparecem no arquivo final, nesta ordem exata — mesmo
# que um arquivo específico não as tenha (nesse caso, ficam em branco para
# as linhas daquele arquivo).
# =============================================================================
BASE_COLUMN_ORDER: List[str] = [
    'Purchasing Document',
    'Item',
    'Vendor Name',
    'Material Description',
    'Status',
    'Comment',
    'Last FUP',
    'Internal Contact',
    'Estimated Arrival',
    'Delivery date',
    'Stat.-Rel. Del. Date',
    'Warn',
    'Next FUP',
    'Invoice #',
    'First Delivery Date',
    'Delta PR Delivery',
    'Delta PR Stat',
    'Project Code',
    'Andritz WBS Element',
    'Document Date',
    'PO Created by',
    'Material',
    'Order Unit',
    'Order Quantity',
    'Total GR Quantity',
    'Quantity to be delivered',
    'PO Created by ZP (partner role)',
    'Value to be delivered',
    'Purchase Requisition',
    'PR Created by',
    'Net order value',
    'Requisition Date',
    'Purchase Requisition Delivery Date',
    'Vendor',
    'Country',
    'Incoterms',
    'Incoterms (Part 2)',
    'Currency',
    'PBXX Condition Amount',
    'Max. GR Document Date',
    'Gross Price',
    'Price unit',
    'Order Price Unit',
    'Terms of Payment',
    'Inspection Plan',
    'Responsible',
    'Cost Center',
    'Region',
    'City',
    'Pendente desde',
    'Data atual',
    'Dias pendentes',
    'Notification Responsible',
    'Pending since',
    'Suggested Responsible',
    'Control Code (NCM)',
    'Purchasing Group',
    'Storage location',
    'Atraso',
    'Grau de Criticidade',
    'Tipo de Acompanhamento',
    'Motivo de Atraso',
    'Quantity Progress',
    'Value to be invoiced',
    'Supplier',
    'Country/Region Key',
    'PBXX Amount',
    'Payment terms',
    'Plant',
    'Inspection Request Date',
    'Inspection Included',
    'Inspection Step',
    'Inspection Requisition Date',
    'Inspection Done',
    'Inspection Result',
    'Inspection Needed Days in Advance',
    'Acct Assignment Cat.',
    'Item Category',
    'Tax Code',
]

# Nomes de coluna conhecidos, usados só para RECONHECER com confiança qual
# linha é o cabeçalho de verdade quando ele não está na linha 1.
KNOWN_COLUMN_HINTS = (
    set(BASE_COLUMN_ORDER) | set(NUMERIC_SOURCE_COLUMNS) | set(DATE_COLUMNS) | set(ID_COLUMNS)
)

# Colunas que PRECISAM existir no cabeçalho de cada arquivo para que os
# cálculos façam sentido. Se faltarem, o arquivo ainda é processado (para
# não travar o lote), mas um AVISO é registrado no diagnóstico, pois os
# valores derivados daquela coluna sairão zerados/vazios para aquele arquivo.
REQUIRED_COLUMNS = ['Purchasing Document', 'Item', 'Order Quantity', 'Net order value']

# Colunas calculadas, na ordem em que devem aparecer ao final do arquivo
# (sempre depois de BASE_COLUMN_ORDER e de eventuais colunas "extras").
# 'unique' é, por definição, sempre a ÚLTIMA coluna do arquivo final.
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


def friendly_open_error(e: Exception) -> str:
    """Traduz as exceções mais comuns do openpyxl/zipfile em mensagens úteis."""
    if isinstance(e, zipfile.BadZipFile):
        return (
            "O arquivo não é um .xlsx válido (zip corrompido). Isso costuma acontecer "
            "quando o arquivo é na verdade um .xls antigo, um HTML/SYLK exportado pelo "
            "SAP e apenas renomeado para .xlsx, ou o download foi interrompido/corrompido. "
            "Abra o arquivo no Excel e use 'Salvar como > Excel Workbook (.xlsx)' para regravá-lo."
        )
    if isinstance(e, InvalidFileException):
        return (
            "O openpyxl não reconheceu o formato do arquivo. Verifique se ele não está "
            "protegido por senha (arquivos criptografados não podem ser abertos "
            "diretamente) e se a extensão .xlsx corresponde ao conteúdo real do arquivo."
        )
    return str(e)


# =============================================================================
# PASSADA 0 — mapear todas as colunas presentes em todos os arquivos
# =============================================================================

def locate_header_and_rows(ws: Any) -> Tuple[List[str], Any, int]:
    """
    Encontra a linha de cabeçalho real dentro das primeiras
    HEADER_SCAN_MAX_ROWS linhas do arquivo, mesmo que existam linhas em
    branco ou de título/logo acima dela (comum em exports do SAP GUI).

    Estratégia: dá uma pontuação a cada linha não totalmente vazia dentro da
    janela de escaneamento, contando quantas células batem com nomes de
    coluna conhecidos (KNOWN_COLUMN_HINTS). A linha com maior pontuação (>=2
    matches) vence. Se nenhuma linha bater com nomes conhecidos, usa como
    reserva a primeira linha não vazia com pelo menos 3 células preenchidas
    (evita confundir uma linha de título de 1 célula com o cabeçalho real).

    Retorna (header_normalizado, gerador_das_linhas_de_dados_restantes,
    índice_da_linha_de_cabeçalho). Só a janela de escaneamento (no máximo
    HEADER_SCAN_MAX_ROWS linhas) fica em memória — o resto do arquivo
    continua sendo lido em streaming, um registro de cada vez.
    """
    rows_iter = ws.iter_rows(values_only=True)
    scanned: List[Tuple[Any, ...]] = []
    best_idx: Optional[int] = None
    best_score = -1
    fallback_idx: Optional[int] = None

    for i, row in enumerate(rows_iter):
        scanned.append(row)
        non_empty = [c for c in row if c is not None and str(c).strip() != '']
        if non_empty:
            score = sum(1 for c in non_empty if str(c).strip() in KNOWN_COLUMN_HINTS)
            if score > best_score:
                best_score = score
                best_idx = i
            if fallback_idx is None and len(non_empty) >= 3:
                fallback_idx = i
        if i >= HEADER_SCAN_MAX_ROWS - 1:
            break

    if best_score >= 2:
        header_idx = best_idx
    elif fallback_idx is not None:
        header_idx = fallback_idx
    elif best_idx is not None:
        header_idx = best_idx
    else:
        header_idx = None

    if header_idx is None:
        return [], iter(()), -1

    header = normalize_header(scanned[header_idx])
    remaining_buffered = scanned[header_idx + 1:]

    def _chained_rows():
        for r in remaining_buffered:
            yield r
        for r in rows_iter:
            yield r

    return header, _chained_rows(), header_idx


def scan_header(uploaded_file: Any) -> Tuple[List[str], int]:
    """Lê só a janela inicial do arquivo para localizar o cabeçalho real
    (custo desprezível de memória/tempo). Retorna (cabeçalho, índice_da_linha)."""
    uploaded_file.seek(0)
    wb = load_workbook(uploaded_file, read_only=True, data_only=True)
    try:
        ws = wb.worksheets[0]
        header, _, header_idx = locate_header_and_rows(ws)
        return header, header_idx
    finally:
        wb.close()
        uploaded_file.seek(0)


def build_master_columns(
    uploaded_files: List[Any], diagnostics: List[Dict[str, str]]
) -> Tuple[List[str], List[Any]]:
    """
    União de todas as colunas de todos os arquivos válidos, na ordem em que
    aparecem (essa ordem "de aparição" só é usada depois para detectar quais
    colunas são "extras", isto é, não fazem parte da ordem padrão fixa —
    veja BASE_COLUMN_ORDER e COMPUTED_COLUMNS_ORDER). Arquivos que falharem
    ao ter o cabeçalho lido são registrados em `diagnostics` e EXCLUÍDOS de
    `valid_files` (que é retornado junto), para que as etapas seguintes nem
    tentem reabri-los.
    """
    master_columns: List[str] = []
    seen: Set[str] = set()
    valid_files: List[Any] = []

    for f in uploaded_files:
        try:
            header, header_idx = scan_header(f)
        except Exception as e:
            logger.exception(f"Falha ao ler cabeçalho de {f.name}")
            diagnostics.append({
                'arquivo': f.name,
                'etapa': 'Etapa 1 — Mapeamento de colunas',
                'tipo': 'erro',
                'mensagem': friendly_open_error(e),
            })
            continue

        if header_idx == -1 or not any(header):
            diagnostics.append({
                'arquivo': f.name,
                'etapa': 'Etapa 1 — Mapeamento de colunas',
                'tipo': 'erro',
                'mensagem': (
                    f"Não foi possível localizar uma linha de cabeçalho nas "
                    f"primeiras {HEADER_SCAN_MAX_ROWS} linhas do arquivo (todas "
                    "vazias, ou nenhuma parece uma linha de colunas de verdade). "
                    "Verifique se o cabeçalho existe e não está mais abaixo do "
                    "que o esperado."
                ),
            })
            continue

        if header_idx > 0:
            diagnostics.append({
                'arquivo': f.name,
                'etapa': 'Etapa 1 — Mapeamento de colunas',
                'tipo': 'aviso',
                'mensagem': (
                    f"O cabeçalho não estava na linha 1 — foram encontradas "
                    f"{header_idx} linha(s) em branco/título acima dele, que "
                    "foram ignoradas automaticamente. O cabeçalho real foi "
                    f"localizado na linha {header_idx + 1} da planilha."
                ),
            })

        faltando = [c for c in REQUIRED_COLUMNS if c not in header]
        if faltando:
            diagnostics.append({
                'arquivo': f.name,
                'etapa': 'Etapa 1 — Mapeamento de colunas',
                'tipo': 'aviso',
                'mensagem': (
                    "Colunas obrigatórias ausentes neste arquivo: "
                    f"{', '.join(faltando)}. As linhas deste arquivo ainda serão "
                    "processadas, mas os totais/valores derivados dessas colunas "
                    "sairão zerados ou vazios para ele."
                ),
            })

        valid_files.append(f)
        for col in header:
            if col and col not in seen:
                seen.add(col)
                master_columns.append(col)

    return master_columns, valid_files


def build_final_header(master_columns: List[str]) -> Tuple[List[str], List[str]]:
    """
    Monta o cabeçalho final do arquivo de saída, respeitando a ordem FIXA
    pedida:

        BASE_COLUMN_ORDER (sempre, nessa ordem, mesmo colunas ausentes
        em todos os arquivos, que saem em branco)
        + COMPUTED_COLUMNS_ORDER, exceto 'unique'
        + colunas "extras" (qualquer coluna encontrada em algum arquivo que
          não faça parte de BASE_COLUMN_ORDER nem de COMPUTED_COLUMNS_ORDER),
          na ordem em que foram encontradas
        + 'unique' (sempre a última coluna)

    Retorna (final_header, extra_columns) — extra_columns é devolvido só
    para fins de diagnóstico/log.
    """
    known_cols = set(BASE_COLUMN_ORDER) | set(COMPUTED_COLUMNS_ORDER)
    extra_columns = [c for c in master_columns if c not in known_cols]
    computed_sem_unique = [c for c in COMPUTED_COLUMNS_ORDER if c != 'unique']

    final_header = BASE_COLUMN_ORDER + computed_sem_unique + extra_columns + ['unique']
    return final_header, extra_columns


# =============================================================================
# PASSADA 1 — agregar totais por Pedido de Compra (PO), um arquivo de cada vez
# =============================================================================

def aggregate_file(uploaded_file: Any, po_totals: Dict[int, Dict[str, float]],
                    seen_keys: Set[Tuple[Optional[int], Optional[int]]]) -> int:
    """
    Percorre um arquivo linha a linha e acumula os totais por PO em `po_totals`.
    `seen_keys` evita contar a mesma linha (mesmo PO+Item) duas vezes.
    Retorna o número de linhas válidas processadas (para métricas/log).
    Pode lançar exceção — o chamador (process_files) é responsável por
    isolar a falha por arquivo.
    """
    uploaded_file.seek(0)
    wb = load_workbook(uploaded_file, read_only=True, data_only=True)
    processed = 0
    try:
        ws = wb.worksheets[0]
        header, rows_iter, header_idx = locate_header_and_rows(ws)
        if header_idx == -1:
            return 0
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

def write_file_rows(uploaded_file: Any, ws_out: Any, final_header: List[str],
                     po_totals: Dict[int, Dict[str, float]],
                     seen_keys: Set[Tuple[Optional[int], Optional[int]]],
                     vendor_names_seen: Set[str]) -> int:
    """
    Percorre um arquivo linha a linha, calcula as colunas derivadas e grava
    cada linha diretamente na planilha de saída (write_only), sem acumular
    o resultado em memória. Retorna o número de linhas gravadas.

    A linha gravada segue exatamente `final_header`: qualquer coluna que
    não exista neste arquivo específico (ex: colunas de BASE_COLUMN_ORDER
    ausentes, ou colunas "extras" vindas de outro arquivo do lote) sai em
    branco para essas linhas — nunca desalinha ou encurta a linha.

    Pode lançar exceção — o chamador (process_files) é responsável por
    isolar a falha por arquivo.
    """
    uploaded_file.seek(0)
    wb = load_workbook(uploaded_file, read_only=True, data_only=True)
    written = 0
    try:
        ws = wb.worksheets[0]
        header, rows_iter, header_idx = locate_header_and_rows(ws)
        if header_idx == -1:
            return 0
        col_idx = build_col_index(header)

        # Só processamos colunas que este arquivo de fato possui — mas a
        # linha final sempre é montada respeitando `final_header` por
        # completo (colunas ausentes ficam em branco via out_row.get).
        file_columns = [c for c in header if c]

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

            # Monta a linha de saída com TODAS as colunas deste arquivo
            # (origem) + calculadas. Colunas de BASE_COLUMN_ORDER que este
            # arquivo não tem simplesmente não entram aqui, e por isso saem
            # em branco na hora do append final (out_row.get(col, '')).
            out_row: Dict[str, Any] = {}
            for col in file_columns:
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
                # IMPORTANTE: nunca deixar None aqui, senão a linha gravada
                # fica mais curta/desalinhada em relação ao cabeçalho final.
                if val is None:
                    val = ''
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
        n_cols = len(header)
        preview_rows = []
        for i, row in enumerate(rows_iter):
            if i >= n:
                break
            # Defensa extra: nunca deixar uma linha mais curta/longa que o
            # cabeçalho quebrar o preview — completa com '' ou corta o excesso.
            row = list(row)
            if len(row) < n_cols:
                row = row + [''] * (n_cols - len(row))
            elif len(row) > n_cols:
                row = row[:n_cols]
            preview_rows.append(row)
        return pd.DataFrame(preview_rows, columns=header)
    finally:
        wb.close()


# =============================================================================
# Orquestração do pipeline completo (as 3 passadas, com isolamento de falhas)
# =============================================================================

def process_files(uploaded_files: List[Any], progress_bar: Any, status_placeholder: Any) -> Dict[str, Any]:
    """
    Executa as 3 passadas sobre a lista de arquivos e grava o resultado em um
    arquivo temporário em disco. Retorna um dicionário com o caminho do
    arquivo final, as métricas calculadas e a lista de diagnósticos
    (erros/avisos por arquivo) encontrados ao longo do processamento.
    """
    diagnostics: List[Dict[str, str]] = []
    n_files = len(uploaded_files)

    # --- Passada 0: mapear colunas -----------------------------------------
    status_placeholder.info("🔎 Etapa 1/3 — Mapeando colunas dos arquivos...")
    master_columns, valid_files = build_master_columns(uploaded_files, diagnostics)
    final_header, extra_columns = build_final_header(master_columns)

    if extra_columns:
        diagnostics.append({
            'arquivo': '(vários arquivos)',
            'etapa': 'Etapa 1 — Mapeamento de colunas',
            'tipo': 'aviso',
            'mensagem': (
                "Colunas encontradas nos arquivos que não fazem parte da ordem "
                f"padrão foram preservadas e posicionadas antes de 'unique': "
                f"{', '.join(extra_columns)}."
            ),
        })

    progress_bar.progress(0.05)

    if not valid_files:
        return {
            'output_path': None,
            'total_rows': 0,
            'total_pos': 0,
            'total_vendors': 0,
            'total_columns': 0,
            'diagnostics': diagnostics,
            'files_ok': 0,
            'files_total': n_files,
        }

    # --- Passada 1: agregar totais por PO -----------------------------------
    po_totals: Dict[int, Dict[str, float]] = {}
    seen_keys_agg: Set[Tuple[Optional[int], Optional[int]]] = set()
    files_ok_agg: List[Any] = []
    for idx, f in enumerate(valid_files):
        status_placeholder.info(
            f"➕ Etapa 2/3 — Calculando totais por PO... arquivo {idx + 1}/{len(valid_files)}: {f.name}"
        )
        try:
            aggregate_file(f, po_totals, seen_keys_agg)
            files_ok_agg.append(f)
        except Exception as e:
            logger.exception(f"Falha ao agregar totais do arquivo {f.name}")
            diagnostics.append({
                'arquivo': f.name,
                'etapa': 'Etapa 2 — Cálculo de totais por PO',
                'tipo': 'erro',
                'mensagem': friendly_open_error(e),
            })
        progress_bar.progress(0.05 + 0.45 * ((idx + 1) / len(valid_files)))
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
    files_ok_write = 0
    # Só tenta gravar arquivos que sobreviveram à etapa de agregação, para
    # manter os totais por PO consistentes com as linhas gravadas.
    for idx, f in enumerate(files_ok_agg):
        status_placeholder.info(
            f"💾 Etapa 3/3 — Gerando arquivo final... arquivo {idx + 1}/{len(files_ok_agg)}: {f.name}"
        )
        try:
            total_rows_written += write_file_rows(
                f, ws_out, final_header, po_totals, seen_keys_write, vendor_names_seen
            )
            files_ok_write += 1
        except Exception as e:
            logger.exception(f"Falha ao gravar linhas do arquivo {f.name}")
            diagnostics.append({
                'arquivo': f.name,
                'etapa': 'Etapa 3 — Gravação do arquivo final',
                'tipo': 'erro',
                'mensagem': friendly_open_error(e),
            })
        progress_bar.progress(0.5 + 0.45 * ((idx + 1) / max(len(files_ok_agg), 1)))

    wb_out.save(out_path)
    progress_bar.progress(1.0)

    return {
        'output_path': out_path,
        'total_rows': total_rows_written,
        'total_pos': len(po_totals),
        'total_vendors': len(vendor_names_seen),
        'total_columns': len(final_header),
        'diagnostics': diagnostics,
        'files_ok': files_ok_write,
        'files_total': n_files,
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
        st.session_state.file_diagnostics = []
        st.session_state.last_exception = None

    st.header("📑 Sistema de Processamento de Pedidos de Compra")
    st.caption(
        "Processamento em lote: cada arquivo é lido e liberado da memória um de "
        "cada vez, e o resultado é gravado em disco linha a linha — sem manter "
        "todos os dados na RAM de uma vez. O arquivo final segue sempre a mesma "
        "ordem fixa de colunas; colunas ausentes em algum arquivo saem em branco, "
        "e colunas extras/desconhecidas aparecem logo antes de 'unique'. "
        "Se um arquivo específico tiver problema, ele é isolado e reportado, sem "
        "derrubar o processamento dos demais."
    )
    tab1, tab2, tab3, tab4 = st.tabs([
        "📤 Upload e Extração", "📊 Visualização de Dados", "🛠️ Diagnóstico", "❓ Como Utilizar"
    ])

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
                        st.session_state.file_diagnostics = result.get('diagnostics', [])

                        if result['total_rows'] == 0:
                            st.error(
                                "❌ O processamento não gerou nenhum registro válido. "
                                "Veja os detalhes na aba '🛠️ Diagnóstico'."
                            )
                            cleanup_output_file()
                        else:
                            st.session_state.output_path = result['output_path']
                            st.session_state.download_filename = f"PO_{randon}.xlsx"
                            st.session_state.metrics = result

                            status_placeholder.empty()
                            st.session_state.preview_df = read_preview_rows(result['output_path'])

                            elapsed_time = time.time() - start_time

                            if result['files_ok'] < result['files_total']:
                                st.warning(
                                    f"⚠️ Processamento concluído, mas {result['files_total'] - result['files_ok']} "
                                    f"de {result['files_total']} arquivo(s) NÃO foram incluídos por erro. "
                                    "Veja a aba '🛠️ Diagnóstico' para saber qual arquivo e o motivo."
                                )
                            elif result.get('diagnostics'):
                                st.success(
                                    "✅ Processamento concluído com sucesso, com alguns avisos "
                                    "(veja a aba '🛠️ Diagnóstico')."
                                )
                            else:
                                st.success("✅ Processamento concluído com sucesso, 100% dos arquivos incluídos!")

                            m1, m2, m3, m4 = st.columns(4)
                            m1.metric("Tempo de processamento", f"{elapsed_time:.2f}s")
                            m2.metric("Arquivos incluídos", f"{result['files_ok']}/{result['files_total']}")
                            m3.metric("Registros processados", result['total_rows'])
                            m4.metric("PO's distintas", result['total_pos'])
                    except Exception as e:
                        # Falha realmente inesperada (fora do isolamento por arquivo).
                        # Agora mostramos o motivo de verdade, em vez de esconder.
                        logger.error(f"Falha inesperada no processamento: {str(e)}")
                        st.session_state.last_exception = traceback.format_exc()
                        st.error(
                            "❌ Ocorreu um erro inesperado e o processamento foi interrompido. "
                            "Detalhes técnicos na aba '🛠️ Diagnóstico'."
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
        st.subheader("🛠️ Diagnóstico do último processamento")
        diags = st.session_state.get('file_diagnostics') or []
        last_exc = st.session_state.get('last_exception')

        if not diags and not last_exc:
            st.info(
                "Nenhum erro ou aviso registrado ainda. Depois de rodar o processamento, "
                "qualquer arquivo com problema (corrompido, protegido por senha, coluna "
                "obrigatória ausente, coluna extra fora da ordem padrão, etc.) vai "
                "aparecer detalhado aqui."
            )
        else:
            if diags:
                erros = [d for d in diags if d['tipo'] == 'erro']
                avisos = [d for d in diags if d['tipo'] == 'aviso']

                if erros:
                    st.markdown(f"#### ❌ Erros ({len(erros)}) — arquivo(s) excluído(s) do resultado")
                    for d in erros:
                        with st.expander(f"{d['arquivo']} — {d['etapa']}"):
                            st.write(d['mensagem'])

                if avisos:
                    st.markdown(f"#### ⚠️ Avisos ({len(avisos)}) — arquivo processado mesmo assim")
                    for d in avisos:
                        with st.expander(f"{d['arquivo']} — {d['etapa']}"):
                            st.write(d['mensagem'])

            if last_exc:
                st.markdown("#### 💥 Erro inesperado (interrompeu todo o processamento)")
                st.code(last_exc, language="text")

    with tab4:
        st.subheader("📖 Guia de Utilização")
        st.markdown(f"""
        ### Como usar o Sistema de Processamento de PO

        1. **Upload de Arquivos**
           - Acesse a aba "Upload e Extração"
           - Selecione um ou mais arquivos Excel (.xlsx)
           - O sistema aceita arquivos até {MAX_UPLOAD_SIZE_MB}MB no total (requer config.toml ajustado)

        2. **Processamento (em 3 etapas, por arquivo)**
           - Etapa 1: mapeia todas as colunas presentes nos arquivos e valida colunas obrigatórias
           - Etapa 2: calcula os totais por Pedido de Compra (PO)
           - Etapa 3: grava o arquivo final direto em disco, linha a linha
           - Cada arquivo é aberto, processado e liberado da memória antes do próximo
           - Se UM arquivo falhar em qualquer etapa, ele é isolado e reportado na aba
             "🛠️ Diagnóstico" — os demais arquivos do lote continuam sendo processados

        3. **Ordem das colunas no arquivo final (FIXA)**
           - O arquivo final sempre segue a mesma ordem de colunas, independente
             da ordem em que elas apareçam nos arquivos enviados.
           - Se algum arquivo do lote não tiver uma dessas colunas, ela ainda
             aparece no resultado — só que em branco para as linhas daquele arquivo.
           - Se algum arquivo tiver uma coluna com nome diferente de tudo que está
             na ordem padrão, ela é preservada e posicionada logo **antes** da
             coluna `unique`, que é sempre a última coluna do arquivo.

        4. **Visualização**
           - Acesse a aba "Visualização de Dados"
           - Veja as métricas gerais e uma prévia das primeiras {PREVIEW_ROWS} linhas

        5. **Diagnóstico**
           - Sempre que algo não sair 100% como esperado, confira essa aba antes de
             qualquer outra coisa — ela mostra exatamente qual arquivo, em qual etapa,
             e qual foi o erro ou aviso (inclusive avisos sobre colunas extras
             encontradas fora da ordem padrão).

        ### Estrutura esperada da planilha (por arquivo)
        - Cabeçalho **exatamente na linha 1** (sem título, logo ou linhas em branco acima)
        - Nenhuma célula mesclada na linha de cabeçalho
        - Colunas obrigatórias presentes, com esses nomes exatos:
          `Purchasing Document`, `Item`, `Order Quantity`, `Net order value`
        - Formato real `.xlsx` (não `.xls` antigo renomeado, não HTML/SYLK exportado
          do SAP com a extensão trocada, e sem senha/proteção)
        - Colunas de data no formato de data do Excel ou texto reconhecível
          (`dd/mm/aaaa` ou `aaaa-mm-dd`)

        ### O que fazer se o Diagnóstico apontar "arquivo corrompido / formato inválido"
        - Abra o arquivo no Excel e use **Arquivo > Salvar como > Excel Workbook (.xlsx)**
          para regravá-lo em formato .xlsx nativo, depois reenvie.
        - Se o arquivo tiver senha, remova a proteção antes de enviar.

        ### Dúvidas Frequentes
        1. **Tipos de arquivo aceitos?**
           - Apenas arquivos Excel (.xlsx) nativos.

        2. **Limite de tamanho?**
           - {MAX_UPLOAD_SIZE_MB}MB no total (ajustável em `.streamlit/config.toml`, veja o
             topo deste arquivo). Se o uploader mostrar "Limit 200MB per file", o
             config.toml não está sendo aplicado nesta sessão.

        3. **O que acontece se faltar uma coluna obrigatória em um dos arquivos?**
           - O arquivo ainda é processado, mas os valores derivados dessa coluna saem
             zerados/vazios para ele, e um aviso aparece na aba Diagnóstico.

        4. **E se um arquivo estiver corrompido ou protegido por senha?**
           - Esse arquivo específico é excluído do resultado (não trava o lote inteiro),
             e o motivo exato aparece na aba Diagnóstico.

        5. **E se um arquivo tiver uma coluna com nome que não existe nos outros?**
           - Ela é preservada no arquivo final, posicionada logo antes da coluna
             `unique`. Um aviso é registrado na aba Diagnóstico listando quais
             colunas extras foram encontradas.

        6. **Dados processados são salvos?**
           - O arquivo final fica em um arquivo temporário no servidor durante a sessão
             e é removido ao clicar em "Limpar e Voltar ao Início" (ou ao reiniciar a sessão).
        """)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Application error: {str(e)}")
        st.error("Ocorreu um erro inesperado. Por favor, tente novamente.")
        with st.expander("Detalhes técnicos"):
            st.code(traceback.format_exc(), language="text")

    st.markdown("---")
    st.markdown(
        """
        <div style='text-align: center'>
            <p>Desenvolvido com ❤️ | PO Processor Pro v2.2 (ordem fixa de colunas)</p>
        </div>
        """,
        unsafe_allow_html=True
    )
