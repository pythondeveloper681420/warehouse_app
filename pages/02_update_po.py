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
# ARQUITETURA DESTE SCRIPT (v4.0 — dedup por "mais recente" + saída ordenada)
# -----------------------------------------------------------------------------
# REGRA DE DEDUPLICAÇÃO ('unique' nunca se repete):
#   - A chave de um registro é (Purchasing Document, Item). Se essa MESMA
#     chave aparecer em mais de um arquivo do lote (ou mais de uma vez no
#     mesmo arquivo), apenas UM registro sobrevive no resultado final —
#     nunca duas linhas com o mesmo valor de 'unique'.
#   - O registro que sobrevive é escolhido comparando a coluna
#     'Document Date' de cada ocorrência:
#       1) Se as duas ocorrências tiverem 'Document Date', vence a mais
#          recente (data maior).
#       2) Se só uma das duas tiver 'Document Date', vence a que TEM data
#          (considerada mais completa/confiável).
#       3) Se nenhuma tiver 'Document Date', ou as duas datas forem iguais,
#          vence a última processada (desempate estável pela ordem de
#          leitura dos arquivos/linhas).
#
# ORDENAÇÃO FINAL:
#   - As linhas do arquivo de saída são sempre gravadas em ordem CRESCENTE
#     de 'unique' (Purchasing Document e, dentro dele, Item) — ou seja, a
#     planilha final sai ordenada do menor para o maior 'unique'.
#
# POR QUE A ARQUITETURA MUDOU EM RELAÇÃO À VERSÃO ANTERIOR (cache em disco):
#   - A versão anterior conseguia gravar o arquivo final em streaming (linha
#     a linha, sem guardar nada em memória) porque bastava manter a PRIMEIRA
#     ocorrência de cada chave.
#   - Agora, para decidir qual ocorrência é "mais recente", é necessário
#     comparar todas as ocorrências da MESMA chave antes de decidir qual
#     gravar — e para ordenar o resultado por 'unique' também é necessário
#     ter todas as linhas escolhidas em mãos antes de começar a escrever.
#   - Por isso, este script mantém em memória apenas UM registro por chave
#     (Purchasing Document + Item) — nunca as linhas duplicadas descartadas
#     — com os valores já convertidos (não o Excel bruto). Isso é bem mais
#     leve do que manter todas as linhas de todos os arquivos, mas ainda
#     assim é maior que zero, então o consumo de memória cresce com o
#     número de combinações ÚNICAS de Purchasing Document + Item no lote
#     (não com o número total de linhas lidas).
#
# ISOLAMENTO DE FALHAS POR ARQUIVO (mantido):
#   - Cada arquivo é lido dentro de um try/except próprio. Se um arquivo
#     falhar (corrompido, protegido por senha, sem cabeçalho legível etc.),
#     ele é marcado como "com erro", pulado, e o processamento CONTINUA
#     normalmente para os demais arquivos do lote. Nenhum arquivo com
#     problema interrompe o lote inteiro.
#   - Todos os erros/avisos ficam em st.session_state.file_diagnostics e são
#     exibidos na aba "🛠️ Diagnóstico".
#
# =============================================================================
# ORDEM FIXA DE COLUNAS (baseada no arquivo EXEMPLO fornecido)
# -----------------------------------------------------------------------------
#   - FINAL_COLUMN_ORDER abaixo é a ordem EXATA de colunas do arquivo de
#     exemplo (EXEMPLO.xlsx), na mesma sequência em que elas aparecem lá,
#     já REMOVENDO duplicatas exatas de nome (quando o mesmo nome de coluna
#     aparecia mais de uma vez no exemplo, mantemos apenas UMA ocorrência,
#     na posição da primeira aparição).
#   - O arquivo final SEMPRE sai com essas colunas, nessa ordem, mesmo que
#     um arquivo de entrada específico não tenha alguma delas (nesse caso a
#     célula fica em branco só para as linhas daquele arquivo).
#   - 'unique' é sempre a ÚLTIMA coluna dentro de FINAL_COLUMN_ORDER, e cada
#     valor de 'unique' aparece no máximo UMA vez no arquivo final.
#   - Se algum arquivo do lote tiver uma coluna com nome que NÃO está em
#     FINAL_COLUMN_ORDER (coluna nova/desconhecida), ela é preservada e
#     posicionada DEPOIS de 'unique' (ou seja, no final de tudo) — nunca
#     antes. Isso garante que a ordem fixa nunca seja quebrada.
#   - Nenhuma coluna do arquivo é descartada: as conhecidas seguem a ordem
#     fixa, as desconhecidas vão para o final, na ordem em que forem
#     encontradas nos arquivos do lote.
# =============================================================================

import streamlit as st
import pandas as pd
from datetime import datetime
import time
import os
import gc
import re
import traceback
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
    'Price unit', 'Gross Price', 'Value to be delivered', 'Value to be invoiced',
    'PBXX Amount', 'Total GR Quantity', 'Quantity to be delivered',
    'Delta PR Delivery', 'Delta PR Stat', 'Quantity Progress',
]

# Colunas de data "de origem" que devem ser reformatadas para dd/mm/aaaa
DATE_COLUMNS = [
    'Delivery date', 'Last FUP',
    'Stat.-Rel. Del. Date', 'Delivery Date',
    'Requisition Date', 'Inspection Request Date',
    'First Delivery Date', 'Purchase Requisition Delivery Date',
    'Inspection Requisition Date', 'Pending since', 'Next FUP',
    'Estimated Arrival', 'Document Date',
]

# Colunas de identificador que são normalizadas para inteiro
ID_COLUMNS = ['Purchasing Document', 'Item', 'Material']

# Quantas linhas no topo de cada arquivo podem ser escaneadas em busca do
# cabeçalho real, caso as primeiras linhas estejam em branco ou sejam só
# título/logo (comum em relatórios exportados do SAP GUI).
HEADER_SCAN_MAX_ROWS = 20

# Coluna usada para decidir qual ocorrência de uma chave duplicada é a
# "mais recente" (ver regra de deduplicação no topo do arquivo).
RECENCY_COLUMN = 'Document Date'

# =============================================================================
# ORDEM FIXA E DEFINITIVA DAS COLUNAS NO ARQUIVO FINAL
# =============================================================================
FINAL_COLUMN_ORDER: List[str] = [
    'Purchasing Document',
    'Item',
    'Supplier',
    'Vendor',
    'Vendor Name',
    'Material Description',
    'Material',
    'Control Code (NCM)',
    'Order Quantity',
    'Quantity to be delivered',
    'Order Unit',
    'Andritz WBS Element',
    'Cost Center',
    'Project Code',
    'Purchase Requisition Delivery Date',
    'Purchase Requisition',
    'Delivery date',
    'Stat.-Rel. Del. Date',
    'First Delivery Date',
    'Requisition Date',
    'PR Created by',
    'Value to be delivered',
    'Net order value',
    'Delta PR Delivery',
    'Delta PR Stat',
    'Total GR Quantity',
    'Document Date',
    'PO Created by',
    'PO Created by ZP (partner role)',
    'Responsible',
    'Suggested Responsible',
    'Internal Contact',
    'Quantity Progress Responsible',
    'Plant',
    'Planta',
    'Plant 2',
    'Country',
    'Region',
    'City',
    'Country/Region Key',
    'Currency',
    'Incoterms',
    'Incoterms (Part 2)',
    'Next FUP',
    'Estimated Arrival',
    'Warn',
    'Pending since',
    'Invoice #',
    'Value to be invoiced',
    'PBXX Amount',
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
    'Purchasing Group',
    'Quantity Progress',
    'Inspection Request Date',
    'Inspection Plan',
    'Inspection Included',
    'Inspection Step',
    'Inspection Requisition Date',
    'Inspection Done',
    'Inspection Result',
    'Inspection Needed Days in Advance',
    'Coluna1',
    'Acct Assignment Cat.',
    'Item Category',
    'Tax Code',
    'Storage Location',
    'Storage location',
    'Payment terms',
    'Payment Terms',
    'Atraso',
    'Grau de Criticidade',
    'Column1',
    'Comment',
    'Status',
    'Last FUP',
    'unique',
]

# Colunas calculadas (não existem nos arquivos de origem — são derivadas
# durante o processamento). Todas já estão posicionadas corretamente dentro
# de FINAL_COLUMN_ORDER; esta lista serve só para sabermos quais nomes NÃO
# devem ser tratados como "coluna de origem" ao ler os arquivos de entrada.
COMPUTED_COLUMN_NAMES: Set[str] = {
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
}

# Nomes de coluna conhecidos, usados só para RECONHECER com confiança qual
# linha é o cabeçalho de verdade quando ele não está na linha 1.
KNOWN_COLUMN_HINTS = set(FINAL_COLUMN_ORDER) | set(NUMERIC_SOURCE_COLUMNS) | set(DATE_COLUMNS) | set(ID_COLUMNS)

# Colunas que PRECISAM existir no cabeçalho de cada arquivo para que os
# cálculos façam sentido. Se faltarem, o arquivo ainda é processado (para
# não travar o lote), mas um AVISO é registrado no diagnóstico, pois os
# valores derivados daquela coluna sairão zerados/vazios para aquele arquivo.
REQUIRED_COLUMNS = ['Purchasing Document', 'Item', 'Order Quantity', 'Net order value']


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
    """Mapa nome-da-coluna -> índice, ignorando colunas sem nome.
    Em caso de nome duplicado dentro do MESMO arquivo, mantém a primeira
    ocorrência (política consistente: '1 coluna com aquele nome')."""
    idx = {}
    for i, name in enumerate(header):
        if name and name not in idx:
            idx[name] = i
    return idx


def get_cell(row: Tuple[Any, ...], col_idx: Dict[str, int], name: str, default: Any = None) -> Any:
    i = col_idx.get(name)
    if i is None or i >= len(row):
        return default
    return row[i]


def friendly_open_error(e: Exception) -> str:
    """Traduz as exceções mais comuns do openpyxl/zipfile em mensagens úteis."""
    import zipfile
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
    colunas são "extras", isto é, não fazem parte de FINAL_COLUMN_ORDER — ver
    build_final_header). Arquivos que falharem ao ter o cabeçalho lido são
    registrados em `diagnostics` e EXCLUÍDOS de `valid_files` (que é
    retornado junto), para que as etapas seguintes nem tentem reabri-los.
    Nenhum erro aqui interrompe o processamento dos demais arquivos do lote.
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

        if RECENCY_COLUMN not in header:
            diagnostics.append({
                'arquivo': f.name,
                'etapa': 'Etapa 1 — Mapeamento de colunas',
                'tipo': 'aviso',
                'mensagem': (
                    f"Coluna '{RECENCY_COLUMN}' ausente neste arquivo. Ela é usada para "
                    "decidir qual registro prevalece quando o mesmo Purchasing Document + "
                    "Item aparece em mais de um arquivo do lote. Sem ela, linhas deste "
                    "arquivo só vencem um empate se forem as últimas processadas."
                ),
            })

        valid_files.append(f)
        # Deduplica nomes repetidos dentro do PRÓPRIO cabeçalho do arquivo
        # (mesma política de build_col_index: mantém a primeira ocorrência).
        for col in header:
            if col and col not in seen:
                seen.add(col)
                master_columns.append(col)

    return master_columns, valid_files


def build_final_header(master_columns: List[str]) -> Tuple[List[str], List[str]]:
    """
    Monta o cabeçalho final do arquivo de saída, respeitando a ordem FIXA
    pedida:

        FINAL_COLUMN_ORDER (sempre, nessa ordem exata — termina em 'unique')
        + colunas "extras" (qualquer coluna encontrada em algum arquivo que
          não faça parte de FINAL_COLUMN_ORDER), na ordem em que foram
          encontradas, sempre DEPOIS de 'unique'.

    Retorna (final_header, extra_columns) — extra_columns é devolvido também
    para fins de diagnóstico/log.
    """
    known_cols = set(FINAL_COLUMN_ORDER)
    extra_columns = [c for c in master_columns if c not in known_cols]

    final_header = FINAL_COLUMN_ORDER + extra_columns
    return final_header, extra_columns


# =============================================================================
# PASSADA 1 — ler cada arquivo UMA vez e manter, por chave (Purchasing
# Document + Item), apenas o registro "vencedor" (mais recente)
# =============================================================================

def is_better_candidate(candidate_date: Optional[datetime], current_date: Optional[datetime]) -> bool:
    """
    Decide se um novo registro (candidate) deve substituir o registro atual
    (current) como vencedor de uma chave (Purchasing Document + Item), com
    base na coluna 'Document Date' de cada um.

    Regras (nessa ordem):
      1) As duas têm data -> vence a mais recente (data maior ou igual, o
         que faz o último processado vencer em caso de empate exato).
      2) Só o candidato tem data -> o candidato vence (mais completo).
      3) Só o atual tem data -> o atual permanece.
      4) Nenhum dos dois tem data -> o candidato vence (desempate estável
         pela ordem de processamento: o último lido vence).
    """
    if candidate_date is not None and current_date is not None:
        return candidate_date >= current_date
    if candidate_date is not None and current_date is None:
        return True
    if candidate_date is None and current_date is not None:
        return False
    return True  # nenhum dos dois tem data -> último processado vence


def extract_row_record(row: Tuple[Any, ...], col_idx: Dict[str, int],
                        file_columns: List[str]) -> Optional[Dict[str, Any]]:
    """
    Transforma UMA linha bruta do Excel em um "registro" com todos os
    valores já convertidos (datas formatadas, números limpos, IDs
    normalizados) e os campos auxiliares necessários para agregação e
    escrita final. Retorna None se a linha não tiver um Purchasing Document
    válido (mesmo filtro da versão anterior: descarta linhas de rodapé/total
    onde o Purchasing Document veio como texto).
    """
    raw_po = get_cell(row, col_idx, 'Purchasing Document')
    if isinstance(raw_po, str):
        return None
    po_id = parse_id(raw_po)
    if po_id is None:
        return None

    item_id = parse_id(get_cell(row, col_idx, 'Item'))
    material_id = parse_id(get_cell(row, col_idx, 'Material'))

    qty = to_number(get_cell(row, col_idx, 'Order Quantity', 0))
    net_value = to_number(get_cell(row, col_idx, 'Net order value', 0))
    pbxx = to_number(get_cell(row, col_idx, 'PBXX Condition Amount', 0))
    valor_unitario = safe_division(net_value, qty)
    valor_item_com_impostos = pbxx * qty

    doc_date = parse_date_value(get_cell(row, col_idx, RECENCY_COLUMN))

    wbs_raw = get_cell(row, col_idx, 'Andritz WBS Element')
    codigo_projeto_str = extract_code(wbs_raw) if isinstance(wbs_raw, str) else ''
    codigo_projeto = int(codigo_projeto_str) if codigo_projeto_str else ''

    vendor_name_raw = get_cell(row, col_idx, 'Vendor Name')
    vendor_name = str(vendor_name_raw) if vendor_name_raw else None

    # Monta os valores de TODAS as colunas de origem deste arquivo, já
    # convertidos. Colunas calculadas nunca vêm do arquivo, então são
    # excluídas de `file_columns` por quem chama esta função.
    out_partial: Dict[str, Any] = {}
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
        # IMPORTANTE: nunca deixar None aqui, senão a linha final fica
        # desalinhada em relação ao cabeçalho.
        if val is None:
            val = ''
        out_partial[col] = val

    return {
        'po_id': po_id,
        'item_id': item_id,
        'qty': qty,
        'net_value': net_value,
        'valor_unitario': valor_unitario,
        'valor_item_com_impostos': valor_item_com_impostos,
        'doc_date': doc_date,
        'codigo_projeto': codigo_projeto,
        'vendor_name': vendor_name,
        'out_partial': out_partial,
    }


def process_file_into_winners(uploaded_file: Any,
                               winners: Dict[Tuple[int, Optional[int]], Dict[str, Any]]) -> int:
    """
    Lê o arquivo original UMA única vez, linha a linha (streaming, sem
    carregar o arquivo inteiro em memória). Para cada linha válida, decide
    se ela deve entrar/substituir o registro vencedor de sua chave
    (Purchasing Document + Item) dentro de `winners`, usando a regra de
    'mais recente' (ver is_better_candidate). Retorna o número de linhas
    válidas lidas (métrica). Pode lançar exceção — o chamador
    (process_files) isola a falha por arquivo.
    """
    uploaded_file.seek(0)
    wb = load_workbook(uploaded_file, read_only=True, data_only=True)
    read_count = 0
    try:
        ws = wb.worksheets[0]
        header, rows_iter, header_idx = locate_header_and_rows(ws)
        if header_idx == -1:
            return 0
        col_idx = build_col_index(header)
        file_columns = [c for c in header if c and c not in COMPUTED_COLUMN_NAMES]

        for row in rows_iter:
            record = extract_row_record(row, col_idx, file_columns)
            if record is None:
                continue
            key = (record['po_id'], record['item_id'])
            current = winners.get(key)
            if current is None or is_better_candidate(record['doc_date'], current['doc_date']):
                winners[key] = record
            read_count += 1
    finally:
        wb.close()
        uploaded_file.seek(0)
    return read_count


# =============================================================================
# PASSADA 2 — agregar totais por PO (a partir só dos vencedores) e gravar o
# arquivo final, já ORDENADO por 'unique' crescente
# =============================================================================

def build_po_totals(winners: Dict[Tuple[int, Optional[int]], Dict[str, Any]]) -> Dict[int, Dict[str, float]]:
    """Agrega net/com_impostos/qty por Purchasing Document, considerando
    apenas os registros vencedores (nunca conta uma linha duas vezes, pois
    duplicatas já foram eliminadas antes desta etapa)."""
    po_totals: Dict[int, Dict[str, float]] = {}
    for record in winners.values():
        entry = po_totals.setdefault(record['po_id'], {'net': 0.0, 'com_impostos': 0.0, 'qty': 0.0})
        entry['net'] += record['net_value']
        entry['com_impostos'] += record['valor_item_com_impostos']
        entry['qty'] += record['qty']
    return po_totals


def write_sorted_output(ws_out: Any, final_header: List[str],
                         winners: Dict[Tuple[int, Optional[int]], Dict[str, Any]],
                         po_totals: Dict[int, Dict[str, float]]) -> Tuple[int, Set[str]]:
    """
    Grava uma linha por chave vencedora, em ordem CRESCENTE de
    (Purchasing Document, Item) — o que corresponde à ordem crescente da
    própria coluna 'unique'. Retorna (linhas_gravadas, nomes_de_fornecedor_vistos).
    """
    vendor_names_seen: Set[str] = set()
    written = 0

    sorted_keys = sorted(
        winners.keys(),
        key=lambda k: (k[0], k[1] if k[1] is not None else -1)
    )

    for key in sorted_keys:
        record = winners[key]
        po_id = record['po_id']
        item_id = record['item_id']
        totals = po_totals.get(po_id, {'net': 0.0, 'com_impostos': 0.0, 'qty': 0.0})

        if record['vendor_name']:
            vendor_names_seen.add(record['vendor_name'])

        out_row: Dict[str, Any] = dict(record['out_partial'])
        out_row['total_itens_po'] = totals['qty']
        out_row['valor_unitario'] = record['valor_unitario']
        out_row['valor_item_com_impostos'] = record['valor_item_com_impostos']
        out_row['total_valor_po_liquido'] = totals['net']
        out_row['total_valor_po_com_impostos'] = totals['com_impostos']
        out_row['valor_unitario_formatted'] = format_currency(record['valor_unitario'])
        out_row['valor_item_com_impostos_formatted'] = format_currency(record['valor_item_com_impostos'])
        out_row['Net order value_formatted'] = format_currency(record['net_value'])
        out_row['total_valor_po_liquido_formatted'] = format_currency(totals['net'])
        out_row['total_valor_po_com_impostos_formatted'] = format_currency(totals['com_impostos'])
        out_row['PO Creation Date'] = record['doc_date'].strftime('%d/%m/%Y') if record['doc_date'] else ''
        out_row['codigo_projeto'] = record['codigo_projeto']
        # 'unique' nunca se repete: é exatamente a chave de deduplicação.
        out_row['unique'] = f"{po_id}{item_id if item_id is not None else ''}"

        ws_out.append([out_row.get(col, '') for col in final_header])
        written += 1

    return written, vendor_names_seen


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
# Orquestração do pipeline completo (com isolamento de falhas por arquivo)
# =============================================================================

def process_files(uploaded_files: List[Any], progress_bar: Any, status_placeholder: Any) -> Dict[str, Any]:
    """
    Executa o pipeline completo sobre a lista de arquivos e grava o
    resultado em um arquivo temporário em disco. Retorna um dicionário com o
    caminho do arquivo final, as métricas calculadas e a lista de
    diagnósticos (erros/avisos por arquivo) encontrados ao longo do
    processamento.

    Nenhum arquivo com problema interrompe o lote: a leitura de cada arquivo
    é isolada; se uma falhar, o processamento segue para o próximo.
    """
    diagnostics: List[Dict[str, str]] = []
    n_files = len(uploaded_files)

    # --- Etapa 1: mapear colunas --------------------------------------------
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
                f"padrão foram preservadas e posicionadas DEPOIS de 'unique', "
                f"no final do arquivo: {', '.join(extra_columns)}."
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

    # --- Etapa 2: ler cada arquivo uma vez e escolher o registro vencedor ---
    # por chave (Purchasing Document + Item), com base na coluna
    # 'Document Date' (ver is_better_candidate). Só o registro vencedor de
    # cada chave fica em memória — nunca as ocorrências duplicadas descartadas.
    winners: Dict[Tuple[int, Optional[int]], Dict[str, Any]] = {}
    files_ok_read: List[Any] = []

    for idx, f in enumerate(valid_files):
        status_placeholder.info(
            f"➕ Etapa 2/3 — Lendo arquivos e selecionando o registro mais recente de cada "
            f"Purchasing Document + Item... arquivo {idx + 1}/{len(valid_files)}: {f.name}"
        )
        try:
            process_file_into_winners(f, winners)
            files_ok_read.append(f)
        except Exception as e:
            logger.exception(f"Falha ao ler o arquivo {f.name}")
            diagnostics.append({
                'arquivo': f.name,
                'etapa': 'Etapa 2 — Leitura e seleção do registro mais recente',
                'tipo': 'erro',
                'mensagem': friendly_open_error(e),
            })
        progress_bar.progress(0.05 + 0.55 * ((idx + 1) / len(valid_files)))

    # --- Etapa 3: agregar totais por PO (só com os vencedores) e gravar -----
    # o arquivo final já ORDENADO por 'unique' crescente.
    status_placeholder.info("💾 Etapa 3/3 — Agregando totais por PO e gravando o arquivo final ordenado...")

    total_rows_written = 0
    total_pos = 0
    vendor_names_seen: Set[str] = set()
    out_path: Optional[str] = None

    try:
        po_totals = build_po_totals(winners)
        total_pos = len(po_totals)

        import tempfile
        out_fd, out_path = tempfile.mkstemp(suffix='.xlsx', prefix='po_processado_')
        os.close(out_fd)

        wb_out = Workbook(write_only=True)
        ws_out = wb_out.create_sheet('PO_Processado')
        ws_out.append(final_header)

        total_rows_written, vendor_names_seen = write_sorted_output(ws_out, final_header, winners, po_totals)

        wb_out.save(out_path)
    except Exception as e:
        logger.exception("Falha ao agregar totais / gravar o arquivo final")
        diagnostics.append({
            'arquivo': '(vários arquivos)',
            'etapa': 'Etapa 3 — Agregação e gravação do arquivo final',
            'tipo': 'erro',
            'mensagem': friendly_open_error(e),
        })
        if out_path and os.path.exists(out_path):
            try:
                os.remove(out_path)
            except Exception:
                pass
        out_path = None
        total_rows_written = 0

    progress_bar.progress(1.0)
    gc.collect()

    return {
        'output_path': out_path,
        'total_rows': total_rows_written,
        'total_pos': total_pos,
        'total_vendors': len(vendor_names_seen),
        'total_columns': len(final_header),
        'diagnostics': diagnostics,
        'files_ok': len(files_ok_read),
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
        "Processamento em lote: cada arquivo é lido uma única vez. Quando o mesmo "
        "Purchasing Document + Item aparece em mais de um arquivo, só o registro com a "
        "'Document Date' mais recente é mantido — a coluna 'unique' nunca se repete no "
        "resultado final. O arquivo final sai sempre ordenado do menor para o maior "
        "'unique' e segue a mesma ordem fixa de colunas (baseada no arquivo de exemplo); "
        "colunas ausentes em algum arquivo saem em branco, e colunas extras/desconhecidas "
        "aparecem depois de 'unique', no final do arquivo. Se um arquivo específico tiver "
        "problema, ele é isolado e reportado, sem derrubar o processamento dos demais."
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
                            m3.metric("Registros únicos (unique)", result['total_rows'])
                            m4.metric("PO's distintas", result['total_pos'])
                    except Exception as e:
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
            c1.metric(label="Total de Linhas (unique)", value=metrics['total_rows'])
            c2.metric(label="Número de Fornecedores", value=metrics['total_vendors'])
            c3.metric(label="Número de PO'S", value=metrics['total_pos'])
            c4.metric(label="Total de Colunas", value=metrics['total_columns'])

            st.caption(
                f"Mostrando as primeiras {min(PREVIEW_ROWS, len(preview_df))} linhas do arquivo final, "
                "já ordenado por 'unique' crescente (apenas para conferência — o arquivo baixado "
                "contém todos os registros, todos com 'unique' distinto)."
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

        2. **Processamento (em 3 etapas)**
           - Etapa 1: mapeia todas as colunas presentes nos arquivos e valida colunas obrigatórias
           - Etapa 2: lê cada arquivo uma vez e, para cada Purchasing Document + Item, mantém
             apenas o registro com a 'Document Date' mais recente (se o mesmo PO + Item
             aparecer em mais de um arquivo, o mais antigo é descartado)
           - Etapa 3: agrega os totais por Pedido de Compra (considerando só os registros
             mantidos) e grava o arquivo final, já ORDENADO por 'unique' crescente
           - Se UM arquivo falhar na leitura, ele é isolado e reportado na aba
             "🛠️ Diagnóstico" — os demais arquivos do lote continuam sendo processados
             normalmente, sem nenhuma interrupção do lote inteiro

        3. **Regra de "mais recente" e da coluna 'unique'**
           - A chave de cada registro é Purchasing Document + Item.
           - Se essa chave aparecer mais de uma vez no lote (no mesmo arquivo ou em
             arquivos diferentes), só UMA linha sobrevive: a que tiver a 'Document Date'
             mais recente. Entre uma linha com data e outra sem, vence a que tem data.
             Em caso de empate total, vence a última processada.
           - Por isso, a coluna `unique` (Purchasing Document + Item) nunca se repete
             no arquivo final — cada valor aparece no máximo uma vez.
           - O arquivo final sai sempre ordenado do menor para o maior valor de `unique`.

        4. **Ordem das colunas no arquivo final (FIXA, baseada no arquivo de exemplo)**
           - O arquivo final sempre segue a mesma ordem de colunas, independente
             da ordem em que elas apareçam nos arquivos enviados.
           - Se algum arquivo do lote não tiver uma dessas colunas, ela ainda
             aparece no resultado — só que em branco para as linhas daquele arquivo.
           - Se algum arquivo tiver uma coluna com nome diferente de tudo que está
             na ordem padrão, ela é preservada e posicionada **depois** da
             coluna `unique`, no final de tudo.

        5. **Visualização**
           - Acesse a aba "Visualização de Dados"
           - Veja as métricas gerais e uma prévia das primeiras {PREVIEW_ROWS} linhas

        6. **Diagnóstico**
           - Sempre que algo não sair 100% como esperado, confira essa aba antes de
             qualquer outra coisa — ela mostra exatamente qual arquivo, em qual etapa,
             e qual foi o erro ou aviso.

        ### Estrutura esperada da planilha (por arquivo)
        - Cabeçalho **exatamente na linha 1** (sem título, logo ou linhas em branco acima) —
          mas o sistema também consegue localizar o cabeçalho automaticamente se ele
          estiver um pouco mais abaixo
        - Nenhuma célula mesclada na linha de cabeçalho
        - Colunas obrigatórias presentes, com esses nomes exatos:
          `Purchasing Document`, `Item`, `Order Quantity`, `Net order value`
        - Coluna `Document Date` presente e preenchida, para que a regra de
          "mantém o mais recente" funcione corretamente entre arquivos
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

        3. **O que acontece se o mesmo Purchasing Document + Item aparecer em dois arquivos?**
           - Só um sobrevive: o que tiver a 'Document Date' mais recente. O outro é
             descartado silenciosamente do resultado (isso não é um erro, é o
             comportamento esperado da deduplicação).

        4. **O que acontece se faltar uma coluna obrigatória em um dos arquivos?**
           - O arquivo ainda é processado, mas os valores derivados dessa coluna saem
             zerados/vazios para ele, e um aviso aparece na aba Diagnóstico.

        5. **E se um arquivo estiver corrompido ou protegido por senha?**
           - Esse arquivo específico é excluído do resultado (não trava o lote inteiro),
             e o motivo exato aparece na aba Diagnóstico.

        6. **E se um arquivo tiver uma coluna com nome que não existe nos outros?**
           - Ela é preservada no arquivo final, posicionada logo DEPOIS da coluna
             `unique` (no final de tudo). Um aviso é registrado na aba Diagnóstico
             listando quais colunas extras foram encontradas.

        7. **Dados processados são salvos?**
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
            <p>Desenvolvido com ❤️ | PO Processor Pro v4.0 (dedup por mais recente + saída ordenada)</p>
        </div>
        """,
        unsafe_allow_html=True
    )
