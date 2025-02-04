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

def normalize_string(texto):
    if not isinstance(texto, str):
        return str(texto)
    texto = texto.lower()
    texto = ''.join(
        char for char in unicodedata.normalize('NFKD', texto)
        if unicodedata.category(char) != 'Mn'
    )
    return re.sub(r'[^\w\s]', '', texto)

def criar_padrao_flexivel(texto):
    normalizado = normalize_string(texto)
    fragmentos = normalizado.split()
    padrao = '.*'.join(f'(?=.*{re.escape(fragmento)})' for fragmento in fragmentos)
    return padrao + '.*'

@st.cache_resource
def obter_cliente_mongodb():
    usuario = st.secrets["MONGO_USERNAME"]
    senha = st.secrets["MONGO_PASSWORD"]
    cluster = st.secrets["MONGO_CLUSTER"]
    banco = st.secrets["MONGO_DB"]
    
    usuario_escaped = urllib.parse.quote_plus(usuario)
    senha_escaped = urllib.parse.quote_plus(senha)
    URI = f"mongodb+srv://{usuario_escaped}:{senha_escaped}@{cluster}/{banco}?retryWrites=true&w=majority"
    
    return MongoClient(URI)

@st.cache_data
def obter_colunas_colecao(nome_colecao):
    cliente = obter_cliente_mongodb()
    db = cliente.warehouse
    colecao = db[nome_colecao]
    
    total_docs = colecao.count_documents({})
    doc_exemplo = colecao.find_one()
    
    colunas = []
    if doc_exemplo:
        colunas = [col for col in doc_exemplo.keys() if col != '_id']
    
    colunas_padrao = {
        'xml': [
            'url_imagens',
            'Nota Fiscal', 
            'Item Nf',
            'Nome Material',
            'Codigo NCM',
            'Quantidade',
            'Unidade',
            'Valor Unitario Produto',
            'Valor Total Produto',
            'Valor Total Nota Fiscal',
            'Total itens Nf',
            'data nf',
            'Data Vencimento',
            'Chave NF-e',    
            'Nome Emitente',
            'CNPJ Emitente',
            'CFOP Categoria',
            'PO',
            'Itens recebidos PO',
            'Valor Recebido PO',
            'Codigo Projeto',
            'Projeto',
            'WBS Andritz',
            'Centro de Custo',
            'Codigo Projeto Envio',
            'Projeto Envio',
            'grupo',
            'subgrupo'
        ],
        'nfspdf': ['Competencia', 'CNPJ Prestador'],
        'po': ['Item', 'Supplier'],
        'reqepi': [
            'url_imagens',
            'Item',
            'Código Material',
            'Descrição Material',
            'Código Material Atendido',
            'Descrição Material Atendido',
            'Quantidade Solicitada',
            'Quantidade Atendida',
            'Unidade',
            'Valor Unitário',
            'Valor Total',
            'Número Requisição',
            'Nome Solicitante',
            'Chave Nota Fiscal',
            'Data Solicitação',
            'Centro de Custo - WBS',
            'Obra / Setor',
            'ID Solicitante',
            'Status Requisição',
            'Nome Arquivo',
            'Número Mês',
            'Mês',
            'Ano',
            #'Dia',
            'Mês/Ano',
            'Código Requisição',
            'unique'
        ]
    }
    
    fallback = colunas_padrao.get(nome_colecao, colunas[:26])
    padroes_finais = [col for col in fallback if col in colunas]
    
    if not padroes_finais:
        padroes_finais = colunas[:10]
    
    return total_docs, colunas, padroes_finais

def atualizar_documento(nome_colecao, id_documento, dados):
    try:
        cliente = obter_cliente_mongodb()
        colecao = cliente.warehouse[nome_colecao]
        colecao.update_one(
            {"_id": ObjectId(id_documento)},
            {"$set": dados}
        )
        return True, "Registro atualizado com sucesso!"
    except Exception as e:
        return False, f"Erro na atualização: {str(e)}"

def excluir_documento(nome_colecao, id_documento):
    try:
        cliente = obter_cliente_mongodb()
        colecao = cliente.warehouse[nome_colecao]
        colecao.delete_one({"_id": ObjectId(id_documento)})
        return True, "Registro excluído com sucesso!"
    except Exception as e:
        return False, f"Erro na exclusão: {str(e)}"

@st.dialog("Editar Registro", width="large")
def dialog_edicao(nome_colecao, registro, colunas_visiveis):
    dados_editados = {}
    for col in colunas_visiveis:
        if col != '_id':
            valor_atual = registro.get(col, "")
            dados_editados[col] = st.text_input(col, valor_atual)
    
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Salvar", type="primary", use_container_width=True):
            sucesso, mensagem = atualizar_documento(nome_colecao, str(registro['_id']), dados_editados)
            if sucesso:
                st.success(mensagem)
                st.rerun()
            else:
                st.error(mensagem)
    with col2:
        if st.button("Cancelar", use_container_width=True):
            st.rerun()

@st.dialog("Confirmar Exclusão", width="small")
def dialog_exclusao(nome_colecao, registro):
    st.warning("Tem certeza que deseja excluir este registro?")
    
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Sim, Excluir", type="primary", use_container_width=True):
            sucesso, mensagem = excluir_documento(nome_colecao, str(registro['_id']))
            if sucesso:
                st.success(mensagem)
                st.rerun()
            else:
                st.error(mensagem)
    with col2:
        if st.button("Cancelar", use_container_width=True):
            st.rerun()

def obter_valores_unicos(nome_colecao, coluna):
    cliente = obter_cliente_mongodb()
    colecao = cliente.warehouse[nome_colecao]
    
    pipeline = [
        {"$group": {"_id": f"${coluna}"}},
        {"$sort": {"_id": 1}},
        {"$limit": 100000}
    ]
    
    try:
        valores_unicos = [
            doc["_id"] for doc in colecao.aggregate(pipeline)
            if doc["_id"] is not None
        ]
        return sorted(valores_unicos)
    except Exception as e:
        st.error(f"Erro ao obter valores únicos para {coluna}: {str(e)}")
        return []

def converter_para_numerico(valor):
    valor_limpo = str(valor).strip().replace(',', '.')
    try:
        return int(valor_limpo)
    except ValueError:
        try:
            return float(valor_limpo)
        except ValueError:
            return valor

def obter_tipos_colunas(nome_colecao):
    try:
        cliente = obter_cliente_mongodb()
        colecao = cliente.warehouse[nome_colecao]
        doc_exemplo = colecao.find_one()
        
        if not doc_exemplo:
            return {}
        
        def determinar_tipo(valor):
            if valor is None:
                return 'str'
            if isinstance(valor, (int, float)):
                return 'float64' if isinstance(valor, float) else 'int64'
            if isinstance(valor, str):
                try:
                    float(valor.replace(',', '.'))
                    return 'float64'
                except ValueError:
                    return 'str'
            return 'str'
        
        return {k: determinar_tipo(v) for k, v in doc_exemplo.items() if k != '_id'}
    except Exception as e:
        st.error(f"Erro ao obter tipos de colunas: {str(e)}")
        return {}

def construir_query_mongo(filtros, tipos_colunas):
    query = {}
    
    for coluna, info_filtro in filtros.items():
        tipo_filtro = info_filtro['tipo']
        valor_filtro = info_filtro['valor']
        
        if not valor_filtro:
            continue
            
        if tipos_colunas.get(coluna, 'str') in ['int64', 'float64']:
            try:
                valor_numerico = converter_para_numerico(valor_filtro)
                if isinstance(valor_numerico, (int, float)):
                    query[coluna] = valor_numerico
                    continue
            except:
                pass
        
        if tipo_filtro == 'texto':
            padrao = criar_padrao_flexivel(valor_filtro)
            query[coluna] = {'$regex': padrao, '$options': 'i'}
        elif tipo_filtro == 'multi':
            query[coluna] = {'$in': valor_filtro}
    
    return query

def criar_interface_filtros(nome_colecao, colunas):
    tipos_colunas = obter_tipos_colunas(nome_colecao)
    filtros = {}
    
    with st.expander("**Filtros:**", expanded=False):
        colunas_selecionadas = st.multiselect(
            "Selecione as Colunas para filtrar:",
            colunas,
            key=f"filter_cols_{nome_colecao}"
        )
        
        if colunas_selecionadas:
            cols = st.columns(2)
            for idx, coluna in enumerate(colunas_selecionadas):
                with cols[idx % 2]:
                    st.markdown(f"#### {coluna}")
                    tipo_coluna = tipos_colunas.get(coluna, 'str')
                    tipo_filtro = st.radio(
                        "Tipo de filtro:",
                        ["Texto", "Seleção Múltipla"],
                        key=f"radio_{nome_colecao}_{coluna}",
                        horizontal=True
                    )
                    
                    if tipo_filtro == "Texto":
                        valor = st.text_input(
                            f"Buscar {coluna}" + (" (numérico)" if tipo_coluna in ['int64', 'float64'] else ""),
                            key=f"text_filter_{nome_colecao}_{coluna}"
                        )
                        if valor:
                            filtros[coluna] = {'tipo': 'texto', 'valor': valor}
                    else:
                        valores_unicos = obter_valores_unicos(nome_colecao, coluna)
                        if valores_unicos:
                            selecionados = st.multiselect(
                                "Selecione os valores:",
                                options=valores_unicos,
                                key=f"multi_filter_{nome_colecao}_{coluna}"
                            )
                            if selecionados:
                                filtros[coluna] = {'tipo': 'multi', 'valor': selecionados}
                    
                    st.markdown("---")
    
    return filtros, tipos_colunas

def converter_para_pandas(doc):
    convertido = {}
    for chave, valor in doc.items():
        if isinstance(valor, ObjectId):
            convertido[chave] = str(valor)
        elif isinstance(valor, dict):
            convertido[chave] = converter_para_pandas(valor)
        elif isinstance(valor, list):
            convertido[chave] = [str(item) if isinstance(item, ObjectId) else item for item in valor]
        else:
            convertido[chave] = valor
    return convertido

def carregar_dados_paginados(nome_colecao, pagina, tamanho_pagina, filtros=None, tipos_colunas=None):
    if tipos_colunas is None:
        tipos_colunas = obter_tipos_colunas(nome_colecao)
    
    cliente = obter_cliente_mongodb()
    colecao = cliente.warehouse[nome_colecao]
    query = construir_query_mongo(filtros, tipos_colunas) if filtros else {}
    pular = (pagina - 1) * tamanho_pagina
    
    try:
        ordenacao = [("Data Emissao", -1)] if nome_colecao == 'xml' else None
        total_filtrado = colecao.count_documents(query)
        
        cursor = (colecao.find(query)
                 .sort(ordenacao) if ordenacao else colecao.find(query))
        cursor = cursor.skip(pular).limit(tamanho_pagina)
        
        documentos = [converter_para_pandas(doc) for doc in cursor]
        df = pd.DataFrame(documentos)
        
        return df, total_filtrado
    except Exception as e:
        st.error(f"Erro ao carregar dados: {str(e)}")
        return pd.DataFrame(), 0

def processar_urls(urls):
    if isinstance(urls, pd.Series):
        urls = urls.iloc[0]
    if urls is None or (isinstance(urls, float) and math.isnan(urls)):
        return None
    if isinstance(urls, str):
        return urls
    if isinstance(urls, list):
        urls_validas = [url for url in urls if url and isinstance(url, str)]
        return urls_validas[0] if urls_validas else None
    return None

def formatar_valor(valor):
    if isinstance(valor, pd.Series):
        valor = valor.iloc[0]
    if pd.isna(valor):
        return '-'
    if isinstance(valor, (int, float)):
        # Remove decimal places if value is a whole number
        if float(valor).is_integer():
            return f'{int(valor):,}'.replace(',', '')
        return f'{valor:.2f}'.replace('.', ',')
    if isinstance(valor, str):
        valor_limpo = valor.strip().replace(',', '.')
        try:
            valor_num = float(valor_limpo)
            if float(valor_num).is_integer():
                return f'{int(valor_num):,}'.replace(',', '')
            return f'{valor_num:.2f}'.replace('.', ',')
        except ValueError:
            return str(valor)[:45]
    return str(valor)[:45]

def renderizar_cartoes(df, colunas_visiveis, nome_colecao):
    colunas_imagem = [col for col in df.columns if 'url_imagens' in col.lower()]
    num_colunas = 5
    linhas = [df.iloc[i:i+num_colunas] for i in range(0, len(df), num_colunas)]
    
    for idx_linha, linha in enumerate(linhas):
        cols = st.columns(num_colunas)
        
        for idx_col, (_, registro) in enumerate(linha.iterrows()):
            with cols[idx_col]:
                url_imagem = None
                for col_img in colunas_imagem:
                    if col_img in registro:
                        url_imagem = processar_urls(registro[col_img])
                        if url_imagem:
                            break

                detalhes_cartao = ''.join([
                    f'<div style="margin-bottom: 4px; font-size: 0.8rem;">'
                    f'<strong>{col}:</strong> {formatar_valor(registro.get(col, "-"))}</div>'
                    for col in colunas_visiveis 
                    if col not in colunas_imagem and col != '_id'
                ])
                
                estilo_cartao = (
                    "border: 1px solid #e0e0e0; border-radius: 8px; "
                    "box-shadow: 0 4px 6px rgba(0,0,0,0.1); margin-bottom: 10px; "
                    "overflow: hidden; transition: transform 0.2s; "
                    "height: 400px;"
                )
                
                if url_imagem:
                    html_cartao = f"""
                    <div style='{estilo_cartao}'>
                        <div style="width:100%; height:180px; display:flex; justify-content:center; align-items:center; overflow:hidden;">
                            <img src="{url_imagem}" style="width:100%; height:100%; object-fit:contain; object-position:center;">
                        </div>
                        <div style='padding: 8px; font-size: 0.8rem; height: 180px; overflow-y: auto;'>
                            {detalhes_cartao}
                        </div>
                    </div>
                    """
                else:
                    html_cartao = f"""
                    <div style='{estilo_cartao}'>
                        <div style='padding: 8px; font-size: 0.8rem; height: 360px; overflow-y: auto;'>
                            {detalhes_cartao}
                        </div>
                    </div>
                    """
                
                st.markdown(html_cartao, unsafe_allow_html=True)
                
                # Botões de ação
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("✏️ Editar", key=f"edit_btn_{nome_colecao}_{registro['_id']}", use_container_width=True):
                        dialog_edicao(nome_colecao, registro, colunas_visiveis)
                with col2:
                    if st.button("🗑️ Excluir", key=f"delete_btn_{nome_colecao}_{registro['_id']}", use_container_width=True):
                        dialog_exclusao(nome_colecao, registro)

def exibir_pagina_dados(nome_colecao):
    total_documentos, colunas, padrao_visiveis = obter_colunas_colecao(nome_colecao)
    
    if total_documentos == 0:
        st.error(f"Nenhum documento encontrado na coleção {nome_colecao}")
        return

    with st.expander("**Colunas Visíveis:**", expanded=False):
        chave_estado = f'colunas_visiveis_{nome_colecao}'
        if chave_estado not in st.session_state:
            st.session_state[chave_estado] = padrao_visiveis
        
        # Create columns for the multiselect and new button
        col_multiselect, col_button = st.columns([0.8, 0.2])
        
        with col_multiselect:
            colunas_visiveis = st.multiselect(
                "Selecione as Colunas para exibir:",
                options=colunas,
                default=st.session_state[chave_estado],
                key=f'seletor_colunas_{nome_colecao}'
            )
        
        # Add button to show all columns
        with col_button:
            if st.button("Mostrar Todas", key=f'mostrar_todas_{nome_colecao}', use_container_width=True):
                st.session_state[chave_estado] = colunas
                colunas_visiveis = colunas
                st.rerun()
        
        st.session_state[chave_estado] = colunas_visiveis

    filtros, tipos_colunas = criar_interface_filtros(nome_colecao, colunas)
    
    with st.expander("**Configurações:**", expanded=False):
        col1, col2, col3 = st.columns([1,1,1])
        
        with col1:
            tamanho_pagina = st.selectbox(
                "Registros por página:",
                options=[10, 25, 50, 100],
                index=1,
                key=f"tamanho_pagina_{nome_colecao}"
            )
        
        chave_pagina = f'pagina_{nome_colecao}'
        if chave_pagina not in st.session_state:
            st.session_state[chave_pagina] = 1
            
        df, total_filtrado = carregar_dados_paginados(
            nome_colecao,
            st.session_state[chave_pagina],
            tamanho_pagina,
            filtros,
            tipos_colunas
        )
        
        total_paginas = math.ceil(total_filtrado / tamanho_pagina) if total_filtrado > 0 else 1
        st.session_state[chave_pagina] = min(st.session_state[chave_pagina], total_paginas)
        
        with col2:
            st.write(f"Total: {total_filtrado} registros | Página {st.session_state[chave_pagina]} de {total_paginas}")
        
        with col3:
            cols_nav = st.columns(4)
            if cols_nav[0].button("⏪", key=f"first_{nome_colecao}"):
                st.session_state[chave_pagina] = 1
                st.rerun()
            if cols_nav[1].button("◀️", key=f"prev_{nome_colecao}"):
                st.session_state[chave_pagina] = max(1, st.session_state[chave_pagina] - 1)
                st.rerun()
            if cols_nav[2].button("▶️", key=f"next_{nome_colecao}"):
                st.session_state[chave_pagina] = min(total_paginas, st.session_state[chave_pagina] + 1)
                st.rerun()
            if cols_nav[3].button("⏩", key=f"last_{nome_colecao}"):
                st.session_state[chave_pagina] = total_paginas
                st.rerun()

    if not df.empty and colunas_visiveis:
        renderizar_cartoes(df[colunas_visiveis + ['_id']], colunas_visiveis, nome_colecao)
        
        if st.button("📥 Baixar dados filtrados", key=f"download_{nome_colecao}"):
            texto_progresso = "Preparando download..."
            barra_progresso = st.progress(0, text=texto_progresso)
            
            todos_dados = []
            tamanho_lote = 1000
            total_paginas_download = math.ceil(total_filtrado / tamanho_lote)
            
            for pagina in range(1, total_paginas_download + 1):
                progresso = pagina / total_paginas_download
                barra_progresso.progress(progresso, text=f"{texto_progresso} ({pagina}/{total_paginas_download})")
                
                df_pagina, _ = carregar_dados_paginados(nome_colecao, pagina, tamanho_lote, filtros)
                if colunas_visiveis:
                    df_pagina = df_pagina[colunas_visiveis]
                todos_dados.append(df_pagina)
            
            df_completo = pd.concat(todos_dados, ignore_index=True)
            
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                df_completo.to_excel(writer, index=False, sheet_name='Dados')
            
            st.download_button(
                label="💾 Clique para baixar Excel",
                data=buffer.getvalue(),
                file_name=f'{nome_colecao}_dados.xlsx',
                mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
            
            barra_progresso.empty()
    else:
        st.warning("Nenhum dado encontrado com os filtros aplicados")

def initialize_session_state():
    if 'user' not in st.session_state:
        st.session_state.user = {}
        st.switch_page("app.py")

def principal():
    st.set_page_config(
        page_title="Home Admin",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="collapsed"
    )
    
    initialize_session_state()

    with st.sidebar:
        user = st.session_state.user
        name = user.get('name', '')
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
                <div style='font-size: 1.1rem; font-weight: bold; margin-bottom: 0.5rem; display: flex; justify-content: center;'>
                    {name}
                </div>
                <div style='font-size: 0.9rem; color: #666; display: flex; justify-content: center;'>
                    {email}
                </div>
                <div style='font-size: 0.9rem; color: #666; display: flex; justify-content: center;'>
                    {phone}
                </div>
            </div>
            """,
            unsafe_allow_html=True
        )
        
        initials = st.session_state.user.get('initials', '')
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
        
        if st.button("Logout", key="logout_button", type="primary", use_container_width=True):
            for key in st.session_state.keys():
                del st.session_state[key]
            st.switch_page("app.py")

    col1, col2 = st.columns([3, 1], gap="large")
    with col1:
        st.markdown('## **📊 :rainbow[Home Epi]**')

    colecoes = ['reqepi', 'xml', 'nfspdf', 'po']
    abas = st.tabs([colecao.upper() for colecao in colecoes])
    
    for aba, nome_colecao in zip(abas, colecoes):
        with aba:
            exibir_pagina_dados(nome_colecao)
    
    st.divider()
    st.caption("Dashboard de Dados MongoDB v1.0")

if __name__ == "__main__":
    principal()