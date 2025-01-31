import streamlit as st
import pandas as pd
from pymongo import MongoClient
import urllib.parse
import unicodedata
import re
import math
import io
from bson.objectid import ObjectId

def normalizar_string(texto):
    if not isinstance(texto, str):
        return str(texto)
    
    texto = texto.lower()
    texto = ''.join(
        char for char in unicodedata.normalize('NFKD', texto)
        if unicodedata.category(char) != 'Mn'
    )
    
    texto = re.sub(r'[^\w\s]', '', texto)
    
    return texto

def criar_padrao_flexivel(texto):
    texto_normalizado = normalizar_string(texto)
    fragmentos = texto_normalizado.split()
    
    padrao = '.*'.join(
        f'(?=.*{re.escape(fragmento)})' for fragmento in fragmentos
    )
    
    return padrao + '.*'

@st.cache_resource
def obter_cliente_mongodb():
    nome_usuario = st.secrets["MONGO_USERNAME"]
    senha = st.secrets["MONGO_PASSWORD"]
    cluster = st.secrets["MONGO_CLUSTER"]
    nome_banco_dados = st.secrets["MONGO_DB"]
    
    nome_usuario_escapado = urllib.parse.quote_plus(nome_usuario)
    senha_escapada = urllib.parse.quote_plus(senha)
    URI_MONGO = f"mongodb+srv://{nome_usuario_escapado}:{senha_escapada}@{cluster}/{nome_banco_dados}?retryWrites=true&w=majority"
    
    return MongoClient(URI_MONGO)

@st.cache_data
def obter_colunas_colecao(nome_colecao):
    cliente = obter_cliente_mongodb()
    banco_dados = cliente.warehouse
    colecao = banco_dados[nome_colecao]
    
    total_documentos = colecao.count_documents({})
    documento_exemplo = colecao.find_one()
    
    colunas = []
    if documento_exemplo:
        colunas = [col for col in documento_exemplo.keys() if col != '_id']
    
    colunas_padrao = {
        'xml': [],
        'po': [],
        'nfspdf': []
    }
    
    return total_documentos, colunas, colunas_padrao.get(nome_colecao, colunas[:6])

@st.cache_data
def obter_valores_unicos_do_banco_de_dados(nome_colecao, coluna):
    cliente = obter_cliente_mongodb()
    banco_dados = cliente.warehouse
    colecao = banco_dados[nome_colecao]
    
    pipeline = [
        {"$group": {"_id": f"${coluna}"}},
        {"$sort": {"_id": 1}},
        {"$limit": 100000}
    ]
    
    try:
        valores_unicos = [doc["_id"] for doc in colecao.aggregate(pipeline) if doc["_id"] is not None]
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

def obter_colunas_com_tipos(nome_colecao):
    try:
        cliente = obter_cliente_mongodb()
        banco_dados = cliente.warehouse
        colecao = banco_dados[nome_colecao]
        
        documento_exemplo = colecao.find_one()
        
        if not documento_exemplo:
            st.warning(f"Nenhum documento encontrado na coleção {nome_colecao}")
            return {}
        
        def determinar_tipo(valor):
            if valor is None:
                return 'str'
            if isinstance(valor, int):
                return 'int64'
            elif isinstance(valor, float):
                return 'float64'
            elif isinstance(valor, str):
                try:
                    int(valor.replace(',', ''))
                    return 'int64'
                except ValueError:
                    try:
                        float(valor.replace(',', '.'))
                        return 'float64'
                    except ValueError:
                        return 'str'
            return 'str'
        
        tipos_colunas = {}
        for chave, valor in documento_exemplo.items():
            if chave != '_id':
                tipos_colunas[chave] = determinar_tipo(valor)
        
        return tipos_colunas
    
    except Exception as e:
        st.error(f"Erro ao obter tipos de colunas: {str(e)}")
        return {}

def construir_consulta_mongo(filtros, colunas_tipos):
    consulta = {}
    
    for coluna, info_filtro in filtros.items():
        tipo_filtro = info_filtro['type']
        valor_filtro = info_filtro['value']
        
        if not valor_filtro:
            continue
        
        if colunas_tipos.get(coluna, 'str') in ['int64', 'float64']:
            try:
                valor_numerico = converter_para_numerico(valor_filtro)
                
                if isinstance(valor_numerico, (int, float)):
                    consulta[coluna] = valor_numerico
                    continue
            except:
                pass
        
        if tipo_filtro == 'text':
            padrao_flexivel = criar_padrao_flexivel(valor_filtro)
            
            consulta[coluna] = {
                '$regex': padrao_flexivel, 
                '$options': 'i'
            }
        elif tipo_filtro == 'multi':
            if len(valor_filtro) > 0:
                consulta[coluna] = {'$in': valor_filtro}
    
    return consulta

def converter_documento_para_pandas(doc):
    documento_convertido = {}
    for chave, valor in doc.items():
        if isinstance(valor, ObjectId):
            documento_convertido[chave] = str(valor)
        elif isinstance(valor, dict):
            documento_convertido[chave] = converter_documento_para_pandas(valor)
        elif isinstance(valor, list):
            documento_convertido[chave] = [str(item) if isinstance(item, ObjectId) else item for item in valor]
        else:
            documento_convertido[chave] = valor
    return documento_convertido

def criar_interface_filtros(nome_colecao, colunas):
    colunas_tipos = obter_colunas_com_tipos(nome_colecao) or {}
    
    filtros = {}
    texto = ('**Filtros:**')   
    
    with st.expander(label=texto, expanded=False):
        colunas_selecionadas = st.multiselect(
            "Selecione as colunas para filtrar:",
            colunas,
            key=f"filter_cols_{nome_colecao}"
        )
        
        if colunas_selecionadas:
            cols = st.columns(2)
            
            for idx, coluna in enumerate(colunas_selecionadas):
                with cols[idx % 2]:
                    st.markdown(f"#### {coluna}")
                    
                    tipo_coluna = colunas_tipos.get(coluna, 'str')
                    
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
                            filtros[coluna] = {'type': 'text', 'value': valor}
                    else:
                        valores_unicos = obter_valores_unicos_do_banco_de_dados(nome_colecao, coluna)
                        if valores_unicos:
                            selecionados = st.multiselect(
                                "Selecione os valores:",
                                options=valores_unicos,
                                key=f"multi_filter_{nome_colecao}_{coluna}",
                                help="Selecione um ou mais valores para filtrar"
                            )
                            if selecionados:
                                filtros[coluna] = {'type': 'multi', 'value': selecionados}
                    
                    st.markdown("---")
    
    return filtros, colunas_tipos

def carregar_dados_paginados(nome_colecao, pagina, tamanho_pagina, filtros=None, colunas_tipos=None):
    if colunas_tipos is None:
        colunas_tipos = obter_colunas_com_tipos(nome_colecao)
    
    cliente = obter_cliente_mongodb()
    banco_dados = cliente.warehouse
    colecao = banco_dados[nome_colecao]
    
    consulta = construir_consulta_mongo(filtros, colunas_tipos) if filtros else {}
    pular = (pagina - 1) * tamanho_pagina
    
    try:
        ordenacao = [("Data Emissao", -1)] if nome_colecao == 'xml' else None
        
        total_filtrado = colecao.count_documents(consulta)
        
        opcoes_consulta = {'allowDiskUse': True}
        
        if ordenacao:
            try:
                cursor = colecao.find(consulta).sort(ordenacao).skip(pular).limit(tamanho_pagina)
                cursor.with_options(**opcoes_consulta)
            except Exception as sort_error:
                cursor = colecao.find(consulta).skip(pular).limit(tamanho_pagina)
        else:
            cursor = colecao.find(consulta).skip(pular).limit(tamanho_pagina)
        
        documentos = [converter_documento_para_pandas(doc) for doc in cursor]
        
        if documentos:
            df = pd.DataFrame(documentos)
            if '_id' in df.columns:
                df = df.drop('_id', axis=1)
        else:
            df = pd.DataFrame()
            
        return df, total_filtrado
        
    except Exception as e:
        st.error(f"Erro ao carregar dados: {str(e)}")
        return pd.DataFrame(), 0

def renderizar_cards(df, colunas_visiveis):
    def processar_urls(urls):
        if pd.isna(urls):
            return None
        
        if isinstance(urls, str):
            urls = [urls]
        elif not isinstance(urls, list):
            return None
        
        urls_validas = [url for url in urls if url and isinstance(url, str)]
        return urls_validas[0] if urls_validas else None

    def formatar_valor(valor):
        if pd.isna(valor):
            return '-'
        
        if isinstance(valor, (int, float)):
            if float(valor).is_integer():
                return f'{int(valor):,}'.replace(',', '')
            return f'{valor:.2f}'.replace('.', ',')
        
        if isinstance(valor, str):
            valor_limpo = valor.strip().replace(',', '.')
            try:
                num_valor = float(valor_limpo)
                if float(num_valor).is_integer():
                    return f'{int(num_valor):,}'.replace(',', '')
                return f'{num_valor:.2f}'.replace('.', ',')
            except ValueError:
                return str(valor)[:30]
        
        return str(valor)[:30]

    colunas_imagens = [col for col in df.columns if 'url_imagens' in col.lower()]
    num_colunas = 4
    linhas = [df.iloc[i:i+num_colunas] for i in range(0, len(df), num_colunas)]
    
    for linha in linhas:
        cols = st.columns(num_colunas)
        
        for idx, (_, registro) in enumerate(linha.iterrows()):
            with cols[idx]:
                url_imagem = None
                for col_img in colunas_imagens:
                    url_imagem = processar_urls(registro.get(col_img))
                    if url_imagem:
                        break
                
                detalhes_card = ''.join([
                    f'<div style="margin-bottom: 4px; font-size: 0.75rem;"><strong>{col}:</strong> {formatar_valor(registro.get(col, "-"))}' 
                    for col in colunas_visiveis if col not in colunas_imagens
                ])
                
                card_style = (
                    "border: 1px solid #e0e0e0; border-radius: 8px; "
                    "box-shadow: 0 4px 6px rgba(0,0,0,0.1); margin-bottom: 10px; "
                    "overflow: hidden; transition: transform 0.2s;"
                )
                
                if url_imagem:
                    card_html = f"""
                    <div style='{card_style}' onmouseover="this.style.transform='scale(1.02)'" onmouseout="this.style.transform='scale(1)'">
                        <img src="{url_imagem}" style="width:100%; height:200px; object-fit:cover;">
                        <div style='padding: 8px; font-size: 0.8rem;'>
                            {detalhes_card}
                        </div>
                    </div>
                    """
                else:
                    card_html = f"""
                    <div style='{card_style}' onmouseover="this.style.transform='scale(1.02)'" onmouseout="this.style.transform='scale(1)'">
                        <div style='padding: 8px; font-size: 0.8rem;'>
                            {detalhes_card}
                        </div>
                    </div>
                    """
                
                st.markdown(card_html, unsafe_allow_html=True)

def exibir_pagina_dados(nome_colecao):
    total_documentos, colunas, colunas_visiveis_padrao = obter_colunas_colecao(nome_colecao)
    
    if total_documentos == 0:
        st.error(f"Nenhum documento encontrado na coleção {nome_colecao}")
        return

    with st.expander("**Colunas Visíveis:**", expanded=False):
        if f'colunas_visiveis_{nome_colecao}' not in st.session_state:
            colunas_imagens = [col for col in colunas if 'url_imagens' in col.lower()]
            outras_colunas = [col for col in colunas if 'url_imagens' not in col.lower()]
            st.session_state[f'colunas_visiveis_{nome_colecao}'] = (
                colunas_imagens + 
                [col for col in colunas_visiveis_padrao if col not in colunas_imagens] + 
                outras_colunas
            )[:10]
            
        colunas_visiveis = st.multiselect(
            "Selecione as colunas para exibir:",
            options=colunas,
            default=st.session_state[f'colunas_visiveis_{nome_colecao}'],
            key=f'seletor_colunas_{nome_colecao}'
        )
        st.session_state[f'colunas_visiveis_{nome_colecao}'] = colunas_visiveis
        
        if st.button("Restaurar Colunas Padrão", key=f"restaurar_colunas_{nome_colecao}"):
            st.session_state[f'colunas_visiveis_{nome_colecao}'] = (
                [col for col in colunas if 'url_imagens' in col.lower()] + 
                colunas_visiveis_padrao
            )[:10]
            st.rerun()
    
    filtros, colunas_tipos = criar_interface_filtros(nome_colecao, colunas)
    
    with st.expander("**Configurações:**", expanded=False):
        col1, col2, col3 = st.columns([1,1,1], gap='small')
        
        with col1:
            c1, c2 = st.columns([2, 1])
            c1.write('Registros por página:')
            tamanho_pagina = c2.selectbox(
                "Registros por página:",
                options=[10, 25, 50, 100, 1000],
                index=1,
                key=f"tamanho_pagina_{nome_colecao}",
                label_visibility='collapsed'
            )
        
        if f'pagina_{nome_colecao}' not in st.session_state:
            st.session_state[f'pagina_{nome_colecao}'] = 1
        pagina_atual = st.session_state[f'pagina_{nome_colecao}']
        
        df, total_filtrado = carregar_dados_paginados(
            nome_colecao, 
            pagina_atual, 
            tamanho_pagina, 
            filtros, 
            colunas_tipos
        )
        
        if not df.empty and colunas_visiveis:
            df = df[colunas_visiveis]
        
        total_paginas = math.ceil(total_filtrado / tamanho_pagina) if total_filtrado > 0 else 1
        pagina_atual = min(pagina_atual, total_paginas)
        
        with col2:
            st.write(f"Total: {total_filtrado} registros | Página {pagina_atual} de {total_paginas}")
        
        with col3:
            cols = st.columns(4)
            navegacao_callbacks = {
                "⏪": lambda: st.session_state.update({f'pagina_{nome_colecao}': 1}),
                "◀️": lambda: st.session_state.update({f'pagina_{nome_colecao}': max(1, pagina_atual - 1)}),
                "▶️": lambda: st.session_state.update({f'pagina_{nome_colecao}': min(total_paginas, pagina_atual + 1)}),
                "⏩": lambda: st.session_state.update({f'pagina_{nome_colecao}': total_paginas})
            }
            
            for idx, (texto, callback) in enumerate(navegacao_callbacks.items()):
                if cols[idx].button(texto, key=f"{texto}_{nome_colecao}"):
                    callback()
                    st.rerun()

    if not df.empty:
        renderizar_cards(df, colunas_visiveis)
        
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
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as escritor:
                df_completo.to_excel(escritor, index=False, sheet_name='Dados')
            
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
        st.switch_page("app.py")

def principal():
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

    colecoes = ['xml', 'nfspdf', 'po']
    abas = st.tabs([colecao.upper() for colecao in colecoes])
    
    for aba, nome_colecao in zip(abas, colecoes):
        with aba:
            exibir_pagina_dados(nome_colecao)
    
    st.divider()
    st.caption("Dashboard de Dados MongoDB v1.0")

if __name__ == "__main__":
    principal()