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

def normalize_string_edit(texto):
    """
    Normaliza uma string removendo acentos e caracteres especiais
    """
    if not isinstance(texto, str):
        return str(texto)
    texto = texto.lower()
    texto = ''.join(
        char for char in unicodedata.normalize('NFKD', texto)
        if unicodedata.category(char) != 'Mn'
    )
    return re.sub(r'[^\w\s]', '', texto)

def criar_padrao_flexivel_edit(texto):
    """
    Cria um padrão de busca flexível para pesquisa case-insensitive
    """
    normalizado = normalize_string_edit(texto)
    fragmentos = normalizado.split()
    padrao = '.*'.join(f'(?=.*{re.escape(fragmento)})' for fragmento in fragmentos)
    return padrao + '.*'

@st.cache_resource
def obter_cliente_mongodb_edit():
    """
    Estabelece conexão com o MongoDB usando credenciais do Streamlit
    """
    usuario = st.secrets["MONGO_USERNAME"]
    senha = st.secrets["MONGO_PASSWORD"]
    cluster = st.secrets["MONGO_CLUSTER"]
    banco = st.secrets["MONGO_DB"]
    
    usuario_escaped = urllib.parse.quote_plus(usuario)
    senha_escaped = urllib.parse.quote_plus(senha)
    URI = f"mongodb+srv://{usuario_escaped}:{senha_escaped}@{cluster}/{banco}?retryWrites=true&w=majority"
    
    return MongoClient(URI)

@st.cache_data
def obter_colunas_edit_colecao_edit_edit(nome_colecao_edit):
    """
    Obtém as colunas_edit disponíveis em uma coleção do MongoDB
    """
    cliente = obter_cliente_mongodb_edit()
    db = cliente.warehouse
    colecao_edit = db[nome_colecao_edit]
    
    total_docs = colecao_edit.count_documents({})
    doc_exemplo = colecao_edit.find_one()
    
    colunas_edit = []
    if doc_exemplo:
        colunas_edit = [col for col in doc_exemplo.keys() if col != '_id']
    
    colunas_edit_padrao = {
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
        'nfspdf': [
            'Competencia', 
            'CNPJ Prestador'
        ],
        'po': [
            'Item', 
            'Supplier',
        ]
    }
    
    fallback = colunas_edit_padrao.get(nome_colecao_edit, colunas_edit[:26])
    padroes_finais = [col for col in fallback if col in colunas_edit]
    
    if not padroes_finais:
        padroes_finais = colunas_edit[:10]
    
    return total_docs, colunas_edit, padroes_finais

class GerenciadorCartoes:
    """
    Gerencia as operações de cartões (cards) na interface
    """
    def __init__(self, nome_colecao_edit):
        self.nome_colecao_edit = nome_colecao_edit
        if 'cartoes_edicao' not in st.session_state:
            st.session_state.cartoes_edicao = set()
        if 'cartoes_exclusao' not in st.session_state:
            st.session_state.cartoes_exclusao = set()

    def obter_colecao_edit_edit(self):
        cliente = obter_cliente_mongodb_edit()
        return cliente.warehouse[self.nome_colecao_edit]

    def atualizar_documento_edit(self, id_cartao, dados):
        """
        Atualiza um documento no MongoDB
        """
        try:
            colecao_edit = self.obter_colecao_edit_edit()
            colecao_edit.update_one(
                {"_id": ObjectId(id_cartao)},
                {"$set": dados}
            )
            return True, "Registro atualizado com sucesso!"
        except Exception as e:
            return False, f"Erro na atualização: {str(e)}"

    def excluir_documento_edit(self, id_cartao):
        """
        Exclui um documento do MongoDB
        """
        try:
            colecao_edit = self.obter_colecao_edit_edit()
            colecao_edit.delete_one({"_id": ObjectId(id_cartao)})
            return True, "Registro excluído com sucesso!"
        except Exception as e:
            return False, f"Erro na exclusão: {str(e)}"

    def renderizar_modal_edicao_edit(self, id_cartao, registro, colunas_edit_visiveis, colunas_edit_imagem):
        """
        Renderiza o modal de edição de um cartão
        """
        with st.container():
            dados_editados = {}
            for col in colunas_edit_visiveis:
                valor_atual = registro.get(col, "")
                
                if col == '_id':
                    st.text_input(
                        "ID do Documento",
                        value=valor_atual,
                        key=f"edit_{self.nome_colecao_edit}_{id_cartao}_{col}",
                        disabled=True
                    )
                    continue
                
                dados_editados[col] = st.text_input(
                    col,
                    valor_atual,
                    key=f"edit_{self.nome_colecao_edit}_{id_cartao}_{col}"
                )
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("Salvar", type="primary", key=f"save_{self.nome_colecao_edit}_{id_cartao}"):
                    sucesso, mensagem = self.atualizar_documento_edit(id_cartao, dados_editados)
                    if sucesso:
                        st.success(mensagem)
                        st.session_state.cartoes_edicao.remove(id_cartao)
                        st.rerun()
                    else:
                        st.error(mensagem)
            with col2:
                if st.button("Cancelar", key=f"cancel_edit_{self.nome_colecao_edit}_{id_cartao}"):
                    st.session_state.cartoes_edicao.remove(id_cartao)
                    st.rerun()

    def renderizar_modal_exclusao_edit(self, id_cartao):
        """
        Renderiza o modal de confirmação de exclusão
        """
        dialog = st.container()
        
        with dialog:
            st.warning("Tem certeza que deseja excluir este registro?")
            col1, col2 = st.columns(2)
            with col1:
                if st.button(
                    "Sim, Excluir",
                    type="primary",
                    key=f"confirm_delete_{self.nome_colecao_edit}_{id_cartao}"
                ):
                    sucesso, mensagem = self.excluir_documento_edit(id_cartao)
                    if sucesso:
                        st.success(mensagem)
                        st.session_state.cartoes_exclusao.remove(id_cartao)
                        st.rerun()
                    else:
                        st.error(mensagem)
            with col2:
                if st.button(
                    "Cancelar",
                    key=f"cancel_delete_{self.nome_colecao_edit}_{id_cartao}"
                ):
                    st.session_state.cartoes_exclusao.remove(id_cartao)
                    st.rerun()

def obter_valores_unicos_edit(nome_colecao_edit, coluna):
    """
    Obtém valores únicos de uma coluna para filtros
    """
    cliente = obter_cliente_mongodb_edit()
    colecao_edit = cliente.warehouse[nome_colecao_edit]
    
    pipeline = [
        {"$group": {"_id": f"${coluna}"}},
        {"$sort": {"_id": 1}},
        {"$limit": 100000}
    ]
    
    try:
        valores_unicos = [
            doc["_id"] for doc in colecao_edit.aggregate(pipeline)
            if doc["_id"] is not None
        ]
        return sorted(valores_unicos)
    except Exception as e:
        st.error(f"Erro ao obter valores únicos para {coluna}: {str(e)}")
        return []

def converter_para_numerico_edit(valor):
    """
    Converte um valor para numérico, se possível
    """
    valor_limpo = str(valor).strip().replace(',', '.')
    try:
        return int(valor_limpo)
    except ValueError:
        try:
            return float(valor_limpo)
        except ValueError:
            return valor

def obter_tipos_colunas_edit_edit(nome_colecao_edit):
    """
    Determina os tipos de dados das colunas_edit
    """
    try:
        cliente = obter_cliente_mongodb_edit()
        colecao_edit = cliente.warehouse[nome_colecao_edit]
        doc_exemplo = colecao_edit.find_one()
        
        if not doc_exemplo:
            st.warning(f"Nenhum documento encontrado na coleção {nome_colecao_edit}")
            return {}
        
        def determinar_tipo_edit(valor):
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
        
        return {k: determinar_tipo_edit(v) for k, v in doc_exemplo.items() if k != '_id'}
    except Exception as e:
        st.error(f"Erro ao obter tipos de colunas_edit: {str(e)}")
        return {}

def construir_query_mongo_edit(filtros, tipos_colunas_edit):
    """
    Constrói a query do MongoDB baseada nos filtros aplicados
    """
    query = {}
    
    for coluna, info_filtro in filtros.items():
        tipo_filtro = info_filtro['tipo']
        valor_filtro = info_filtro['valor']
        
        if not valor_filtro:
            continue
        
        if tipos_colunas_edit.get(coluna, 'str') in ['int64', 'float64']:
            try:
                valor_numerico = converter_para_numerico_edit(valor_filtro)
                if isinstance(valor_numerico, (int, float)):
                    query[coluna] = valor_numerico
                    continue
            except:
                pass
        
        if tipo_filtro == 'texto':
            padrao = criar_padrao_flexivel_edit(valor_filtro)
            query[coluna] = {'$regex': padrao, '$options': 'i'}
        elif tipo_filtro == 'multi':
            if valor_filtro:
                query[coluna] = {'$in': valor_filtro}
    
    return query

def criar_interface_filtros_edit(nome_colecao_edit, colunas_edit):
    """
    Cria a interface de filtros para a coleção
    """
    tipos_colunas_edit = obter_tipos_colunas_edit_edit(nome_colecao_edit)
    filtros = {}
    
    with st.expander("**Filtros:**", expanded=False):
        colunas_edit_selecionadas = st.multiselect(
            "Selecione as Colunas para filtrar:",
            colunas_edit,
            key=f"filter_cols_{nome_colecao_edit}"
        )
        
        if colunas_edit_selecionadas:
            cols = st.columns(2)
            for idx, coluna in enumerate(colunas_edit_selecionadas):
                with cols[idx % 2]:
                    st.markdown(f"#### {coluna}")
                    tipo_coluna = tipos_colunas_edit.get(coluna, 'str')
                    tipo_filtro = st.radio(
                        "Tipo de filtro:",
                        ["Texto", "Seleção Múltipla"],
                        key=f"radio_{nome_colecao_edit}_{coluna}",
                        horizontal=True
                    )
                    
                    if tipo_filtro == "Texto":
                        valor = st.text_input(
                            f"Buscar {coluna}" + (" (numérico)" if tipo_coluna in ['int64', 'float64'] else ""),
                            key=f"text_filter_{nome_colecao_edit}_{coluna}"
                        )
                        if valor:
                            filtros[coluna] = {'tipo': 'texto', 'valor': valor}
                    else:
                        valores_unicos = obter_valores_unicos_edit(nome_colecao_edit, coluna)
                        if valores_unicos:
                            selecionados = st.multiselect(
                                "Selecione os valores:",
                                options=valores_unicos,
                                key=f"multi_filter_{nome_colecao_edit}_{coluna}"
                            )
                            if selecionados:
                                filtros[coluna] = {'tipo': 'multi', 'valor': selecionados}
                    
                    st.markdown("---")
    
    return filtros, tipos_colunas_edit

def converter_para_pandas_edit(doc):
    """
    Converte um documento do MongoDB para formato compatível com Pandas
    """
    convertido = {}
    for chave, valor in doc.items():
        if isinstance(valor, ObjectId):
            convertido[chave] = str(valor)
        elif isinstance(valor, dict):
            convertido[chave] = converter_para_pandas_edit(valor)
        elif isinstance(valor, list):
            convertido[chave] = [str(item) if isinstance(item, ObjectId) else item for item in valor]
        else:
            convertido[chave] = valor
    return convertido

def carregar_dados_paginados_edit(nome_colecao_edit, pagina, tamanho_pagina, filtros=None, tipos_colunas_edit=None):
    """
    Carrega dados paginados da coleção do MongoDB
    """
    if tipos_colunas_edit is None:
        tipos_colunas_edit = obter_tipos_colunas_edit_edit(nome_colecao_edit)
    
    cliente = obter_cliente_mongodb_edit()
    colecao_edit = cliente.warehouse[nome_colecao_edit]
    query = construir_query_mongo_edit(filtros, tipos_colunas_edit) if filtros else {}
    pular = (pagina - 1) * tamanho_pagina
    
    try:
        ordenacao = [("Data Emissao", -1)] if nome_colecao_edit == 'xml' else None
        total_filtrado = colecao_edit.count_documents(query)
        
        if ordenacao:
            try:
                cursor = colecao_edit.find(query).sort(ordenacao).skip(pular).limit(tamanho_pagina)
            except Exception:
                cursor = colecao_edit.find(query).skip(pular).limit(tamanho_pagina)
        else:
            cursor = colecao_edit.find(query).skip(pular).limit(tamanho_pagina)
        
        documentos = [converter_para_pandas_edit(doc) for doc in cursor]
        df = pd.DataFrame(documentos)
        
        return df, total_filtrado
    except Exception as e:
        st.error(f"Erro ao carregar dados: {str(e)}")
        return pd.DataFrame(), 0

def processar_urls_edit(urls):
    """
    Processa URLs de imagens para exibição
    """
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

def formatar_valor_edit(valor):
    """
    Formata valores para exibição nos cartões
    """
    if isinstance(valor, pd.Series):
        valor = valor.iloc[0]
    if pd.isna(valor):
        return '-'
    if isinstance(valor, (int, float)):
        if float(valor).is_integer():
            return f'{int(valor):,}'.replace(',', '.')
        return f'{valor:.2f}'.replace('.', ',')
    if isinstance(valor, str):
        valor_limpo = valor.strip().replace(',', '.')
        try:
            valor_num = float(valor_limpo)
            if float(valor_num).is_integer():
                return f'{int(valor_num):,}'.replace(',', '.')
            return f'{valor_num:.2f}'.replace('.', ',')
        except ValueError:
            return str(valor)[:35]
    return str(valor)[:35]

def renderizar_cartoes_edit(df, colunas_edit_visiveis, nome_colecao_edit):
    """
    Renderiza os cartões de dados na interface
    """
    gerenciador = GerenciadorCartoes(nome_colecao_edit)
    colunas_edit_imagem = [col for col in df.columns if 'url_imagens' in col.lower()]
    num_colunas_edit = 5
    linhas = [df.iloc[i:i+num_colunas_edit] for i in range(0, len(df), num_colunas_edit)]
    
    for idx_linha, linha in enumerate(linhas):
        cols = st.columns(num_colunas_edit)
        
        for idx_col, (_, registro) in enumerate(linha.iterrows()):
            with cols[idx_col]:
                url_imagem = None
                for col_img in colunas_edit_imagem:
                    if col_img in registro:
                        url_imagem = processar_urls_edit(registro[col_img])
                        if url_imagem:
                            break
                #Selecione as colunas_edit para filtrar
                # Conteúdo do Cartão
                detalhes_cartao = ''.join([
                    f'<div style="margin-bottom: 4px; font-size: 0.8rem;">'
                    f'<strong>{col}:</strong> {formatar_valor_edit(registro.get(col, "-"))}</div>'
                    for col in colunas_edit_visiveis 
                    if col not in colunas_edit_imagem and col != '_id'
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
                    if st.button(
                        "✏️ Editar",
                        key=f"edit_btn_{nome_colecao_edit}_{registro['_id']}",
                        use_container_width=True
                    ):
                        st.session_state.cartoes_edicao.add(str(registro['_id']))
                with col2:
                    if st.button(
                        "🗑️ Excluir",
                        key=f"delete_btn_{nome_colecao_edit}_{registro['_id']}",
                        use_container_width=True
                    ):
                        st.session_state.cartoes_exclusao.add(str(registro['_id']))
                
                # Modais
                id_cartao = str(registro['_id'])
                if id_cartao in st.session_state.cartoes_edicao:
                    gerenciador.renderizar_modal_edicao_edit(id_cartao, registro, colunas_edit_visiveis + ['_id'], colunas_edit_imagem)
                if id_cartao in st.session_state.cartoes_exclusao:
                    gerenciador.renderizar_modal_exclusao_edit(id_cartao)

def exibir_pagina_dados_edit(nome_colecao_edit):
    """
    Exibe a página principal_edit de dados para uma coleção
    """
    total_documentos, colunas_edit, padrao_visiveis = obter_colunas_edit_colecao_edit_edit(nome_colecao_edit)
    
    colunas_edit = [col for col in colunas_edit if col != '_id']
    padrao_visiveis = [col for col in padrao_visiveis if col != '_id']
    
    if total_documentos == 0:
        st.error(f"Nenhum documento encontrado na coleção {nome_colecao_edit}")
        return

    with st.expander("**Colunas Visíveis:**", expanded=False):
        chave_estado = f'colunas_edit_visiveis_{nome_colecao_edit}'
        if chave_estado not in st.session_state:
            st.session_state[chave_estado] = padrao_visiveis
            
        colunas_edit_visiveis = st.multiselect(
            "Selecione as Colunas para exibir:",
            options=colunas_edit,
            default=st.session_state[chave_estado],
            key=f'seletor_colunas_edit_{nome_colecao_edit}'
        )
        st.session_state[chave_estado] = colunas_edit_visiveis
        
        if st.button("Mostrar Todas as Colunas", key=f"mostrar_todas_{nome_colecao_edit}"):
            st.session_state[chave_estado] = colunas_edit
            st.rerun()

    filtros, tipos_colunas_edit = criar_interface_filtros_edit(nome_colecao_edit, colunas_edit)
    
    with st.expander("**Configurações:**", expanded=False):
        col1, col2, col3 = st.columns([1,1,1], gap='small')
        
        with col1:
            c1, c2 = st.columns([2, 1])
            c1.write('Registros por página:')
            tamanho_pagina = c2.selectbox(
                "Registros por página:",
                options=[10, 25, 50, 100],
                index=1,
                key=f"tamanho_pagina_{nome_colecao_edit}",
                label_visibility='collapsed'
            )
        
        chave_pagina = f'pagina_{nome_colecao_edit}'
        if chave_pagina not in st.session_state:
            st.session_state[chave_pagina] = 1
        pagina_atual = st.session_state[chave_pagina]
        
        df, total_filtrado = carregar_dados_paginados_edit(
            nome_colecao_edit,
            pagina_atual,
            tamanho_pagina,
            filtros,
            tipos_colunas_edit
        )
        
        if not df.empty and colunas_edit_visiveis:
            df = df[colunas_edit_visiveis + ['_id']]
        
        total_paginas = math.ceil(total_filtrado / tamanho_pagina) if total_filtrado > 0 else 1
        pagina_atual = min(pagina_atual, total_paginas)
        
        with col2:
            st.write(f"Total: {total_filtrado} registros | Página {pagina_atual} de {total_paginas}")
        
        with col3:
            cols = st.columns(4)
            navegacao = {
                "⏪": lambda: st.session_state.update({chave_pagina: 1}),
                "◀️": lambda: st.session_state.update({chave_pagina: max(1, pagina_atual - 1)}),
                "▶️": lambda: st.session_state.update({chave_pagina: min(total_paginas, pagina_atual + 1)}),
                "⏩": lambda: st.session_state.update({chave_pagina: total_paginas})
            }
            
            for idx, (texto, callback) in enumerate(navegacao.items()):
                if cols[idx].button(texto, key=f"{texto}_{nome_colecao_edit}"):
                    callback()
                    st.rerun()

    if not df.empty:
        renderizar_cartoes_edit(df, colunas_edit_visiveis, nome_colecao_edit)
        
        if st.button("📥 Baixar dados filtrados", key=f"download_{nome_colecao_edit}"):
            texto_progresso = "Preparando download..."
            barra_progresso = st.progress(0, text=texto_progresso)
            
            todos_dados = []
            tamanho_lote = 1000
            total_paginas_download = math.ceil(total_filtrado / tamanho_lote)
            
            for pagina in range(1, total_paginas_download + 1):
                progresso = pagina / total_paginas_download
                barra_progresso.progress(progresso, text=f"{texto_progresso} ({pagina}/{total_paginas_download})")
                
                df_pagina, _ = carregar_dados_paginados_edit(nome_colecao_edit, pagina, tamanho_lote, filtros)
                if colunas_edit_visiveis:
                    df_pagina = df_pagina[colunas_edit_visiveis]
                todos_dados.append(df_pagina)
            
            df_completo = pd.concat(todos_dados, ignore_index=True)
            
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                df_completo.to_excel(writer, index=False, sheet_name='Dados')
            
            st.download_button(
                label="💾 Clique para baixar Excel",
                data=buffer.getvalue(),
                file_name=f'{nome_colecao_edit}_dados.xlsx',
                mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
            
            barra_progresso.empty()
    else:
        st.warning("Nenhum dado encontrado com os filtros aplicados")

def initialize_session_state_edit():
    """Initialize session state variables"""
    if 'user' not in st.session_state:
        st.session_state.user = {
            st.switch_page("app.py")      
        }




def principal_edit():
    st.set_page_config(
        page_title="Home Admin",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="collapsed"       
    )
    # Initialize session state
    initialize_session_state_edit()

    with st.sidebar:    
        # Get user info from session state
        user = st.session_state.user
        name = user.get('name',)
        email = user.get('email', '')
        phone = user.get('phone', '')
        
        # Create a container for user info with custom styling
        st.markdown(
            f"""
            <div style='
                padding: 1rem;
                border-radius: 0.5rem;
                background-color: #f0f2f6;
                margin-bottom: 1rem;
            '>
                <div style='font-size: 1.1rem; font-weight: bold; margin-bottom: 0.5rem;                 display: flex;
                justify-content: center;'>
                    {name}
                </div>
                <div style='font-size: 0.9rem; color: #666;                display: flex;
                justify-content: center;'>
                     {email}  
                </div>
                <div style='font-size: 0.9rem; color: #666;                display: flex;
                justify-content: center;'>
                     {phone}  
                </div>
            </div>
            """,
            unsafe_allow_html=True
        )
    # Enhanced initials display with gradient background
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
        
        # Add the logout button
        if st.button(
            "Logout",
            key="logout_button",
            type="primary",
            use_container_width=True
        ):
            # Limpar todos os estados da sessão
            for key in st.session_state.keys():
                del st.session_state[key]
            
            # Redirecionar para a página inicial
            st.switch_page("app.py")

    col1, col2 = st.columns([3, 1], gap="large")

    # Coluna 1
    with col1:
        st.markdown('## **📊 :rainbow[Home Admin]**')

    colecoes = ['xml', 'nfspdf', 'po']
    abas = st.tabs([colecao_edit.upper() for colecao_edit in colecoes])
    
    for aba, nome_colecao_edit in zip(abas, colecoes):
        with aba:
            exibir_pagina_dados_edit(nome_colecao_edit)
    
    st.divider()
    st.caption("Dashboard de Dados MongoDB v1.0")

if __name__ == "__main__":
    principal_edit()