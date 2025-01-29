import streamlit as st
import pandas as pd
import os
import xml.etree.ElementTree as ET
from datetime import date,datetime,timedelta
import re
import time
import pickle
import numpy as np
import io
import unicodedata

import polars as pl
from pymongo import MongoClient
import urllib.parse
from bson.objectid import ObjectId

####

# Função para converter ObjectId para strings
def convert_objectid_to_str(documents):
    for document in documents:
        for key, value in document.items():
            if isinstance(value, ObjectId):
                document[key] = str(value)
    return documents

# Função para carregar documentos do MongoDB com valores únicos de "Purchasing Document" e outras colunas
@st.cache_data
def mongo_collection_to_polars_with_unique_documents(mongo_uri, db_name, collection_po, selected_columns):
    # Conectar ao MongoDB
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_po]

    # Construir o pipeline de agregação
    group_stage = {
        "$group": {
            "_id": "$Purchasing Document",  # Agrupar por "Purchasing Document"
            **{col: {"$first": f"${col}"} for col in selected_columns if col != "Purchasing Document"}  # Pegar o primeiro valor das outras colunas
        }
    }

    # Executar o pipeline de agregação
    documents = list(collection.aggregate([group_stage]))

    # Ajustar os resultados (renomear '_id' para "Purchasing Document")
    for doc in documents:
        doc["Purchasing Document"] = doc.pop("_id")

    # Converter ObjectId para strings
    documents = convert_objectid_to_str(documents)

    # Se não houver documentos, retornar um DataFrame vazio
    if not documents:
        return pl.DataFrame()

    # Converter documentos em DataFrame Polars
    try:
        polars_df = pl.DataFrame(documents, infer_schema_length=1000)
    except Exception as e:
        st.error(f"Erro ao criar DataFrame Polars: {e}")
        return pl.DataFrame()

    return polars_df

# Função para carregar documentos do MongoDB com valores únicos de "codigo_projeto" e o primeiro valor de "Project Code"
@st.cache_data
def mongo_collection_to_polars_with_unique_codigo_projeto(mongo_uri, db_name, collection_po):
    # Conectar ao MongoDB
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_po]

    # Construir o pipeline de agregação
    group_stage = {
        "$group": {
            "_id": "$codigo_projeto",  # Agrupar por "codigo_projeto"
            "Project Code": {"$first": "$Project Code"}  # Pegar o primeiro valor de "Project Code"
        }
    }

    # Executar o pipeline de agregação
    documents = list(collection.aggregate([group_stage]))

    # Ajustar os resultados (renomear '_id' para "codigo_projeto")
    for doc in documents:
        doc["codigo_projeto"] = doc.pop("_id")

    # Converter ObjectId para strings
    documents = convert_objectid_to_str(documents)

    # Se não houver documentos, retornar um DataFrame vazio
    if not documents:
        return pl.DataFrame()

    # Converter documentos em DataFrame Polars
    try:
        polars_df = pl.DataFrame(documents, infer_schema_length=1000)
    except Exception as e:
        st.error(f"Erro ao criar DataFrame Polars: {e}")
        return pl.DataFrame()

    return polars_df

selected_col = [
    "tags",
    "grupo",
    "subgrupo",
    "url_imagens"
    # Add other columns as needed
]
@st.cache_data
# Define the selected columns
def mongo_collection_to_polars_category(mongo_uri, db_name, collection_category,selected_col):
    with MongoClient(mongo_uri) as client:
        db = client[db_name]
        collection = db[collection_category]
        
        projection = {col: 1 for col in selected_col}
        documents = list(collection.find({}, projection))
        #documents = list(collection.find({}))

        # Remove _id if it's included by accident
        documents = [{k: v for k, v in doc.items() if k != '_id'} for doc in documents]
        
        # Convert ObjectId to strings
        documents = convert_objectid_to_str(documents)
        
        # Create Polars DataFrame
        if not documents:
            return pl.DataFrame()
        
        try:
            polars_cat = pl.DataFrame(documents, infer_schema_length=1000)
        except Exception as e:
            st.error(f"Error creating Polars DataFrame: {e}")
            return pl.DataFrame()
        
        return polars_cat

# Informações de conexão
username = st.secrets["MONGO_USERNAME"]
password = st.secrets["MONGO_PASSWORD"]
cluster = st.secrets["MONGO_CLUSTER"]
db_name = st.secrets["MONGO_DB"]  # Nome do banco de dados
collection_po = 'po'
collection_category='category'

# Escapar o nome de usuário e a senha
escaped_username = urllib.parse.quote_plus(username)
escaped_password = urllib.parse.quote_plus(password)

# Montar a string de conexão
MONGO_URI = f"mongodb+srv://{escaped_username}:{escaped_password}@{cluster}/{db_name}?retryWrites=true&w=majority"

# Configuração da página no Streamlit
st.set_page_config(
    page_title="XML Invoice Processor",
    page_icon=":page_with_curl:",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Main title
st.header("📃 Processamento de Arquivos XML")

# Carregar os dados do MongoDB
with st.spinner("Carregando dados..."):
    # Definir as colunas desejadas para o primeiro DataFrame
    selected_columns = ["Purchasing Document", "Project Code", "Andritz WBS Element", "codigo_projeto", "Cost Center"]

    # Carregar o primeiro DataFrame com valores únicos de "Purchasing Document"
    polars_po = mongo_collection_to_polars_with_unique_documents(MONGO_URI, db_name, collection_po, selected_columns)

    # Carregar o segundo DataFrame com valores únicos de "codigo_projeto"
    polars_cod_project = mongo_collection_to_polars_with_unique_codigo_projeto(MONGO_URI, db_name, collection_po)

    polars_cat = mongo_collection_to_polars_category(MONGO_URI, db_name, collection_category,selected_col)
    # Mostrar o primeiro DataFrame
    # if not polars_cat.is_empty():
    #     st.write("DataFrame 1 (Com valores únicos de 'Purchasing Document'):")
    #     st.text(len(polars_cat))
    #     st.write(polars_cat)
    # else:
    #     st.write("Nenhum documento encontrado para 'Purchasing Document'.")

    # # Mostrar o segundo DataFrame
    # if not polars_cod_project.is_empty():
    #     st.write("DataFrame 2 (Com valores únicos de 'codigo_projeto'):")
    #     st.text(len(polars_cod_project))
    #     st.write(polars_cod_project)
    # else:
    #     st.write("Nenhum documento encontrado para 'codigo_projeto'.")
###            

def slugify(text):
    """
    Convert a text string into a slug format.
    - Convert to lowercase
    - Remove special characters
    - Replace spaces with hyphens
    - Remove consecutive hyphens
    """
    if not isinstance(text, str):
        text = str(text)
    
    # Convert to lowercase and normalize unicode characters
    text = text.lower()
    text = unicodedata.normalize('NFKD', text)
    text = text.encode('ascii', 'ignore').decode('utf-8')
    
    # Replace any non-alphanumeric character with a hyphen
    text = re.sub(r'[^a-z0-9]+', '-', text)
    
    # Remove leading and trailing hyphens
    text = text.strip('-')
    
    # Replace multiple consecutive hyphens with a single hyphen
    text = re.sub(r'-+', '-', text)
    
    return text

def tag(text):
    """
    Convert a text string into a slug format.
    - Convert to lowercase
    - Remove special characters
    - Replace spaces with hyphens
    - Remove consecutive hyphens
    """
    if not isinstance(text, str):
        text = str(text)
    
    # Convert to lowercase and normalize unicode characters
    text = text.lower()
    text = unicodedata.normalize('NFKD', text)
    text = text.encode('ascii', 'ignore').decode('utf-8')
    
    # Replace any non-alphanumeric character with a hyphen
    text = re.sub(r'[^a-z0-9]+', '-', text)
    
    # Remove leading and trailing hyphens
    text = text.strip(' ')
    
    # Replace multiple consecutive hyphens with a single hyphen
    text = re.sub(r'-+', ' ', text)
    
    return text

def clean_description(description):
    """Remove múltiplos espaços consecutivos e espaços no início e no final da string."""
    if description is None:
        return ""
    description = re.sub(' +', ' ', description)
    description = description.strip()
    return description

# def filter_info_adic(info_adic):
#     """Filtra a informação adicional para encontrar prefixos específicos e formata o texto resultante."""
#     if not info_adic:
#         return ""
#     prefixos = ['4501', '4502', '4503', '4504', '4505']
#     palavras = info_adic.split()
#     palavras_filtradas = [p[:10] for p in palavras if any(p.startswith(prefix) for prefix in prefixos)]
#     if palavras_filtradas:
#         return ' '.join(palavras_filtradas)
#     return ""

# def filter_info_adic(info_adic):
#     """
#     Filtra a informação adicional para encontrar prefixos específicos e formata o texto resultante.
    
#     Parameters:
#     info_adic (str): A string contendo a informação adicional a ser filtrada.
    
#     Returns:
#     str: Uma string contendo as palavras que começam com os prefixos desejados, limitadas a 10 caracteres.
#     """
#     if not info_adic:
#         return ""
    
#     prefixos = ['4501', '4502', '4503', '4504']
#     palavras = info_adic.split()
#     palavras_filtradas = []
    
#     for palavra in palavras:
#         for prefixo in prefixos:
#             if palavra.startswith(prefixo):
#                 palavras_filtradas.append(palavra[:10])
#                 break
    
#     if palavras_filtradas:
#         return ' '.join(palavras_filtradas)
#     else:
#         return ""
    
def extract_numbers(text):
   """
   Extrai números que começam com 4501-4506, retornando apenas 6 dígitos apos o prefixo.
   
   Parameters:
   text (str): O texto onde procurar os números
   
   Returns:
   str: String com os números encontrados separados por espaço
   """
   import re
   
   if not text or not isinstance(text, str):
       return ""
   
   # Primeiro encontra números começando com 450[1-6] e qualquer quantidade de dígitos após
   pattern = r'(450[1-6]\d{6,})'
   matches = re.findall(pattern, text)
   
   # Processa cada match para garantir apenas 6 dígitos após o prefixo
   processed_numbers = []
   for number in matches:
       # Pega apenas os primeiros 10 dígitos (4 do prefixo + 6 dígitos)
       truncated = number[:10]
       processed_numbers.append(truncated)
   
   # Remove duplicatas mantendo a ordem
   unique_numbers = list(dict.fromkeys(processed_numbers))
   
   # Retorna os primeiros 10 números encontrados
   return ' '.join(unique_numbers[:10]) if unique_numbers else ""

def extract_code(text):
   """
   Extrai apenas os 6 dígitos do padrão X-XX-XXXXXX-XXX-XXXX-XXX, onde X pode ser letra ou número.
   
   Parameters:
   text (str): O texto onde procurar os códigos
   
   Returns:
   str: String apenas com os 6 dígitos ou vazio se não encontrar
   """
   import re
   
   if not text or not isinstance(text, str):
       return ""
   
   # Padrão para capturar 6 dígitos após qualquer letra/número e traço
   pattern = r'[A-Z0-9]-[A-Z0-9]{2}-(\d{6})-\d{3}-\d{4}-\d{3}'
   
   # Encontra o match no texto
   match = re.search(pattern, text)
   
   # Retorna apenas os 6 dígitos se encontrar
   return match.group(1) if match else ""

# Para usar no DataFrame:
# df['numero_projeto'] = df['info_adic'].apply(extract_code)

def format_value(value_str):
    """Formata o valor substituindo vírgulas por pontos e convertendo para float se possível."""
    if isinstance(value_str, str):
        value_str = value_str.replace('.', '').replace(',', '.')
        try:
            return float(value_str)
        except ValueError:
            return value_str
    elif isinstance(value_str, (int, float)):
        return value_str
    return ""

class ReadXML:
    def __init__(self, files):
        self.files = files

    def nfe_data(self, xml_file):
        """Extrai dados da NFe de um arquivo XML e retorna uma lista de dados para cada item da nota fiscal."""
        root = ET.parse(xml_file).getroot()
        nsNFe = {"ns": "http://www.portalfiscal.inf.br/nfe"}
        
# Acessar o elemento <infNFe> e extrair o atributo Id
        infNFe = root.find(".//ns:infNFe", nsNFe)
        if infNFe is not None:
            chNFe = infNFe.attrib.get('Id', '')
        else:
            chNFe = ""

        
        #Dados gerais
        NFe = self.check_none(root.find("./ns:NFe/ns:infNFe/ns:ide/ns:nNF", nsNFe))
        serie = self.check_none(root.find("./ns:NFe/ns:infNFe/ns:ide/ns:serie", nsNFe))
        natOp = self.check_none(root.find("./ns:NFe/ns:infNFe/ns:ide/ns:natOp", nsNFe))
        data_emissao = self.check_none(root.find("./ns:NFe/ns:infNFe/ns:ide/ns:dhEmi", nsNFe))
        #data_emissao = f"{data_emissao[8:10]}/{data_emissao[5:7]}/{data_emissao[:4]}" if data_emissao else ""
        info_adic = self.check_none(root.find("./ns:NFe/ns:infNFe/ns:infAdic/ns:infCpl", nsNFe))
        info_AdFisco = self.check_none(root.find("./ns:NFe/ns:infNFe/ns:infAdic/ns:infAdFisco", nsNFe))
        info_xPed = self.check_none(root.find("./ns:NFe/ns:infNFe/ns:compra/ns:xPed", nsNFe))
        
        #'info_AdFisco','info_xPed'
        
        dVenc = self.check_none(root.find("./ns:NFe/ns:infNFe/ns:cobr/ns:dup/ns:dVenc", nsNFe))
        #dVenc = f"{dVenc[8:10]}/{dVenc[5:7]}/{dVenc[:4]}" if dVenc else ""
        #dVenc = dVenc.replace("//", "")

        # Dados Emitente
        emit = root.find("./ns:NFe/ns:infNFe/ns:emit", nsNFe)
        emit_data = {
            'CNPJ Emitente': self.check_none(emit.find("ns:CNPJ", nsNFe)) if emit is not None else "",
            'Nome Emitente': self.check_none(emit.find("ns:xNome", nsNFe)) if emit is not None else "",
            'IE Emitente': self.check_none(emit.find("ns:IE", nsNFe)) if emit is not None else "",
            'Endereco Emitente': {
                'Logradouro': self.check_none(emit.find("ns:enderEmit/ns:xLgr", nsNFe)) if emit is not None else "",
                'Número': self.check_none(emit.find("ns:enderEmit/ns:nro", nsNFe)) if emit is not None else "",
                'Complemento': self.check_none(emit.find("ns:enderEmit/ns:complemento", nsNFe)) if emit is not None else "",
                'Bairro': self.check_none(emit.find("ns:enderEmit/ns:xBairro", nsNFe)) if emit is not None else "",
                'Município': self.check_none(emit.find("ns:enderEmit/ns:xMun", nsNFe)) if emit is not None else "",
                'UF': self.check_none(emit.find("ns:enderEmit/ns:UF", nsNFe)) if emit is not None else "",
                'CEP': self.check_none(emit.find("ns:enderEmit/ns:CEP", nsNFe)) if emit is not None else "",
                'País': self.check_none(emit.find("ns:enderEmit/ns:cPais", nsNFe)) if emit is not None else ""
            }
        }
        #infCpl
        # Dados Destinatário
        dest = root.find("./ns:NFe/ns:infNFe/ns:dest", nsNFe)
        dest_data = {
            'CNPJ Destinatário': self.check_none(dest.find("ns:CNPJ", nsNFe)) if dest is not None else "",
            'Nome Destinatário': self.check_none(dest.find("ns:xNome", nsNFe)) if dest is not None else "",
            'IE Destinatário': self.check_none(dest.find("ns:IE", nsNFe)) if dest is not None else "",
            'Endereco Destinatário': {
                'Logradouro': self.check_none(dest.find("ns:enderDest/ns:xLgr", nsNFe)) if dest is not None else "",
                'Número': self.check_none(dest.find("ns:enderDest/ns:nro", nsNFe)) if dest is not None else "",
                'Complemento': self.check_none(dest.find("ns:enderDest/ns:complemento", nsNFe)) if dest is not None else "",
                'Bairro': self.check_none(dest.find("ns:enderDest/ns:xBairro", nsNFe)) if dest is not None else "",
                'Município': self.check_none(dest.find("ns:enderDest/ns:xMun", nsNFe)) if dest is not None else "",
                'UF': self.check_none(dest.find("ns:enderDest/ns:UF", nsNFe)) if dest is not None else "",
                'CEP': self.check_none(dest.find("ns:enderDest/ns:CEP", nsNFe)) if dest is not None else "",
                'País': self.check_none(dest.find("ns:enderDest/ns:cPais", nsNFe)) if dest is not None else ""
            }
        }

        # Dados Cobrança
        cobr = root.find("./ns:NFe/ns:infNFe/ns:cobr", nsNFe)
        cobr_data = self.extract_cobr_data(cobr, nsNFe) if cobr is not None else {}

        # Dados do Item
        itemNota = 1
        notas = []

        for item in root.findall("./ns:NFe/ns:infNFe/ns:det", nsNFe):
            # Dados do Item
            cod = self.check_none(item.find(".ns:prod/ns:cProd", nsNFe))
            qntd = self.check_none(item.find(".ns:prod/ns:qCom", nsNFe))
            descricao = self.check_none(item.find(".ns:prod/ns:xProd", nsNFe))
            unidade_medida = self.check_none(item.find(".ns:prod/ns:uCom", nsNFe))
            vlUnProd = self.check_none(item.find(".ns:prod/ns:vUnCom", nsNFe))
            valorProd = self.check_none(item.find(".ns:prod/ns:vProd", nsNFe))
            ncm = self.check_none(item.find(".ns:prod/ns:NCM", nsNFe))
            cfop = self.check_none(item.find(".ns:prod/ns:CFOP", nsNFe))
            xPed = self.check_none(item.find(".ns:prod/ns:xPed", nsNFe))
            nItemPed = self.check_none(item.find(".ns:prod/ns:nItemPed", nsNFe))
            infAdProd = self.check_none(item.find(".ns:infAdProd", nsNFe))

            valorNfe = format_value(self.check_none(root.find("./ns:NFe/ns:infNFe/ns:total/ns:ICMSTot/ns:vNF", nsNFe)))
            valor_frete = format_value(self.check_none(root.find("./ns:NFe/ns:infNFe/ns:total/ns:ICMSTot/ns:vFrete", nsNFe)))
            data_importacao = self.check_none(root.find("./ns:NFe/ns:infNFe/ns:transp/ns:vol/ns:veicId", nsNFe))
            usuario = self.check_none(root.find("./ns:NFe/ns:infNFe/ns:transp/ns:vol/ns:placa", nsNFe))
            data_saida = self.check_none(root.find("./ns:NFe/ns:infNFe/ns:transp/ns:vol/ns:uf", nsNFe))

            dados = [chNFe, NFe, serie, natOp, data_emissao, info_adic, dVenc, info_AdFisco,info_xPed,
                    emit_data['CNPJ Emitente'], emit_data['Nome Emitente'],
                    dest_data['CNPJ Destinatário'], dest_data['Nome Destinatário'], valorNfe, valor_frete, itemNota, cod, qntd, descricao, unidade_medida, vlUnProd, valorProd, ncm, cfop , xPed, nItemPed,
                    infAdProd, data_importacao, usuario, data_saida,
                    cobr_data.get('Fatura', ''), cobr_data.get('Duplicata', ''), cobr_data.get('Valor Original', ''), cobr_data.get('Valor Pago', ''),
                    emit_data['Endereco Emitente']['Logradouro'], emit_data['Endereco Emitente']['Número'], emit_data['Endereco Emitente']['Complemento'],
                    emit_data['Endereco Emitente']['Bairro'], emit_data['Endereco Emitente']['Município'], emit_data['Endereco Emitente']['UF'],
                    emit_data['Endereco Emitente']['CEP'], emit_data['Endereco Emitente']['País'],
                    dest_data['Endereco Destinatário']['Logradouro'], dest_data['Endereco Destinatário']['Número'],
                    dest_data['Endereco Destinatário']['Complemento'], dest_data['Endereco Destinatário']['Bairro'],
                    dest_data['Endereco Destinatário']['Município'], dest_data['Endereco Destinatário']['UF'],
                    dest_data['Endereco Destinatário']['CEP'], dest_data['Endereco Destinatário']['País']]
            
            notas.append(dados)
            itemNota += 1
        
        return notas

    def check_none(self, var):
        """Verifica se o elemento XML é None e retorna uma string vazia, caso contrário, retorna o texto do elemento."""
        if var is None:
            return ""
        try:
            return var.text.replace('.', '.') if var.text else ""
        except:
            return ""

    def extract_cobr_data(self, cobr, nsNFe):
        """Extrai os dados da seção <cobr>, incluindo <fat> e <dup>."""
        if cobr is None:
            return {
                'Fatura': '',
                'Duplicata': '',
                'Valor Original': '',
                'Valor Pago': ''
            }
        
        cobr_data = {
            'Fatura': self.check_none(cobr.find("ns:fat/ns:nFat", nsNFe)),
            'Duplicata': self.check_none(cobr.find("ns:dup/ns:nDup", nsNFe)),
            'Valor Original': format_value(self.check_none(cobr.find("ns:fat/ns:vOrig", nsNFe))),
            'Valor Pago': format_value(self.check_none(cobr.find("ns:fat/ns:vLiq", nsNFe)))
        }
        return cobr_data

    def process_xml_files(self):
        """Processa todos os arquivos XML carregados"""
        dados = []
        for uploaded_file in self.files:
            result = self.nfe_data(uploaded_file)
            dados.extend(result)
        return dados

def main():
    # # Page configuration
    # st.set_page_config(
    #     page_title="XML Invoice Processor", 
    #     page_icon=":page_with_curl:", 
    #     layout="wide"
    # )

    # # Main title
    # st.title("XML Invoice Processor 📄")

    # Create tabs
    tab1, tab2, tab3 = st.tabs(["📤 Upload e Extração", "📊 Visualização de Dados", "❓ Como Utilizar"])

    with tab1:
        # File uploader for XML files
        uploaded_files = st.file_uploader(
            "Upload XML Files", 
            type=['xml'], 
            accept_multiple_files=True
        )

        if uploaded_files:
            # Progress bar
            progress_bar = st.progress(0)
            for percent_complete in range(100):
                time.sleep(0.01)
                progress_bar.progress(percent_complete + 1)
            progress_bar.empty()

            # Process XML files
            xml_reader = ReadXML(uploaded_files)
            dados = xml_reader.process_xml_files()

    # Criando DataFrame Pandas
            df = pd.DataFrame(dados, columns=[
                'chaveNfe', 'NFe', 'Série', 'natOp','Data de Emissão', 'info_adic', 'dVenc', 'info_AdFisco','info_xPed','CNPJ Emitente', 'Nome Emitente',
                'CNPJ Destinatário', 'Nome Destinatário', 'Valor NF-e', 'Valor Frete', 'Item Nota', 'Cód Produto',
                'Quantidade', 'Descrição', 'Unidade Medida', 'vlUnProd', 'vlTotProd', 'ncm', 'cfop' ,'xPed', 'nItemPed',
                'infAdProd', 'Data Importação', 'Usuário', 'Data Saída', 'Fatura', 'Duplicata', 'Valor Original', 'Valor Pago',
                'Logradouro Emitente', 'Número Emitente', 'Complemento Emitente', 'Bairro Emitente', 'Município Emitente',
                'UF Emitente', 'CEP Emitente', 'País Emitente', 'Logradouro Destinatário', 'Número Destinatário',
                'Complemento Destinatário', 'Bairro Destinatário', 'Município Destinatário', 'UF Destinatário',
                'CEP Destinatário', 'País Destinatário'
            ])

            colunas = [
                'chaveNfe', 'NFe', 'Nome Emitente', 'Descrição', 'Série', 'natOp','Data de Emissão', 'info_adic', 'dVenc', 'info_AdFisco','info_xPed',
                'CNPJ Emitente', 'CNPJ Destinatário', 'Nome Destinatário', 'Valor NF-e', 'Valor Frete', 'Item Nota', 
                'Cód Produto', 'Quantidade', 'Unidade Medida', 'vlUnProd', 'vlTotProd', 'ncm', 'cfop', 'xPed', 'nItemPed', 
                'infAdProd', 'Data Importação', 'Usuário', 'Data Saída', 'Fatura', 'Duplicata', 'Valor Original', 
                'Valor Pago', 'Logradouro Emitente', 'Número Emitente', 'Complemento Emitente', 'Bairro Emitente', 
                'Município Emitente', 'UF Emitente', 'CEP Emitente', 'País Emitente', 'Logradouro Destinatário', 
                'Número Destinatário', 'Complemento Destinatário', 'Bairro Destinatário', 'Município Destinatário', 
                'UF Destinatário', 'CEP Destinatário', 'País Destinatário'
            ]

            df = df.reindex(columns=colunas)

            # Create unique identifier using slugify
            df['unique'] = df['NFe'].astype(str) + '-' + df['Item Nota'].astype(str) + '-' + df['Descrição'].astype(str)
            df['unique'] = df['unique'].apply(slugify)
            
            # Remove duplicates based on the slugified unique column
            df.drop_duplicates(subset='unique', inplace=True)

            df['tags'] = df['Descrição'].astype(str)
            df['tags'] = df['tags'].apply(tag)
            
            # df=df_formatted
            def convert_to_decimal(df, columns, decimal_places=2):
                """Converte várias colunas para float (decimais) e arredonda para o número especificado de casas decimais."""
                for column in columns:
                    # Converte para float e força valores inválidos para NaN
                    df[column] = pd.to_numeric(df[column], errors='coerce').astype(float)
                    # Arredonda para o número de casas decimais especificado
                    df[column] = df[column].round(decimal_places)
                return df

            # Supondo que você queira converter as colunas 'preco' e 'quantidade' para decimais com duas casas
            columns_to_convert = ['Quantidade','vlUnProd','vlTotProd']

            # Converter as colunas para decimal (float) com duas casas decimais
            df = convert_to_decimal(df, columns_to_convert, decimal_places=2) 
            
            # Função para formatar os números corretamente, lidando com valores vazios
            def formatar_numero(x):
                if pd.isna(x) or x == '':  # Se o valor for nulo ou string vazia
                    return None  # Retorna None para manter o valor como NaN
                else:
                    x_str = str(int(x))  # Certificar que seja inteiro e sem pontos ou espaços
                    
                    # Garantindo que o número tenha pelo menos dois dígitos
                    if len(x_str) > 2:
                        # Inserindo o ponto decimal entre os dois últimos dígitos
                        return x_str[:-2] + "." + x_str[-2:]
                    else:
                        # Se o número for menor que 100, apenas adiciona '0.xx'
                        return "0." + x_str.zfill(2)


            # Aplicar a função diretamente nas colunas existentes
            colunas_para_formatar = ['Valor NF-e', 'Valor Original', 'Valor Pago']
            
            for coluna in colunas_para_formatar:
                # Formatar os números na coluna e converter para float
                df[coluna] = df[coluna].apply(formatar_numero).astype(float)
                    
            # Agrupando por 'Category' e somando os valores de 'Value'
            df['vlNf'] = df.groupby('chaveNfe')['vlTotProd'].transform('sum')

            df['Descrição'] = df['Descrição'].apply(clean_description).str.upper()
            
            # Aplicar a função para filtrar e formatar a coluna 'info_adic''info_AdFisco','info_xPed'
            df['po'] = df['info_adic'].fillna("") + " " + df['xPed'].fillna("") + " " + df['nItemPed'].fillna("") + " " + df['infAdProd'].fillna("")+ df['info_AdFisco'].fillna("") + " " + df['info_xPed'].fillna("")
            #df['po'] = df['po'].apply(filter_info_adic)
            df['po'] = df['po'].apply(extract_numbers)
            
            # Extrair apenas os números e filtrar para 10 caracteres exatos
            df['po'] = df['po'].apply(lambda x: extract_numbers(x) if len(extract_numbers(x)) == 10 else '')
            
            df['codigo_projeto'] = df['info_adic'].apply(extract_code)
            df['codigo_projeto'] = df['codigo_projeto'].apply(
                lambda x: int(x) if x != "" else ""
            )    
            # # Função para truncar os primeiros 10 caracteres
            # def truncate_to_10_chars(text):
            #     return text[:10] if text else ""

            # df['po'] = df['po'].apply(truncate_to_10_chars)
            # df['po'] = df['po'].apply(lambda x: x if len(x) == 10 else '') 
            
            # Função para obter o primeiro valor não vazio para cada 'chaveNfe'
            def get_first_non_empty_po(df):
                first_non_empty_po = {}
                for chave, po in zip(df['chaveNfe'], df['po']):
                    if pd.notna(po) and po != '' and chave not in first_non_empty_po:
                        first_non_empty_po[chave] = po
                return first_non_empty_po
                

            # Atualiza a coluna 'po' com o primeiro valor não vazio
            first_po_dict = get_first_non_empty_po(df)
            df['po'] = df['chaveNfe'].map(first_po_dict)                   
              
            def format_date_to_brazilian(df, columns):
                """
                Converte as colunas especificadas para o formato de data brasileiro (dd/mm/aaaa).
                
                :param df: DataFrame a ser modificado
                :param columns: Lista de nomes das colunas a serem formatadas
                :return: DataFrame com as colunas formatadas como datas brasileiras
                """
                for column in columns:
                    # First, try converting to datetime, handling different potential input formats
                    try:
                        # Try multiple datetime parsing strategies
                        df[column] = pd.to_datetime(
                            df[column], 
                            format='%Y-%m-%d',  # ISO format
                            errors='coerce'
                        )
                    except:
                        try:
                            df[column] = pd.to_datetime(
                                df[column], 
                                format='%d/%m/%Y',  # Brazilian format
                                errors='coerce'
                            )
                        except:
                            # If conversion fails, leave the column as is
                            continue
                    
                    # Format to Brazilian date string only for non-null values
                    df[column] = df[column].dt.strftime('%d/%m/%Y')
                
                return df

            # Aplicar a formatação desejada
            df = format_date_to_brazilian(df, ['dVenc'])
                                        
            #Função para formatar colunas como moeda brasileira (BRL)
            
            def format_to_brl_currency(df, columns):
                """
                Formata as colunas especificadas do DataFrame para o formato de moeda brasileiro (Real) com duas casas decimais.
                Valores não numéricos ou vazios são substituídos por uma string vazia.
                
                :param df: DataFrame a ser modificado
                :param columns: Lista de nomes das colunas a serem formatadas
                :return: DataFrame com as colunas formatadas como moeda brasileira
                """
                for column in columns:
                    df[column] = pd.to_numeric(df[column], errors='coerce')

                    #df[column] = df[column].apply(lambda x: f'R${x:,.2f}'.replace('.', ',') if pd.notna(x) else '')
                    
                    # Convertendo valores para formato desejado
                    df[column] = df[column].apply(lambda x: 'R$ {:,.2f}'.format(x / 1))
                    df[column] = df[column].str.replace(',', 'X').str.replace('.', ',').str.replace('X', '.')
                return df

            df = df
                            
            def convert_columns_to_numeric(df, columns):
                """Converte várias colunas para numérico, forçando erros para NaN."""
                for column in columns:
                    df[column] = pd.to_numeric(df[column], errors='coerce')
                return df

            # Supondo que você queira converter as colunas 'po' e 'NFe'
            columns_to_convert = ['po','NFe','Série','CNPJ Emitente','CNPJ Destinatário','ncm','cfop','CEP Emitente','País Emitente','CEP Destinatário','País Destinatário'] 
            
            # Converter as colunas relevantes para numérico
            df = convert_columns_to_numeric(df, columns_to_convert)   

            # Ordenar o DataFrame pela coluna 'Data' do mais novo para o mais velho
            df = df.sort_values(by='Data de Emissão', ascending=False)  

            # Selecionando colunas, renomeando e reordenando
            colunas_para_exibir =['chaveNfe','NFe','Nome Emitente','Descrição','Série','natOp','Data de Emissão',
                                    'info_adic','dVenc','CNPJ Destinatário','Nome Destinatário','Valor NF-e',
                                    'Valor Frete','Item Nota','Cód Produto','Quantidade','Unidade Medida','vlUnProd','vlTotProd',
                                    'ncm','cfop','xPed','nItemPed','infAdProd','Data Importação','Usuário','Data Saída','Fatura',
                                    'Duplicata','Valor Original','Valor Pago',
                                    'Logradouro Emitente','Número Emitente','Complemento Emitente','Bairro Emitente','Município Emitente','UF Emitente','CEP Emitente','País Emitente',
                                    'Logradouro Destinatário','Número Destinatário','Complemento Destinatário','Bairro Destinatário','Município Destinatário','UF Destinatário','CEP Destinatário','País Destinatário',
                                    'vlNf','po','tags','unique']
            
            # Renomear as colunas

            df = df.rename(columns={'NFe': 'nNf', 'Data de Emissão': 'dtEmi','Item Nota':'itemNf','Descrição':'nomeMaterial','ncm':'ncm','Quantidade':'qtd',
                            'Unidade Medida':'und','vlUnProd':'vlUnProd','vlTotProd':'vlTotProd','Valor NF-e':'vlTotalNf',
                            'dVenc':'dVenc','po':'po',
                            'chaveNfe':'chNfe',
                            'Nome Emitente': 'emitNome','CNPJ Emitente':'emitCnpj','Logradouro Emitente':'emitLogr','Número Emitente':'emitNr','Complemento Emitente':'emitCompl','Bairro Emitente':'emitBairro','Município Emitente':'emitMunic','UF Emitente':'emitUf','CEP Emitente':'emitCep','País Emitente':'emitPais',
                            'Nome Destinatário': 'destNome','CNPJ Destinatário':'destCnpj','Logradouro Destinatário':'destLogr','Número Destinatário':'destNr','Complemento Destinatário':'destCompl','Bairro Destinatário':'destBairro','Município Destinatário':'destMunic','UF Destinatário':'destUf','CEP Destinatário':'destCep','País Destinatário':'destPais',
                            'cfop':'cfop','tags':'tags','unique':'unique'})

            # Exibir apenas as colunas renomeadas
            colunas_renomeadas = ['nNf', 'dtEmi', 'itemNf','nomeMaterial','ncm','qtd','und','vlUnProd','vlTotProd','vlTotalNf','po','dVenc','chNfe',
                                    'emitNome','emitCnpj','emitLogr','emitNr','emitCompl','emitBairro','emitMunic','emitUf','emitCep','emitPais',
                                    'destNome','destCnpj','destLogr','destNr','destCompl','destBairro','destMunic','destUf','destCep','destPais',
                                    'cfop','tags','unique','codigo_projeto']
            
            df= df[colunas_renomeadas]
            
            
            # Converter as colunas para string
            df['emitCnpj'] = df['emitCnpj'].astype(str).replace('.0','')
            df['destCnpj'] = df['destCnpj'].astype(str).replace('.0','')

            # Garantir que as colunas tenham 14 dígitos
            df['emitCnpj'] = df['emitCnpj'].str.zfill(14)
            df['destCnpj'] = df['destCnpj'].str.zfill(14)
            
            st.write(f"Quantidade de linhas: {df.shape[0]}")
                       
            # Create a new column counting invoices per PO
            po_invoice_counts = df.groupby('po')['chNfe'].nunique().reset_index()
            po_invoice_counts.columns = ['po', 'total_invoices_per_po']
            df = df.merge(po_invoice_counts, on='po', how='left')
            
            def categorize_transaction(row):
                """
                Categorize transactions based on CFOP and whether Andritz is the emitter
                Includes categories for maintenance, repairs, and returns
                """
                cfop = str(row['cfop'])
                emit_nome = str(row['emitNome']).upper()
                
                # Check if Andritz is the emitter
                is_andritz_emitter = 'ANDRITZ' in emit_nome
                
                # Manutenção, Conserto e Reparo CFOPs
                manutencao_entrada_cfops = ['1915', '2915', '1916', '2916']  # Entrada para reparo
                manutencao_saida_cfops = ['5915', '6915', '5916', '6916']    # Saída para reparo
                
                # Retorno de mercadoria CFOPs
                retorno_entrada_cfops = ['1201', '1202', '1203', '1204', '1410', '1411', '1503', '1504', 
                                        '2201', '2202', '2203', '2204', '2410', '2411', '2503', '2504']
                retorno_saida_cfops = ['5201', '5202', '5210', '5410', '5411', '5412', '5413', '5503', '5504',
                                    '6201', '6202', '6210', '6410', '6411', '6412', '6413', '6503', '6504']
                
                # Remessa CFOPs
                remessa_entrada_cfops = ['1554','1901', '1902', '1903', '1904', '1905', '1906', '1907', '1908', '1909', '1913', '1914', '1921',
                                        '2901', '2902', '2903', '2904', '2905', '2906', '2907', '2908', '2909', '2913', '2914', '2921']
                remessa_saida_cfops = ['5901', '5902', '5903', '5904', '5905', '5906', '5907', '5908', '5909', '5913', '5914', '5921',
                                    '6901', '6902', '6903', '6904', '6905', '6906', '6907', '6908', '6909', '6913', '6914', '6921']
                
                # Devolução CFOPs
                devolucao_entrada_cfops = ['1201', '1202', '1203', '1204', '1209', '1410', '1411', '1503', '1504', '1921',
                                        '2201', '2202', '2203', '2204', '2209', '2410', '2411', '2503', '2504', '2921']
                devolucao_saida_cfops = ['5201', '5202', '5203', '5204', '5209', '5410', '5411', '5412', '5413', '5503', '5504', '5921',
                                        '6201', '6202', '6203', '6204', '6209', '6410', '6411', '6412', '6413', '6503', '6504', '6921']

                # Industrialização CFOPs
                industrializacao_entrada_cfops = ['1124', '1125', '1126', '2124', '2125', '2126']
                industrializacao_saida_cfops = ['5124', '5125', '5126', '6124', '6125', '6126']

                # Categorization logic
                if cfop in manutencao_entrada_cfops or cfop in manutencao_saida_cfops:
                    return "Manutenção/Conserto/Reparo"
                
                elif cfop in retorno_entrada_cfops or cfop in retorno_saida_cfops:
                    return "Retorno de Mercadoria"
                
                elif cfop in remessa_entrada_cfops or cfop in remessa_saida_cfops:
                    return "Remessa"
                
                elif cfop in devolucao_entrada_cfops or cfop in devolucao_saida_cfops:
                    return "Devolução"
                    
                elif cfop in industrializacao_entrada_cfops or cfop in industrializacao_saida_cfops:
                    return "Industrialização"
                
                elif cfop.startswith('3') or cfop.startswith('7'):
                    return "Importação/Exportação"
                    
                elif cfop.startswith('1') or cfop.startswith('2'):  # Entrada
                    if is_andritz_emitter:
                        return "Transferência Entre Filiais"
                    else:
                        return "Compra de Terceiros"
                        
                elif cfop.startswith('5') or cfop.startswith('6'):  # Saída
                    if is_andritz_emitter:
                        return "Transferência Entre Filiais"
                    else:
                        return "Venda para Terceiros"
                        
                return "Outros"  # Default category

            # Add this code after the existing DataFrame transformations but before the final column selection
            df['categoria'] = df.apply(categorize_transaction, axis=1)
            
            def categorize(row):
                """
                Categoriza transações baseando-se no CFOP, emitente e destinatário.
                As categorias incluem manutenção, retorno, devolução, vendas, industrialização,
                transferências detalhadas, com um foco em clareza e compreensão para todos os envolvidos.
                """
                cfop = str(row['cfop'])
                emit_nome = str(row['emitNome']).upper()  # Nome do emitente
                dest_nome = str(row['destNome']).upper()  # Nome do destinatário
                
                # Verifica se Andritz é o emitente ou destinatário
                is_andritz_emitter = 'ANDRITZ' in emit_nome
                is_andritz_dest = 'ANDRITZ' in dest_nome

                # Categorias de CFOPs
                manutencao_cfops = ['1915', '2915', '1916', '2916', '5915', '6915', '5916', '6916']
                retorno_cfops = ['1201', '1202', '1203', '1204', '1410', '1411', '1503', '1504', 
                                '2201', '2202', '2203', '2204', '2410', '2411', '2503', '2504',
                                '5201', '5202', '5210', '5410', '5411', '5412', '5413', '5503', 
                                '5504', '6201', '6202', '6210', '6410', '6411', '6412', '6413', 
                                '6503', '6504']
                
                devolucao_cfops = ['1201', '1202', '1203', '1204', '1209', '1410', '1411', '1503', 
                                '1504', '1921', '2201', '2202', '2203', '2204', '2209', '2410', 
                                '2411', '2503', '2504', '2921', '5201', '5202', '5203', '5204', 
                                '5209', '5410', '5411', '5412', '5413', '5503', '5504', '5921',
                                '6201', '6202', '6203', '6204', '6209', '6410', '6411', '6412', 
                                '6413', '6503', '6504', '6921']
                
                industrializacao_cfops = ['1124', '1125', '1126', '2124', '2125', '2126', 
                                        '5124', '5125', '5126', '6124', '6125', '6126']
                
                aluguel_comodato_cfop = ['5908','5909','6909']
                
                industrializacao = ['5901']
                
                venda_cfops = ['5101', '5102', '5401', '5403', '5405', '5551', '5653', '5656', 
                            '6101', '6102', '6107', '6108', '6401', '6403', '6404', '5923', 
                            '6653', '6923']
                
                # Transferências detalhadas entre filiais
                transferencia_envio_cfops = ['6949', '5554', '6554', '6555', '5949','5551']  # Envio
                transferencia_retorno_cfops = ['1949', '2554', '2908', '2949']  # Retorno
                
                # Lógica de categorização detalhada com mais clareza
                if cfop in manutencao_cfops:
                    if is_andritz_emitter:
                        return "Manutenção/Conserto/Reparo - Envio para Fornecedor"
                    else:
                        return "Retorno de Manutenção/Reparo - Devolução do Fornecedor"
                
                elif cfop in retorno_cfops:
                    return "Retorno de Mercadoria - Devolução de Produto ao Fornecedor"
                
                elif cfop in devolucao_cfops:
                    return "Devolução de Mercadoria - Retorno de Produto ao Cliente"
                
                elif cfop in industrializacao_cfops:
                    return "Industrialização - Processamento de Mercadorias para Produção"
                
                elif cfop in industrializacao:
                    return "Industrialização - Envio materiais nossa propriedade para Industrialização"
                
                elif cfop in venda_cfops:
                    if is_andritz_emitter:
                        return "Venda Própria - Comercialização de Produtos Andritz"
                    else:
                        return "Venda de Terceiros - Compra de Produtos de Fornecedores"
                
                # Identificação das transferências entre filiais
                elif cfop in transferencia_envio_cfops:
                    if is_andritz_emitter:
                        return "Transferência Entre Filiais - Envio AQA para Projeto"
                    else:
                        return "Transferência Entre Filiais - Envio AQA para Projeto"
                                # Identificação das transferências entre filiais

                elif cfop in aluguel_comodato_cfop:
                    if is_andritz_emitter:
                        return "Aluguel ou Comodato - Devolução"
                    else:
                        return "Aluguel ou Comodato - Recebimento"  
                
                elif cfop in transferencia_retorno_cfops:
                    if is_andritz_dest:
                        return "Transferência Entre Filiais - Retorno Projeto para AQA"
                    else:
                        return "Transferência Entre Filiais - Retorno Projeto para AQA"
                
                return "Outros - Categoria Padrão para CFOPs Não Identificados"

            # Aplicar a função de categorização
            df['my_categoria'] = df.apply(categorize, axis=1)
            
            # Exibir apenas as colunas renomeadas
            colunas_renomeadas = ['nNf', 'dtEmi', 'itemNf','nomeMaterial','ncm','qtd','und','vlUnProd','vlTotProd','vlTotalNf','po','dVenc','chNfe',
                                    'emitNome','emitCnpj','emitLogr','emitNr','emitCompl','emitBairro','emitMunic','emitUf','emitCep','emitPais',
                                    'destNome','destCnpj','destLogr','destNr','destCompl','destBairro','destMunic','destUf','destCep','destPais',
                                    'cfop','total_invoices_per_po', 'categoria','my_categoria','tags','unique','codigo_projeto']
            
            df= df[colunas_renomeadas]

            groupby_cols_nf = ['chNfe']
            df['total_itens_nf'] = df.groupby(groupby_cols_nf )['qtd'].transform('sum')
            
            groupby_cols_po = ['po']
            df['total_itens_po'] = df.groupby(groupby_cols_po )['qtd'].transform('sum')
            df['valor_recebido_po'] = df.groupby(groupby_cols_po )['vlTotProd'].transform('sum')
            
            # # Lista de colunas que você quer limpar
            # columns_to_clean = ['po', 'nNf','ncm','qtd','emitCep','emitCnpj','total_invoices_per_po']

            # for col in columns_to_clean:
            #     df[col] = (
            #         df[col]
            #         .astype(str)
            #         .str.replace(r'\D', '', regex=True)  # Remove caracteres não numéricos
            #         .astype(pd.Int64Dtype())             # Converte para Int64
            #     )
            
            # Conversão para Pandas
            if polars_po.height > 0:
                po_polars = polars_po.to_pandas()
                cod_project_polars = polars_cod_project.to_pandas()
                pandas_cat = polars_cat.to_pandas()

                # Mesclagem 1: po_polars com df
                df_merged = pd.merge(
                    df, 
                    po_polars, 
                    left_on="po", 
                    right_on="Purchasing Document", 
                    how="left"
                )
                
                def convert_columns_to_numeric_po(df_merged, columns):
                    """Converte várias colunas para numérico, forçando erros para NaN."""
                    for column in columns:
                        df_merged[column] = pd.to_numeric(df_merged[column], errors='coerce')
                    return df_merged

                # Supondo que você queira converter as colunas 'po' e 'NFe'
                columns_to_convert = ['codigo_projeto_x'] 
                
                # Converter as colunas relevantes para numérico
                df_merged = convert_columns_to_numeric_po(df_merged, columns_to_convert)   

                # #Garantir que ambas as colunas tenham o mesmo tipo e tratar valores ausentes
                # df_merged["codigo_projeto_x"] = df_merged["codigo_projeto_x"].fillna("").astype(str)
                # cod_project_polars["codigo_projeto"] = cod_project_polars["codigo_projeto"].fillna("").astype(str)
                
                def convert_columns_to_numeric_projects(cod_project_polars, columns):
                    """Converte várias colunas para numérico, forçando erros para NaN."""
                    for column in columns:
                        cod_project_polars[column] = pd.to_numeric(cod_project_polars[column], errors='coerce')
                    return cod_project_polars

                # Supondo que você queira converter as colunas 'po' e 'NFe'
                columns_to_convert = ['codigo_projeto'] 
                
                # Converter as colunas relevantes para numérico
                cod_project_polars = convert_columns_to_numeric_projects(cod_project_polars, columns_to_convert)  
                
                # Mesclagem 2: df_merged com cod_project_polars
                df_merged_projects = pd.merge(
                    df_merged, 
                    cod_project_polars, 
                    left_on="codigo_projeto_x", 
                    right_on="codigo_projeto", 
                    how="left"
                )

                                # Mapeamento dos meses em português
                meses_pt = {
                    1: 'janeiro', 2: 'fevereiro', 3: 'março', 4: 'abril', 5: 'maio', 6: 'junho',
                    7: 'julho', 8: 'agosto', 9: 'setembro', 10: 'outubro', 11: 'novembro', 12: 'dezembro'
                }

                # Pré-processar e transformar DataFrame com operações relacionadas a datas
                df_merged_projects['data nf'] = pd.to_datetime(df_merged_projects['dtEmi'], errors='coerce', utc=True)

                # Criar novas colunas com base em dtEmi
                df_merged_projects['mes_ano'] = df_merged_projects['data nf'].dt.strftime('%Y-%m')
                df_merged_projects['ano'] = df_merged_projects['data nf'].dt.strftime('%Y')
                df_merged_projects['mes'] = df_merged_projects['data nf'].dt.month.map(meses_pt)

                # Criar a coluna 'data' com o formato desejado
                df_merged_projects['data nf'] = df_merged_projects['data nf'].dt.strftime('%d/%m/%Y')
                
                # Exibir apenas as colunas renomeadas
                colunas_visiveis = ['nNf','itemNf','nomeMaterial','ncm','qtd','und','vlUnProd','vlTotProd','vlTotalNf','total_itens_nf','data nf','dVenc','chNfe',
                                    'emitNome','emitCnpj','emitLogr','emitNr','emitCompl','emitBairro','emitMunic','emitUf','emitCep','emitPais',
                                    'destNome','destCnpj','destLogr','destNr','destCompl','destBairro','destMunic','destUf','destCep','destPais',
                                    'cfop','my_categoria',
                                    'po','codigo_projeto_y','Project Code_x','Andritz WBS Element','Cost Center','total_invoices_per_po','total_itens_po','valor_recebido_po',
                                    'codigo_projeto','Project Code_y',
                                    'mes_ano','mes','ano','dtEmi','tags','unique']
                
                df_merged_projects= df_merged_projects[colunas_visiveis]
                
                            # Renomear as colunas
                renomear_colunas = {
                    'nNf': 'Nota Fiscal',  
                    'itemNf': 'Item Nf',
                    'nomeMaterial': 'Nome Material',
                    'ncm': 'Codigo NCM',
                    'qtd': 'Quantidade',
                    'und': 'Unidade',
                    'vlUnProd': 'Valor Unitario Produto',
                    'vlTotProd': 'Valor Total Produto',
                    'vlTotalNf': 'Valor Total Nota Fiscal',
                    'total_itens_nf':'Total itens Nf',
                    'data nf':'data nf',
                    'dVenc': 'Data Vencimento',
                    'chNfe': 'Chave NF-e',
                    'emitNome': 'Nome Emitente',
                    'emitCnpj': 'CNPJ Emitente',
                    'emitLogr': 'Logradouro Emitente',
                    'emitNr': 'Numero Emitente',
                    'emitCompl': 'Complemento Emitente',
                    'emitBairro': 'Bairro Emitente',
                    'emitMunic': 'Municipio Emitente',
                    'emitUf': 'UF Emitente',
                    'emitCep': 'CEP Emitente',
                    'emitPais': 'Pais Emitente',
                    'destNome': 'Nome Destinatario',
                    'destCnpj': 'CNPJ Destinatario',
                    'destLogr': 'Logradouro Destinatario',
                    'destNr': 'Numero Destinatario',
                    'destCompl': 'Complemento Destinatario',
                    'destBairro': 'Bairro Destinatario',
                    'destMunic': 'Municipio Destinatario',
                    'destUf': 'UF Destinatario',
                    'destCep': 'CEP Destinatario',
                    'destPais': 'Pais Destinatario',
                    'cfop': 'CFOP',
                    'my_categoria': 'CFOP Categoria',
                    'po': 'PO',
                    'codigo_projeto_y': 'Codigo Projeto',
                    'Project Code_x': 'Projeto',
                    'Andritz WBS Element': 'WBS Andritz',
                    'Cost Center': 'Centro de Custo',
                    'total_invoices_per_po': 'NF recebidas PO',
                    'total_itens_po': 'Itens recebidos PO',
                    'valor_recebido_po': 'Valor Recebido PO',
                    'codigo_projeto': 'Codigo Projeto Envio',
                    'Project Code_y': 'Projeto Envio',
                    'dtEmi': 'Data Emissao',
                    'mes_ano':'mes_ano','ano':'ano','mes':'mes',
                    'tags':'tags',
                    'unique': 'unique'

                }

                # Aplicar a renomeação
                df_merged_projects = df_merged_projects.rename(columns=renomear_colunas)
                
                df_merged_projects = df_merged_projects.sort_values(by=['Data Emissao','Nota Fiscal','Item Nf'], ascending=[False,True,True])

                # Mesclar DataFrames
                df_mesclado = pd.merge(
                    df_merged_projects, 
                    pandas_cat, 
                    on="tags", 
                    how="left"
                )

                # Remove duplicates based on the slugified unique column
                df_mesclado.drop_duplicates(subset='unique', inplace=True)

                df=df_mesclado
                
                
                            
                #df = df.sort_values(by=['dtEmi','nNf','itemNf'], ascending=[False,True,True])

                # Download buttons
                def convert_df_to_excel(df):
                    output = io.BytesIO()
                    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                        df.to_excel(writer, index=False, sheet_name='Invoices')
                    return output.getvalue()
                        
                randon = datetime.now().strftime("%d%m%Y%H%M%S") + str(datetime.now().microsecond)[:3]


                excel_file = convert_df_to_excel(df)
                st.download_button(
                    label="Download Excel",
                    data=excel_file,
                    file_name=f"NFSXML_{randon}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    type='primary'
                )
                
                st.success(f"Processed {len(uploaded_files)} XML files")
            else:
                st.warning("Primeiro processe alguns arquivos de PO")

    with tab2:
        st.header("Visualização de Dados")
        if 'df' in locals():
            # st.text(len(df_merged))
            # st.text(len(df_merged_projects))
            # st.dataframe(df_merged)
            # st.dataframe(df_merged_projects)
            # # Key Metrics
            # col1, col2, col3 = st.columns(3)
            
            # with col1:
            #     total_invoices = len(df)
            #     st.metric(label="Total de Linhas", value=total_invoices)
            
            # with col2:
            #     unique_issuers = df['emitNome'].nunique()
            #     st.metric(label="Número de Fornecedores", value=unique_issuers)
            
            # with col3:
            #     unique_issuers = df['nNf'].nunique()
            #     st.metric(label="Número de Notas Fiscais", value=unique_issuers)
            
            # Global Search Filter
            st.subheader("Filtrar Dados")
            search_term = st.text_input("Busca Global (filtra em todas as colunas)")
            
            if search_term:
                # Create a boolean mask that checks if the search term is in any column
                mask = df.apply(lambda row: row.astype(str).str.contains(search_term, case=False).any(), axis=1)
                filtered_df = df[mask]
            else:
                filtered_df = df
            
            # Display filtered DataFrame without index
            st.dataframe(filtered_df, hide_index=True)
            
        else:
            st.warning("Primeiro processe alguns arquivos XML na aba de Processamento")

    with tab3:
        st.header("Como Utilizar o Aplicativo")
        st.markdown("""
        ### Processamento de Arquivos XML 🔬

        1. **Carregue seus arquivos XML**
        - Clique em "Upload XML Files"
        - Selecione um ou mais arquivos XML de notas fiscais

        2. **Processamento Automático**
        - O aplicativo processará automaticamente os arquivos
        - Uma barra de progresso será exibida durante o processamento

        3. **Visualização dos Dados**
        - Os dados processados serão exibidos em uma tabela
        - Você pode fazer download em formato Excel ou Pickle

        ### Recursos Principais 📊

        - Extração de informações de notas fiscais
        - Limpeza e formatação dos dados
        - Suporte para múltiplos arquivos XML
        - Visualização de dados processados
        - Download em diferentes formatos

        ### Dicas 💡

        - Certifique-se de que os arquivos XML são de notas fiscais brasileiras (NF-e)
        - O processamento pode levar alguns segundos dependendo do número de arquivos
        - Verifique sempre os dados processados antes do download

        ### Formatos Suportados
        - Arquivos XML com estrutura de Nota Fiscal Eletrônica (NF-e)
        """)

if __name__ == "__main__":
    main()
    
# Footer
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center'>
        <p>Desenvolvido com ❤️ | XML Processor Pro v1.0</p>
    </div>
    """,
    unsafe_allow_html=True
)      