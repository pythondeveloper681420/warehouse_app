import streamlit as st
import os
import pandas as pd
from pathlib import Path
import tempfile
import unicodedata
import re
from datetime import datetime
import plotly.express as px

# Mantendo as funções auxiliares existentes
def letter_to_number_str(text):
    def convert_char(c):
        if c.isalpha():
            return str(ord(c.upper()) - ord('A') + 1)
        return c
    return ''.join(convert_char(c) for c in text)

def letter_to_number(text):
    def convert_char(c):
        if c.isalpha():
            return ord(c.upper()) - ord('A') + 1
        elif c.isdigit():
            return int(c)
        return 0
    return sum(convert_char(c) for c in text)

def get_portuguese_month(month_number):
    try:
        month_idx = int(float(month_number)) - 1
        meses_portugues = [
            "janeiro", "fevereiro", "março", "abril", "maio", "junho",
            "julho", "agosto", "setembro", "outubro", "novembro", "dezembro"
        ]
        return meses_portugues[month_idx] if 0 <= month_idx < 12 else ""
    except (ValueError, TypeError):
        return ""

def slugify(text):
    if not isinstance(text, str):
        text = str(text)
    text = text.lower()
    text = unicodedata.normalize('NFKD', text)
    text = text.encode('ascii', 'ignore').decode('utf-8')
    text = re.sub(r'[^a-z0-9]+', '-', text)
    text = text.strip('-')
    text = re.sub(r'-+', '-', text)
    return text

def get_downloads_folder():
    return str(Path.home() / "Downloads")

def process_files(uploaded_files):
    """Processa os arquivos carregados e retorna o DataFrame combinado"""
    dfs = []
    
    for uploaded_file in uploaded_files:
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_file:
            tmp_file.write(uploaded_file.getvalue())
            tmp_path = tmp_file.name
        
        df = pd.read_excel(tmp_path)
        df['Nome do Arquivo'] = uploaded_file.name
        
        if 'Requisição' in df.columns:
            df['Requisição'] = df['Requisição'].astype(str).str.replace(',', '', regex=False)
        
        if 'Data' in df.columns:
            df['Data'] = pd.to_datetime(df['Data'], errors='coerce')
            mask = df['Data'].notna()
            if mask.any():
                df.loc[mask, 'Mes_Numero'] = df.loc[mask, 'Data'].dt.month.astype(int)
                df.loc[mask, 'Mes'] = df.loc[mask, 'Mes_Numero'].apply(get_portuguese_month)
                df.loc[mask, 'Ano'] = df.loc[mask, 'Data'].dt.year
                df.loc[mask, 'Dia'] = df.loc[mask, 'Data'].dt.day
                
                valid_mes = df['Mes'].notna() & (df['Mes'] != '')
                df.loc[valid_mes, 'Mes/Ano'] = (
                    df.loc[valid_mes, 'Mes'].astype(str) + '-' +
                    df.loc[valid_mes, 'Ano'].astype(str).str.replace('.0', '')
                )
        
        if all(col in df.columns for col in ['Requisição', 'CC - WBS', 'Data', 'Solicitante']):
            df['req_cod'] = (
                df['Requisição'].astype(str) + '_' +
                df['CC - WBS'].astype(str) + '_' +
                df['Data'].astype(str).str.replace('/', '') + '_' +
                df['Nome do Arquivo'].astype(str).str[:8] + '_' +
                df['Solicitante'].astype(str)
            )
            df['req_cod'] = df['req_cod'].apply(slugify)
            df['req_cod'] = df['req_cod'].str.replace('-', '').str.upper()
            df['req_cod'] = 'req'+df['req_cod'].apply(letter_to_number_str)
        
        if all(col in df.columns for col in ['Item', 'Cod. Material', 'Descrição', 'Qtd. Solicitada', 'req_cod']):
            df['unique'] = (
                df['Item'].astype(str) + '' +
                df['Cod. Material'].astype(str) + '' +
                df['Descrição'].astype(str) + '' +
                df['Qtd. Solicitada'].astype(str) + '' +
                df['req_cod'].astype(str)
            )
            df['unique'] = df['unique'].apply(slugify).str.rstrip()
        
        dfs.append(df)
        os.unlink(tmp_path)
    
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    return None

def main():
    # Configuração da página
    st.set_page_config(
        page_title="Processador de Requisições EPI",
        page_icon="📊",
        layout="wide"
    )

    # CSS personalizado
    st.markdown("""
        <style>
        .stTabs [data-baseweb="tab-list"] button [data-testid="stMarkdownContainer"] p {
            font-size: 1.2rem;
            padding: 0.5rem 1rem;
        }
        .stButton > button {
            width: 100%;
            border-radius: 0.5rem;
            height: 3rem;
            background-color: #0075be;
            color: white;
        }
        .stButton > button:hover {
            background-color: #0075be;
        }
        </style>
    """, unsafe_allow_html=True)

    # Título principal
    st.title("📊 Processador de Requisições EPI")

    # Criação das abas
    tab1, tab2, tab3 = st.tabs([
        "💼 Processamento",
        "📈 Visualização",
        "❓ Como Usar"
    ])

    # Variáveis de estado para compartilhar dados entre as abas
    if 'processed_df' not in st.session_state:
        st.session_state.processed_df = None

    # Aba de Processamento
    with tab1:
        st.header("Processamento de Arquivos")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            uploaded_files = st.file_uploader(
                "Selecione os arquivos Excel para processar",
                type=['xlsx'],
                accept_multiple_files=True
            )

        with col2:
            if uploaded_files:
                st.info(f"📁 {len(uploaded_files)} arquivo(s) selecionado(s)")
                
                if st.button("Processar Arquivos", key="process_button",type='primary',use_container_width=True):
                    with st.spinner("Processando arquivos..."):
                        df_principal = process_files(uploaded_files)
                        
                        if df_principal is not None:
                            df_principal = df_principal.sort_values(
                                by=['Ano','Mes_Numero','Dia','req_cod','Item'],
                                ascending=[False,False,False,False,True]
                            )
                            df_principal = df_principal.drop(columns=['Dia'])

                            if 'Qtd. Solicitada' in df_principal.columns:
                                df_principal['Qtd. Solicitada'] = pd.to_numeric(
                                    df_principal['Qtd. Solicitada'],
                                    errors='coerce'
                                )
                                df_principal = df_principal[
                                    df_principal['Qtd. Solicitada'].notna() &
                                    (df_principal['Qtd. Solicitada'] != 0)
                                ]

                            df_principal['Data'] = df_principal['Data'].dt.strftime('%d/%m/%Y')
                            
                            randon = datetime.now().strftime("%d%m%Y%H%M%S") + str(datetime.now().microsecond)[:3]
                            output_path = os.path.join(get_downloads_folder(), f'REQS_EPI_{randon}.xlsx')
                            df_principal.to_excel(output_path, index=False)
                            
                            st.session_state.processed_df = df_principal
                            st.success(f"✅ Arquivo salvo com sucesso em: {output_path}")

    # Aba de Visualização
    with tab2:
        if st.session_state.processed_df is not None:
            st.header("Visualização dos Dados")
            
            col1, col2 = st.columns([1, 2])
            
            with col1:
                st.subheader("Filtros")
                if 'Mes/Ano' in st.session_state.processed_df.columns:
                    selected_month = st.selectbox(
                        "Selecione o Mês/Ano",
                        options=['Todos'] + list(st.session_state.processed_df['Mes/Ano'].unique())
                    )
            
            with col2:
                st.subheader("Dados Processados")
                filtered_df = st.session_state.processed_df
                if selected_month != 'Todos':
                    filtered_df = filtered_df[filtered_df['Mes/Ano'] == selected_month]
                
                st.dataframe(
                    filtered_df,
                    use_container_width=True,
                    height=400
                )
            
            # # Análises e gráficos
            # st.subheader("Análises")
            # col3, col4 = st.columns(2)
            
            # with col3:
            #     if 'Qtd. Solicitada' in filtered_df.columns:
            #         fig = px.bar(
            #             filtered_df.groupby('Mes/Ano')['Qtd. Solicitada'].sum().reset_index(),
            #             x='Mes/Ano',
            #             y='Qtd. Solicitada',
            #             title='Quantidade Solicitada por Mês'
            #         )
            #         st.plotly_chart(fig, use_container_width=True)
            
            # with col4:
            #     if 'Solicitante' in filtered_df.columns:
            #         fig = px.pie(
            #             filtered_df['req_cod'].value_counts().reset_index(),
            #             values='count',
            #             names='req_cod',
            #             title='Distribuição por Requisição'
            #         )
            #         st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("⚠️ Processe alguns arquivos primeiro para visualizar os dados")

    # Aba Como Usar
    with tab3:
        st.header("Como Usar o Processador de Requisições")
        
        st.markdown("""
        ### 📌 Passo a Passo
        
        1. **Preparação dos Arquivos**
           - Certifique-se de que seus arquivos estão no formato Excel (.xlsx)
           - Verifique se as colunas necessárias estão presentes nos arquivos
        
        2. **Upload dos Arquivos**
           - Vá para a aba "Processamento"
           - Arraste seus arquivos ou clique para selecionar
           - Você pode selecionar múltiplos arquivos de uma vez
        
        3. **Processamento**
           - Clique no botão "Processar Arquivos"
           - Aguarde o processamento ser concluído
           - O arquivo combinado será salvo automaticamente na pasta Downloads
        
        4. **Visualização**
           - Após o processamento, vá para a aba "Visualização"
           - Use os filtros para analisar os dados
           - Explore os gráficos e análises disponíveis
        
        ### 📊 Colunas Necessárias
        - Requisição
        - CC - WBS
        - Data
        - Solicitante
        - Item
        - Cod. Material
        - Descrição
        - Qtd. Solicitada
        
        ### ⚠️ Observações Importantes
        - Os arquivos são processados mantendo a integridade dos dados
        - As datas são padronizadas no formato dd/mm/aaaa
        - Quantidades solicitadas igual a zero são removidas
        - O código de requisição é gerado automaticamente
        """)

if __name__ == "__main__":
    main()