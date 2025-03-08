import streamlit as st
import pandas as pd
import pdfplumber
import re
import os
from datetime import datetime
import base64
from io import BytesIO
import tempfile
import unicodedata

# Field mapping dictionary to handle different variations
FIELD_MAPPINGS = {
    "numero_nf": [
        r"NFS-e\s*:?\s*([\d]+)",
        r"Número da\s*NFS-e\s*:?\s*([\d]+)",
        r"Nº da Nota\s*:?\s*([\d]+)",
        r"Número\s*:?\s*([\d]+)"
    ],
    "data_emissao": [
        r"Data e Hora da Emissão\s*:?\s*([\d]{1,2}/[\d]{1,2}/[\d]{4}\s+\d{1,2}:\d{2})",
        r"Emissão da NFS-e\s*:?\s*([\d]{1,2}/[\d]{1,2}/[\d]{4}\s+\d{1,2}:\d{2})",
        r"Data de Emissão\s*:?\s*([\d]{1,2}/[\d]{1,2}/[\d]{4}\s+\d{1,2}:\d{2})"
    ],
    "competencia": [
        r"Competência\s*:?\s*([^\n]+)",
        r"Mês de Competência\s*:?\s*([^\n]+)",
        r"Período de Competência\s*:?\s*([^\n]+)"
    ],
    "codigo_verificacao": [
        r"Código de Verificação\s*:?\s*([^\n]+)",
        r"Código Verificador\s*:?\s*([^\n]+)",
        r"Código de Autenticidade\s*:?\s*([^\n]+)"
    ],
    "numero_rps": [
        r"Número do RPS\s*:?\s*([\d]+)",
        r"RPS Nº\s*:?\s*([\d]+)"
    ],
    "nf_substituida": [
        r"No\. da NFS-e substituída\s*:?\s*([\d]+)",
        r"NFS-e substituída\s*:?\s*([\d]+)"
    ],
    "prestador_nome": [
        r"Razão Social/Nome\s*:?\s*([^\n]+)",
        r"Nome/Razão Social\s*:?\s*([^\n]+)",
        r"Prestador de Serviço\s*:?\s*([^\n]+)"
    ],
    "prestador_cnpj": [
        r"CNPJ/CPF\s*:?\s*([\d\.\-/]+)",
        r"CPF/CNPJ\s*:?\s*([\d\.\-/]+)",
        r"CNPJ\s*:?\s*([\d\.\-/]+)"
    ],
    "prestador_telefone": [
        r"Telefone\s*:?\s*([\d\(\)\s\-]+)",
        r"Fone\s*:?\s*([\d\(\)\s\-]+)",
        r"Tel\s*:?\s*([\d\(\)\s\-]+)"
    ],
    "prestador_email": [
        r"e-mail\s*:?\s*([\w\.\-]+@[\w\.\-]+)",
        r"E-mail\s*:?\s*([\w\.\-]+@[\w\.\-]+)",
        r"Email\s*:?\s*([\w\.\-]+@[\w\.\-]+)"
    ],
    "tomador_nome": [
        r"Tomador de Serviço\s*Razão Social/Nome\s*:?\s*([^\n]+)",
        r"Nome/Razão Social do Tomador\s*:?\s*([^\n]+)",
        r"Tomador\s*:?\s*([^\n]+)"
    ],
    "tomador_cnpj": [
        r"CNPJ/CPF do Tomador\s*:?\s*([\d\.\-/]+)",
        r"CPF/CNPJ do Tomador\s*:?\s*([\d\.\-/]+)",
        r"CNPJ Tomador\s*:?\s*([\d\.\-/]+)"
    ],
    "tomador_endereco": [
        r"Endereço e CEP\s*:?\s*([^\n]+)",
        r"Endereço Tomador\s*:?\s*([^\n]+)"
    ],
    "tomador_telefone": [
        r"Telefone Tomador\s*:?\s*([\d\(\)\s\-]+)",
        r"Fone Tomador\s*:?\s*([\d\(\)\s\-]+)"
    ],
    "tomador_email": [
        r"e-mail Tomador\s*:?\s*([\w\.\-]+@[\w\.\-]+)",
        r"Email Tomador\s*:?\s*([\w\.\-]+@[\w\.\-]+)"
    ],
    "discriminacao_servico": [
        r"Discriminação (do|dos) Serviço(s)?\s*(.+?)(?=Código do Serviço|Detalhamento Específico|Tributos Federais|Valor do Serviço)",
        r"Descrição dos Serviços\s*(.+?)(?=Código|Valor|Tributos)",
        r"Descrição\s*(.+?)(?=Código|Valor|Tributos)"
    ],
    "codigo_servico": [
        r"Código do Serviço\s*/\s*Atividade\s*([^\n]+)",
        r"Código Serviço\s*:?\s*([^\n]+)"
    ],
    "detalhamento_especifico": [
        r"Detalhamento Específico da Construção Civil\s*([^\n]+)",
        r"Detalhamento Específico\s*:?\s*([^\n]+)"
    ],
    "codigo_obra": [
        r"Código da Obra\s*([^\n]+)",
        r"Código Obra\s*:?\s*([^\n]+)"
    ],
    "codigo_art": [
        r"Código ART\s*([^\n]+)",
        r"ART\s*:?\s*([^\n]+)"
    ],
    "tributos_federais": [
        r"Tributos Federais\s*([^\n]+)",
        r"Tributos Fed\.\s*:?\s*([^\n]+)"
    ],
    "valor_servico": [
        r"Valor (do|dos) Serviço(s)?\s*R\$\s*([\d\.,]+)",
        r"Valor Total\s*R\$\s*([\d\.,]+)",
        r"Total da Nota\s*R\$\s*([\d\.,]+)",
        r"Valor do Serviço\s*[\r\n]+\s*([\d\.,]+)",  # New pattern for your PDF format
        r"Valor do Serviço\s*([\d\.,]+)"  # Alternative pattern without newline
    ],
    "desconto_incondicionado": [
        r"Desconto Incondicionado\s*R\$\s*([\d\.,]+)",
        r"Desc\. Incond\.\s*R\$\s*([\d\.,]+)"
    ],
    "desconto_condicionado": [
        r"Desconto Condicionado\s*R\$\s*([\d\.,]+)",
        r"Desc\. Cond\.\s*R\$\s*([\d\.,]+)"
    ],
    "retencao_federal": [
        r"Retenções Federais\s*R\$\s*([\d\.,]+)",
        r"Ret\. Federais\s*R\$\s*([\d\.,]+)"
    ],
    "issqn_retido": [
        r"ISSQN Retido\s*R\$\s*([\d\.,]+)",
        r"ISS Retido\s*R\$\s*([\d\.,]+)"
    ],
    "valor_liquido": [
        r"Valor Líquido\s*R\$\s*([\d\.,]+)",
        r"Líquido\s*R\$\s*([\d\.,]+)"
    ],
    "regime_tributacao": [
        r"Regime Especial Tributação\s*([^\n]+)",
        r"Regime Tributário\s*:?\s*([^\n]+)"
    ],
    "simples_nacional": [
        r"Opção Simples Nacional\s*([^\n]+)",
        r"Simples Nacional\s*:?\s*([^\n]+)"
    ],
    "incentivador_cultural": [
        r"Incentivador Cultural\s*([^\n]+)",
        r"Inc\. Cultural\s*:?\s*([^\n]+)"
    ],
    "avisos": [
        r"Avisos\s*([^\n]+)",
        r"Observações\s*:?\s*([^\n]+)"
    ]
}

def extract_field(text, field_key):
    """Extract field value using multiple possible patterns"""
    if not text:
        return None
    patterns = FIELD_MAPPINGS.get(field_key, [])
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
        if match:
            return match.group(match.lastindex).strip()
    return None

def extract_numbers(text):
    """Extract numbers starting with 4501-4506"""
    if not text or not isinstance(text, str):
        return ""
    pattern = r'(450[1-6]\d{6,})'
    matches = re.findall(pattern, text)
    processed_numbers = [number[:10] for number in matches]
    unique_numbers = list(dict.fromkeys(processed_numbers))
    return ' '.join(unique_numbers[:10]) if unique_numbers else ""

def extract_code(text):
    """Extract 6 digits from X-XX-XXXXXX-XXX-XXXX-XXX pattern"""
    if not text or not isinstance(text, str):
        return ""
    pattern = r'[A-Z0-9]-[A-Z0-9]{2}-(\d{6})-\d{3}-\d{4}-\d{3}'
    match = re.search(pattern, text)
    return match.group(1) if match else ""

def slugify(text):
    """Convert text to slug format"""
    if not isinstance(text, str):
        text = str(text)
    text = text.lower()
    text = unicodedata.normalize('NFKD', text)
    text = text.encode('ascii', 'ignore').decode('utf-8')
    text = re.sub(r'[^a-z0-9]+', '-', text)
    text = text.strip('-')
    text = re.sub(r'-+', '-', text)
    return text

def convert_brazilian_number(value):
    """Convert Brazilian number format to float"""
    if pd.isna(value) or value is None:
        return 0.0
    try:
        clean_value = str(value).replace('.', '').replace(',', '.')
        return float(clean_value)
    except (ValueError, AttributeError):
        return 0.0

def to_excel(df):
    """Convert dataframe to excel file and encode it for download"""
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    excel_data = output.getvalue()
    return base64.b64encode(excel_data).decode('utf-8')

def extrair_dados_nf(pdf_file):
    """Extract data from NF PDF"""
    dados_nf = {
        "Numero NFS-e": None,
        "Data Emissão": None,
        "Competencia": None,
        "Codigo de Verificacao": None,
        "Numero RPS": None,
        "NF-e Substituida": None,
        "Razao Social Prestador": None,
        "CNPJ Prestador": None,
        "Telefone Prestador": None,
        "Email Prestador": None,
        "Razao Social Tomador": None,
        "CNPJ Tomador": None,
        "Endereco Tomador": None,
        "Telefone Tomador": None,
        "Email Tomador": None,
        "Discriminacao do Servico": None,
        "Codigo Servico": None,
        "Detalhamento Especifico": None,
        "Codigo da Obra": None,
        "Codigo ART": None,
        "Tributos Federais": None,
        "Valor do Servico": None,
        "Desconto Incondicionado": None,
        "Desconto Condicionado": None,
        "Retencao Federal": None,
        "ISSQN Retido": None,
        "Valor Liquido": None,
        "Regime Especial Tributacao": None,
        "Simples Nacional": None,
        "Incentivador Cultural": None,
        "Avisos": None,
        "Nome do Arquivo": pdf_file.name,
    }

    with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as tmp_file:
        tmp_file.write(pdf_file.getvalue())
        tmp_file_path = tmp_file.name

    try:
        with pdfplumber.open(tmp_file_path) as pdf:
            text = ""
            for page in pdf.pages:
                text += page.extract_text() or ""
            
            if not text:
                st.warning(f"Falha ao extrair texto do PDF: {pdf_file.name}")
                return dados_nf

            # Extract all fields using mapping
            dados_nf["Numero NFS-e"] = extract_field(text, "numero_nf")
            dados_nf["Data Emissão"] = extract_field(text, "data_emissao")
            dados_nf["Competencia"] = extract_field(text, "competencia")
            dados_nf["Codigo de Verificacao"] = extract_field(text, "codigo_verificacao")
            dados_nf["Numero RPS"] = extract_field(text, "numero_rps")
            dados_nf["NF-e Substituida"] = extract_field(text, "nf_substituida")
            dados_nf["Razao Social Prestador"] = extract_field(text, "prestador_nome")
            dados_nf["CNPJ Prestador"] = extract_field(text, "prestador_cnpj")
            dados_nf["Telefone Prestador"] = extract_field(text, "prestador_telefone")
            dados_nf["Email Prestador"] = extract_field(text, "prestador_email")
            dados_nf["Razao Social Tomador"] = extract_field(text, "tomador_nome")
            dados_nf["CNPJ Tomador"] = extract_field(text, "tomador_cnpj")
            dados_nf["Endereco Tomador"] = extract_field(text, "tomador_endereco")
            dados_nf["Telefone Tomador"] = extract_field(text, "tomador_telefone")
            dados_nf["Email Tomador"] = extract_field(text, "tomador_email")
            dados_nf["Discriminacao do Servico"] = extract_field(text, "discriminacao_servico")
            dados_nf["Codigo Servico"] = extract_field(text, "codigo_servico")
            dados_nf["Detalhamento Especifico"] = extract_field(text, "detalhamento_especifico")
            dados_nf["Codigo da Obra"] = extract_field(text, "codigo_obra")
            dados_nf["Codigo ART"] = extract_field(text, "codigo_art")
            dados_nf["Tributos Federais"] = extract_field(text, "tributos_federais")
            dados_nf["Valor do Servico"] = extract_field(text, "valor_servico")
            dados_nf["Desconto Incondicionado"] = extract_field(text, "desconto_incondicionado")
            dados_nf["Desconto Condicionado"] = extract_field(text, "desconto_condicionado")
            dados_nf["Retencao Federal"] = extract_field(text, "retencao_federal")
            dados_nf["ISSQN Retido"] = extract_field(text, "issqn_retido")
            dados_nf["Valor Liquido"] = extract_field(text, "valor_liquido")
            dados_nf["Regime Especial Tributacao"] = extract_field(text, "regime_tributacao")
            dados_nf["Simples Nacional"] = extract_field(text, "simples_nacional")
            dados_nf["Incentivador Cultural"] = extract_field(text, "incentivador_cultural")
            dados_nf["Avisos"] = extract_field(text, "avisos")

    finally:
        os.unlink(tmp_file_path)

    return dados_nf

def main():
    st.set_page_config(
        page_title="NF-e Extractor",
        page_icon="📄",
        layout="wide",
        initial_sidebar_state="collapsed"
    )

    # Custom CSS
    st.markdown("""
        <style>
        .main {
            padding: 1rem;
        }
        .stButton>button {
            width: 100%;
            border-radius: 5px;
            height: 3rem;
            font-weight: bold;
        }
        .uploadedFile {
            border: 1px solid #e6e6e6;
            border-radius: 5px;
            padding: 1rem;
            margin: 1rem 0;
        }
        .success-message {
            padding: 1rem;
            background-color: #d4edda;
            color: #155724;
            border-radius: 5px;
            margin: 1rem 0;
        }
        .title-container {
            text-align: center;
            padding: 2rem 0;
            background-color: #f8f9fa;
            border-radius: 10px;
            margin-bottom: 2rem;
        }
        </style>
    """, unsafe_allow_html=True)

    st.header("📝 Extrator de Notas Fiscais de Serviço")
    
    tabs = st.tabs(["📤 Upload e Extração", "📊 Visualização dos Dados", "❓Como Utilizar"])
    
    with tabs[0]:
        col1, col2 = st.columns([2, 1])
        
        with col1:
            uploaded_files = st.file_uploader(
                "Arraste ou selecione os arquivos PDF das Notas Fiscais",
                type="pdf",
                accept_multiple_files=True
            )

        if uploaded_files:
            with st.spinner('Processando os arquivos...'):
                dados_extraidos = []
                progress_bar = st.progress(0)
                
                for i, pdf_file in enumerate(uploaded_files):
                    dados_nf = extrair_dados_nf(pdf_file)
                    dados_extraidos.append(dados_nf)
                    progress_bar.progress((i + 1) / len(uploaded_files))
                
                df_nf = pd.DataFrame(dados_extraidos)
                
                # Create unique identifier and remove duplicates
                df_nf['unique'] = df_nf['Numero NFS-e'].astype(str) + '-' + df_nf['CNPJ Prestador'].astype(str)
                df_nf['unique'] = df_nf['unique'].apply(slugify)
                df_nf.drop_duplicates(subset='unique', inplace=True)
                
                # Extract PO numbers and project codes
                df_nf['po'] = df_nf['Discriminacao do Servico'].fillna('').apply(extract_numbers)
                df_nf['codigo_projeto'] = df_nf['Discriminacao do Servico'].apply(extract_code)
                
                # Process data
                df_nf['Data Emissão'] = pd.to_datetime(df_nf['Data Emissão'], format='%d/%m/%Y %H:%M')
                df_nf = df_nf[df_nf['Numero NFS-e'].notna() & (df_nf['Numero NFS-e'] != '')]
                df_nf = df_nf.sort_values(by='Data Emissão', ascending=False)

                # Display summary
                with col2:
                    st.markdown("### Resumo da Extração")
                    col2_1, col2_2 = st.columns(2)
                    with col2_1:
                        st.metric("Total de Arquivos", len(uploaded_files))
                    with col2_2:
                        st.metric("NFs Processadas", len(df_nf))
                    
                    if not df_nf.empty:
                        st.metric("Período", 
                                f"{df_nf['Data Emissão'].min().strftime('%d/%m/%Y')} - "
                                f"{df_nf['Data Emissão'].max().strftime('%d/%m/%Y')}")

                st.session_state['df_nf'] = df_nf
                
                # Generate unique filename with timestamp
                randon = datetime.now().strftime("%d%m%Y%H%M%S") + str(datetime.now().microsecond)[:3]
                excel_file = to_excel(df_nf)
                st.download_button(
                    label="📥 Baixar Excel",
                    data=base64.b64decode(excel_file),
                    file_name=f'nfspdf_{randon}.xlsx',
                    mime="application/vnd.ms-excel"
                )

    with tabs[1]:
        if 'df_nf' in st.session_state:
            df_nf = st.session_state['df_nf']
            
            # Filters
            col1, col2, col3 = st.columns(3)
            with col1:
                if not df_nf.empty and 'Razao Social Prestador' in df_nf.columns:
                    prestador_filter = st.multiselect(
                        '🧑‍🔧 Filtrar por Prestador',
                        options=sorted(df_nf['Razao Social Prestador'].unique())
                    )
            
            with col2:
                if not df_nf.empty:
                    date_range = st.date_input(
                        '📅 Filtrar por Período',
                        value=(df_nf['Data Emissão'].min().date(), 
                              df_nf['Data Emissão'].max().date())
                    )
            
            # Apply filters
            df_filtered = df_nf.copy()
            if prestador_filter:
                df_filtered = df_filtered[df_filtered['Razao Social Prestador'].isin(prestador_filter)]
            if len(date_range) == 2:
                df_filtered = df_filtered[
                    (df_filtered['Data Emissão'].dt.date >= date_range[0]) &
                    (df_filtered['Data Emissão'].dt.date <= date_range[1])
                ]
            
            # Show summary metrics
            if not df_filtered.empty:
                st.markdown("### Métricas")
                met_col1, met_col2, met_col3 = st.columns(3)
                with met_col1:
                    st.metric("Total de NFs", len(df_filtered))
                with met_col2:
                    if 'Valor do Servico' in df_filtered.columns:
                        total_valor = df_filtered['Valor do Servico'].apply(convert_brazilian_number).sum()
                        st.metric("Valor Total", f"R$ {total_valor:,.2f}")
                with met_col3:
                    if 'Valor Liquido' in df_filtered.columns:
                        total_liquido = df_filtered['Valor Liquido'].apply(convert_brazilian_number).sum()
                        st.metric("Valor Líquido Total", f"R$ {total_liquido:,.2f}")
            
            # Display filtered data
            st.markdown("### Dados Detalhados")
            st.dataframe(
                df_filtered,
                use_container_width=True,
                height=400
            )
        else:
            st.info("Faça o upload dos arquivos na aba 'Upload e Extração' para visualizar os dados.")

    with tabs[2]:
        st.markdown("""
        ## Como Usar o Extrator de Notas Fiscais

        ### 1. Upload de Arquivos
        #### Preparação
        - Certifique-se de que seus arquivos estão em formato PDF
        - Verifique se os PDFs são legíveis e não estão protegidos por senha
        - Organize seus arquivos em uma pasta de fácil acesso

        #### Processo de Upload
        1. Acesse a aba "Upload e Extração"
        2. Arraste os arquivos para a área de upload ou clique para selecionar
        3. Aguarde o processamento dos arquivos
        4. Após o processamento, você verá um resumo da extração
        5. Baixe os dados em Excel usando o botão "Baixar Excel"

        ### 2. Visualização e Análise
        #### Filtros Disponíveis
        - **Prestador**: Selecione um ou mais prestadores de serviço
        - **Período**: Defina um intervalo de datas específico

        #### Métricas e Dados
        - Visualize métricas consolidadas no topo da página
        - Examine os dados detalhados na tabela abaixo
        - Use as funcionalidades de ordenação e busca da tabela

        ### 3. Dicas Importantes
        - Para melhores resultados, use PDFs originais das notas fiscais
        - Os arquivos são processados localmente e não são armazenados
        - Recomenda-se processar lotes de até 50 arquivos por vez
        - Verifique sempre os dados extraídos para garantir a precisão

        ### 4. Resolução de Problemas
        #### Problemas Comuns
        - **Arquivo não processado**: Verifique se o PDF está em formato correto
        - **Dados faltando**: Certifique-se de que o PDF está legível
        - **Valores incorretos**: Confirme se o formato do PDF está padronizado

        #### Suporte
        Em caso de dúvidas ou problemas, entre em contato com o suporte técnico.
        """)
    
    # Footer
    st.markdown("---")
    st.markdown(
        """
        <div style='text-align: center'>
            <p style='color: #888;'>Desenvolvido com ❤️ | Extrator de dados nf's PDF Pro v1.0</p>
        </div>
        """,
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()