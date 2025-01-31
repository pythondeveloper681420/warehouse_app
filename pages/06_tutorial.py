import streamlit as st  

def main():  
    st.set_page_config(  
        page_title="Tutorial Completo",  
        page_icon="📚",  
        layout="wide"  
    )  

    st.title("📖 Tutorial Completo do Sistema")  
    st.markdown("---")  

    with st.expander("**🏠 Página Home - Visão Geral**", expanded=True):  
        st.markdown("""  
        ### Funcionalidades Principais:  
        - **Visualização de dados** de 3 coleções principais: XML, PO e NFS PDF  
        - **Filtros avançados** com busca por texto e seleção múltipla  
        - **Controle de colunas** visíveis com ordenação personalizada  
        - **Exportação de dados** para Excel com filtros aplicados  

        ### Como Usar:  
        1. Selecione a aba da coleção desejada  
        2. Use o menu de colunas para escolher quais informações exibir  
        3. Aplique filtros específicos usando os campos de busca  
        4. Navegue entre páginas usando os controles de paginação  
        5. Baixe os dados usando o botão de download  
        """)  

    with st.expander("**📤 Upload de Arquivos - Integração com MongoDB**"):  
        st.markdown("""  
        ### Principais Recursos:  
        - **Upload de arquivos Excel** para o MongoDB  
        - **Remoção inteligente** de duplicatas  
        - **Controle de versão** automático com data de criação  
        - Suporte para **grandes volumes de dados**  

        ### Fluxo de Trabalho:  
        1. Na aba **Upload de Dados**:  
           - Selecione o arquivo Excel  
           - Defina o nome da coleção  
           - Verifique a prévia dos dados  
           - Execute o upload  

        2. Na aba **Limpeza de Dados**:  
           - Selecione a coleção para limpeza  
           - Escolha o campo chave para identificação de duplicatas  
           - Selecione o método de limpeza (rápido ou em lotes)  

        ⚠️ **Dica:** Use o método em lotes para coleções com mais de 100 mil registros  
        """)  

    with st.expander("**📑 Processamento de Pedidos de Compra (PO)**"):  
        st.markdown("""  
        ### Funcionalidades Chave:  
        - **Consolidação de múltiplos arquivos** Excel  
        - **Cálculo automático** de valores totais  
        - **Formatação padronizada** de valores monetários  
        - **Geração de identificadores únicos** para itens  

        ### Passo a Passo:  
        1. Selecione os arquivos de PO  
        2. Revise as métricas de processamento  
        3. Baixe o arquivo consolidado  
        4. Use a visualização para análise rápida:  
           - Filtro global por texto  
           - Métricas de fornecedores e valores  
           - Ordenação por datas  
        """)  

    with st.expander("**📃 Processamento de XML - Notas Fiscais**"):  
        st.markdown("""  
        ### Recursos Principais:  
        - **Leitura automatizada** de arquivos XML  
        - **Integração com dados de PO** do MongoDB  
        - **Classificação automática** por categorias fiscais  
        - **Georreferenciamento** de endereços  

        ### Fluxo Ideal:  
        1. Faça upload dos XMLs  
        2. Revise os dados extraídos:  
           - Dados do emitente/destinatário  
           - Valores fiscais  
           - Classificação por CFOP  
        3. Utilize os dados complementares:  
           - Projetos relacionados  
           - Centros de custo  
           - Histórico de pagamentos  
        4. Exporte para análise detalhada  
        """)  

    with st.expander("**📝 Processamento de PDF - Notas de Serviço**"):  
        st.markdown("""  
        ### Destaques:  
        - **OCR inteligente** para diferentes layouts  
        - **Reconhecimento de padrões** fiscais  
        - **Vinculação automática** com números de PO  
        - **Consolidação temporal** por competência  

        ### Melhores Práticas:  
        1. Organize os PDFs por período  
        2. Verifique a qualidade da digitalização  
        3. Use os filtros pós-processamento:  
           - Período fiscal  
           - Prestadores de serviço  
           - Valores líquidos  
        4. Cruze dados com outras fontes  
        """)  

    st.markdown("---")  
    st.subheader("🛠 Suporte Técnico")  
    col1, col2 = st.columns(2)  
    with col1:  
        st.markdown("**Problemas Comuns:**")  
        st.write("- Formatação inconsistente de arquivos")  
        st.write("- Timeout em processamentos grandes")  
        st.write("- Dados ausentes em PDFs digitalizados")  

    with col2:  
        st.markdown("**Soluções Recomendadas:**")  
        st.write("- Padronize modelos de arquivos")  
        st.write("- Divida processamentos grandes em lotes")  
        st.write("- Verifique resolução de documentos escaneados")  

    st.markdown("---")  
    st.markdown("**📧 Contato:** suporte@empresa.com | 📞 (11) 99999-9999")  

if __name__ == "__main__":  
    main()  