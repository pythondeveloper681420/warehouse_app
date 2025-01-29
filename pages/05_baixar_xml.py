import streamlit as st

# Definir o layout expandido
st.set_page_config(layout="wide")

# Título da página
st.title("Visualizador de Sites DANFE")

# Criar as abas
tab1, tab2 = st.tabs(["Consulta DANFE", "Meu DANFE"])

# HTML para iframe responsivo
def responsive_iframe(url):
    iframe_code = f"""
    <style>
        .iframe-container {{
            position: relative;
            width: 100%;
            height: 0;
            padding-bottom: 75%; /* Ajusta a altura relativa à largura */
        }}
        .iframe-container iframe {{
            position: absolute;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            border: none;
        }}
    </style>
    <div class="iframe-container">
        <iframe src="{url}"></iframe>
    </div>
    """
    return iframe_code

# Aba "Consulta DANFE"
with tab1:
    st.header("Consulta DANFE")
    url_consulta = st.text_input("Insira a URL do site que deseja abrir:", "https://consultadanfe.com/")
    st.markdown(f"[Clique aqui para abrir o site]({url_consulta})")
    st.markdown(responsive_iframe(url_consulta), unsafe_allow_html=True)

# Aba "Meu DANFE"
with tab2:
    st.header("Meu DANFE")
    url_meu_danfe = st.text_input("Insira a URL do site que deseja abrir:", "https://meudanfe.com.br/")
    st.markdown(f"[Clique aqui para abrir o site]({url_meu_danfe})")
    st.markdown(responsive_iframe(url_meu_danfe), unsafe_allow_html=True)
