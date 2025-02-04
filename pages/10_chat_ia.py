import streamlit as st
from google.generativeai import configure, GenerativeModel
import google.generativeai as genai
import time

# Page configuration
st.set_page_config(
    page_title="Chatbot com Streamlit e Google AI",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="collapsed"
)


# Configuração da API do Google (use st.secrets para manter a chave segura)
configure(api_key=st.secrets["GOOGLE_API_KEY"])

# Inicialização do modelo
model = GenerativeModel('gemini-pro')

# Inicializa o histórico de mensagens na sessão se não existir
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []
if "chat" not in st.session_state:
    st.session_state.chat = model.start_chat(history=[])

def gerar_resposta(prompt):
    try:
        # Adiciona um indicador de "digitando"
        with st.spinner('Gerando resposta...'):
            # Gera a resposta usando o modelo
            response = st.session_state.chat.send_message(prompt)
            # Pequena pausa para evitar requisições muito rápidas
            time.sleep(0.5)
            return response.text
    except Exception as e:
        return f"Erro ao gerar resposta: {str(e)}"


st.markdown('## **🤖 :rainbow[Chatbot com Google AI]**')

# Usando um container para o chat
chat_container = st.container()

# Exibe o histórico da conversa
with chat_container:
    for autor, mensagem in st.session_state.chat_history:
        if autor == "Você":
            st.write(f"👤 **Você:** {mensagem}")
        else:
            st.write(f"🤖 **Assistente:** {mensagem}")

# Usando um formulário para gerenciar o input do usuário
with st.form("chat_form", clear_on_submit=True):
    col1, col2 = st.columns([6,1])
    with col1:
        entrada_usuario = st.text_input("Digite sua mensagem:", key="user_input")
    with col2:
        enviado = st.form_submit_button("Enviar")

    if enviado and entrada_usuario:
        # Armazena a mensagem do usuário
        st.session_state.chat_history.append(("Você", entrada_usuario))
        
        # Gera e armazena a resposta
        resposta_chat = gerar_resposta(entrada_usuario)
        st.session_state.chat_history.append(("Assistente", resposta_chat))
        
        # Recarrega a página para atualizar o chat
        st.rerun()

# Adiciona um botão para limpar o histórico
if st.button("Limpar Conversa"):
    st.session_state.chat_history = []
    st.session_state.chat = model.start_chat(history=[])
    st.rerun()