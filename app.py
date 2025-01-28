#pip freeze > requirements.txt
#pip install -r requirements.txt
#python -m venv .venv
#.venv\Scripts\activate.bat
#streamlit run app.py
# limpar terminal Ctrl + L
import streamlit as st
import pymongo
import urllib.parse
import hashlib
import secrets
import requests
from datetime import datetime, timedelta, timezone
import re
from dotenv import load_dotenv
import os
import sendgrid
from sendgrid.helpers.mail import Mail, Email, To, Content
import os
from numpy.typing import NDArray
import numpy as np

# Configuração da página
st.set_page_config(
    page_title="WarehouseApp",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# CSS para esconder elementos Streamlit
st.markdown("""
    <style>
    .main {
        overflow: auto;
    }
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .stApp [data-testid="stToolbar"] {
        display: none;
    }
    .reportview-container {
        margin-top: -2em;
    }
    .stDeployButton {display: none;}
    #stDecoration {display: none;}
    [data-testid="collapsedControl"] {
        display: none;
    }
    </style>
""", unsafe_allow_html=True)

class Config:
    """Classe de configuração com todas as constantes e configurações do sistema"""
    
    @staticmethod
    def get_sendgrid_api():
        """Obtém a chave API do SendGrid das variáveis de ambiente ou secrets do Streamlit"""
        try:
            return st.secrets["SENDGRID_API_KEY"]
        except:
            load_dotenv()
            return os.getenv("SENDGRID_API_KEY")

    @staticmethod
    def get_current_utc_time():
        """Retorna o tempo atual em UTC com timezone"""
        return datetime.now(timezone.utc)

    # Configurações do MongoDB
    MONGO_USERNAME = urllib.parse.quote_plus(st.secrets["MONGO_USERNAME"])
    MONGO_PASSWORD = urllib.parse.quote_plus(st.secrets["MONGO_PASSWORD"])
    MONGO_CLUSTER = st.secrets["MONGO_CLUSTER"]
    MONGO_DB = st.secrets["MONGO_DB"]

    # Configurações de Email
    SENDGRID_API_KEY = get_sendgrid_api()
    SENDER_NAME = "Sistema Warehouse"
    SENDER_EMAIL = "daniel.albuquerque@andritz.com"
    
    # URLs do sistema
    DEV_URL = "http://localhost:8501"
    PROD_URL = "https://warehouse-app.streamlit.app/"
    
    # Configurações de autenticação
    TOKEN_EXPIRY_HOURS = 24 * 7  # 7 dias
    MIN_PASSWORD_LENGTH = 6
    ALLOWED_EMAIL_DOMAIN = "@andritz.com"

class DatabaseConnection:
    """Classe para gerenciar a conexão com o MongoDB"""
    
    @staticmethod
    def get_client():
        """Cria e retorna uma conexão com o MongoDB"""
        try:
            connection_string = (
                f"mongodb+srv://{Config.MONGO_USERNAME}:{Config.MONGO_PASSWORD}"
                f"@{Config.MONGO_CLUSTER}/{Config.MONGO_DB}?retryWrites=true&w=majority"
            )
            return pymongo.MongoClient(connection_string)
        except Exception as e:
            st.error(f"Erro ao conectar ao MongoDB: {str(e)}")
            raise

class MongoDBManager:
    """Classe para gerenciar operações no MongoDB"""
    
    def __init__(self):
        self.client = DatabaseConnection.get_client()
        self.db = self.client[Config.MONGO_DB]
        self.users = self.db['users']
        self.tokens = self.db['tokens']
    
    def find_user(self, email):
        """Busca um usuário pelo email"""
        return self.users.find_one({"email": email})

    def create_user(self, user_data):
        """Cria um novo usuário"""
        try:
            user_data['created_at'] = Config.get_current_utc_time()
            return self.users.insert_one(user_data)
        except Exception as e:
            st.error(f"Erro ao criar usuário: {str(e)}")
            return None

    def create_token(self, token_data):
        """Cria um novo token"""
        try:
            token_data['created_at'] = Config.get_current_utc_time()
            return self.tokens.insert_one(token_data)
        except Exception as e:
            st.error(f"Erro ao criar token: {str(e)}")
            return None

    def find_token(self, token):
        """Busca um token"""
        return self.tokens.find_one({"token": token})

    def update_user(self, email, update_data):
        """Atualiza dados do usuário"""
        try:
            result = self.users.update_one(
                {"email": email},
                {"$set": update_data}
            )
            return result.modified_count > 0
        except Exception as e:
            st.error(f"Erro ao atualizar usuário: {str(e)}")
            return False

    def delete_token(self, token):
        """Deleta um token"""
        try:
            result = self.tokens.delete_one({"token": token})
            return result.deleted_count > 0
        except Exception as e:
            st.error(f"Erro ao deletar token: {str(e)}")
            return False

class EmailManager:
    """Classe para gerenciar o envio de emails com SendGrid"""
    
    def __init__(self):
        self.sg_client = sendgrid.SendGridAPIClient(api_key=Config.SENDGRID_API_KEY)
    
    def send_validation_email(self, email, token, name):
        """Envia email de validação para um novo usuário"""
        try:
            base_url = Config.PROD_URL if not st.session_state.get('dev_mode', False) else Config.DEV_URL
            validation_url = f"{base_url}?token={token}"
            
            from_email = Email(Config.SENDER_EMAIL, Config.SENDER_NAME)
            to_email = To(email, name)
            subject = "Validação de Conta - Sistema Warehouse"
            content = Content(
                "text/html", 
                self._get_email_template(validation_url, name)
            )

            mail = Mail(from_email, to_email, subject, content)
            
            response = self.sg_client.send(mail)
            
            if response.status_code in [200, 202]:
                st.info(f"Email de validação enviado para {email}")
                return True
            else:
                st.error(f"Erro ao enviar email: {response.body}")
                return False
                
        except Exception as e:
            st.error(f"Erro ao enviar email: {str(e)}")
            return False

    def _get_email_template(self, validation_url, name):
        """Retorna o template HTML do email de validação"""
        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                body {{
                    font-family: Arial, sans-serif;
                    margin: 0;
                    padding: 20px;
                }}
                .container {{
                    max-width: 600px;
                    margin: 0 auto;
                    background-color: #ffffff;
                    padding: 20px;
                }}
                .button {{
                    display: inline-block;
                    background-color: #0075be;
                    color: white;
                    text-decoration: none;
                    padding: 12px 24px;
                    border-radius: 4px;
                    transition: background-color 0.3s ease;
                }}
                .button:hover {{
                    background-color: #0056b3;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>Sistema Warehouse - Validação de Conta</h1>
                <p>Olá {name},</p>
                <p>Para validar sua conta no Sistema Warehouse, clique no link abaixo:</p>
                <p>
                    <a href="{validation_url}" class="button">Validar Conta</a>
                </p>
                <p>Este link expira em 7 dias.</p>
                <p>Se você não solicitou esta validação, ignore este email.</p>
            </div>
        </body>
        </html>
        """

class UserManager:
    """Classe para gerenciar operações relacionadas aos usuários"""
    
    def __init__(self, db_manager, email_manager):
        self.db = db_manager
        self.email = email_manager

    def get_initials(self, name):
        """Retorna as iniciais do nome do usuário"""
        parts = name.split()
        if len(parts) >= 2:
            return f"{parts[0][0]}{parts[-1][0]}".upper()
        return parts[0][0].upper() if parts else "U"

    def validate_registration_data(self, name, email, password, phone):
        """Valida os dados de registro do usuário"""
        if not all([name, email, password, phone]):
            st.error("Todos os campos são obrigatórios")
            return False

        if not email.endswith(Config.ALLOWED_EMAIL_DOMAIN):
            st.error(f"Email deve terminar com {Config.ALLOWED_EMAIL_DOMAIN}")
            return False

        if not password.isdigit() or len(password) != Config.MIN_PASSWORD_LENGTH:
            st.error(f"Senha deve conter exatamente {Config.MIN_PASSWORD_LENGTH} dígitos")
            return False

        if self.db.find_user(email):
            st.error("Email já cadastrado")
            return False

        return True

    def create_user(self, name, email, password, phone):
        """Cria um novo usuário"""
        if not self.validate_registration_data(name, email, password, phone):
            return False

        token = secrets.token_urlsafe(32)
        
        user_data = {
            "name": name,
            "email": email,
            "password": hashlib.sha256(password.encode()).hexdigest(),
            "phone": re.sub(r'\D', '', phone),
            "verified": False
        }
        
        if self.db.create_user(user_data):
            token_data = {
                "token": token,
                "email": email
            }
            if self.db.create_token(token_data):
                if self.email.send_validation_email(email, token, name):
                    st.success("Cadastro realizado! Verifique seu email para validar a conta.")
                    return True
        
        st.error("Erro ao criar usuário. Tente novamente.")
        return False

    def validate_token(self, token):
        """Valida um token de verificação de conta"""
        token_doc = self.db.find_token(token)
        if not token_doc:
            st.error("Token inválido ou expirado")
            return False

        token_creation = token_doc['created_at'].replace(tzinfo=timezone.utc)
        expiry_time = token_creation + timedelta(hours=Config.TOKEN_EXPIRY_HOURS)
        
        if Config.get_current_utc_time() > expiry_time:
            self.db.delete_token(token)
            st.error("Token expirado. Faça o cadastro novamente.")
            return False

        if self.db.update_user(token_doc['email'], {"verified": True}):
            self.db.delete_token(token)
            st.success("Conta validada com sucesso! Você já pode fazer login.")
            return True
        return False

    def login(self, email, password):
        """Realiza o login do usuário"""
        if not email or not password:
            st.error("Preencha todos os campos")
            return False

        user = self.db.find_user(email)
        if not user:
            st.error("Email ou senha incorretos")
            return False

        if not user.get('verified', False):
            st.error("Conta não verificada. Verifique seu email.")
            return False

        if user['password'] != hashlib.sha256(password.encode()).hexdigest():
            st.error("Email ou senha incorretos")
            return False

        token = secrets.token_urlsafe(32)
        token_data = {
            "token": token,
            "email": email
        }
        
        if self.db.create_token(token_data):
            st.session_state.auth_token = token
            st.session_state.user = {
                'name': user['name'],
                'email': user['email'],
                'initials': self.get_initials(user['name'])
            }
            st.session_state.logged_in = True
            return True

        st.error("Erro ao realizar login. Tente novamente.")
        return False

    def logout(self):
        """Realiza o logout do usuário"""
        if 'auth_token' in st.session_state:
            token = st.session_state.pop('auth_token')
            self.db.delete_token(token)
        st.session_state.logged_in = False
        st.session_state.user = None

    def check_login(self):
        """Verifica se o usuário está logado"""
        if 'auth_token' in st.session_state:
            token = st.session_state.auth_token
            token_doc = self.db.find_token(token)
            if token_doc:
                token_creation = token_doc['created_at'].replace(tzinfo=timezone.utc)
                expiry_time = token_creation + timedelta(hours=Config.TOKEN_EXPIRY_HOURS)
                
                if Config.get_current_utc_time() <= expiry_time:
                    user = self.db.find_user(token_doc['email'])
                    st.session_state.user = {
                        'name': user['name'],
                        'email': user['email'],
                        'initials': self.get_initials(user['name'])
                    }
                    st.session_state.logged_in = True
                    return
        self.logout()

class WarehouseApp:
    """Classe principal da aplicação"""
    
    def __init__(self):
        self.db_manager = MongoDBManager()
        self.email_manager = EmailManager()
        self.user_manager = UserManager(self.db_manager, self.email_manager)
        
        # Inicializa o estado da sessão
        if 'logged_in' not in st.session_state:
            st.session_state.logged_in = False
            
        self.user_manager.check_login()

    def validate_numeric_input(self, input_value):
        """Valida se a entrada contém apenas números"""
        return ''.join(filter(str.isdigit, input_value))
    
    def show_sidebar(self):
        """Exibe a barra lateral com informações do usuário"""
        if 'user' in st.session_state:
            with st.sidebar:
                st.markdown(
                    "<div style='display:flex;justify-content:center;'><h1>Bem-vindo</h1></div>",
                    unsafe_allow_html=True
                )
                col1, col2, col3 = st.columns([1, 1, 1])
                with col2:
                    initials = st.session_state.user['initials']
                    st.markdown(
                        f"""
                        <div style='
                            display:flex;
                            justify-content:center;
                            background-color:#0075be;
                            color:white;
                            border-radius:50%;
                            width:40px;
                            height:40px;
                            align-items:center;
                            font-size:18px;
                            font-weight:bold;
                            margin-bottom:2rem'
                        >{initials}</div>
                        """,
                        unsafe_allow_html=True
                    )
                
                col1, col2, col3 = st.columns([1, 1, 1])
                with col2:
                    if st.button("Sair"):
                        self.user_manager.logout()
                        st.rerun()

    def login_page(self):
        """Renderiza a página de login"""
        # Esconde elementos da interface que não são necessários na página de login
        st.markdown("""
            <style>
                [data-testid="collapsedControl"] {
                    display: none
                }
                .st-emotion-cache-w3nhqi {display: none}
                .stSidebar {display: none}
            </style>
        """, unsafe_allow_html=True)
        
        # Layout da página de login
        col1, col2 = st.columns([2, 1])
        
        col1.title("WarehouseApp")
        
        # Verifica se há um token de validação na URL
        token = st.query_params.get("token")
        if token:
            self.user_manager.validate_token(token)
            st.query_params.clear()
            st.rerun()
            
        # Cria as abas de login e cadastro
        tab1, tab2 = col1.tabs(["Login", "Cadastro"])
        
        # Aba de Login
        with tab1:
            with st.form("login_form"):
                email = st.text_input("Email").lower()
                # Força entrada apenas numérica para senha
                password = st.text_input("Senha", type="password", 
                    help="Digite apenas números")
                # Limpa caracteres não numéricos antes de processar
                if st.form_submit_button("Entrar"):
                    clean_password = self.validate_numeric_input(password)
                    if password != clean_password:
                        st.error("A senha deve conter apenas números")
                    else:
                        if self.user_manager.login(email, clean_password):
                            st.rerun()

        # Aba de Cadastro
        with tab2:
            with st.form("register_form"):
                name = st.text_input("Nome Completo")
                email = st.text_input("Email (@andritz.com)").lower()
                phone = st.text_input("Telefone", placeholder="(XX) XXXXX-XXXX")
                # Força entrada apenas numérica para senha
                password = st.text_input("Senha (6 dígitos)", type="password",
                    help="Digite apenas números")
                
                if st.form_submit_button("Cadastrar"):
                    # Limpa caracteres não numéricos antes de processar
                    clean_password = self.validate_numeric_input(password)
                    if password != clean_password:
                        st.error("A senha deve conter apenas números")
                    else:
                        self.user_manager.create_user(name, email, clean_password, phone)
                        
        col2.image("login (8).jpg", width=400)

    def main_page(self):
        """Renderiza a página principal do sistema"""
        # Layout principal com duas colunas
        col1, col2 = st.columns([3, 1], gap="large")
        
        with col1:
            st.title("Dashboard")
        
        with col2:
            # Container para o avatar do usuário
            with st.container():
                initials = st.session_state.user['initials']
                st.markdown(
                    f"""
                    <div style='
                        display: flex;
                        justify-content: flex-end;
                        padding-right: 10px;
                    '>
                        <div style='
                            display: flex;
                            justify-content: center;
                            background-color: #0075be;
                            color: white;
                            border-radius: 50%;
                            width: 40px;
                            height: 40px;
                            align-items: center;
                            font-size: 18px;
                            font-weight: bold;
                        '>{initials}</div>
                    </div>
                    """, 
                    unsafe_allow_html=True
                )

    def run(self):
        """Método principal para executar a aplicação"""
        # Esconde o botão de expandir/colapsar sidebar
        st.markdown("""
            <style>
                [data-testid="collapsedControl"] {
                    display: none
                }
            </style>
        """, unsafe_allow_html=True)
        
        # Redireciona para a página apropriada baseado no estado de login
        if not st.session_state.logged_in:
            self.login_page()
        else:
            self.main_page()

def main():
    """Função principal para iniciar a aplicação"""
    # Inicializa e executa a aplicação
    app = WarehouseApp()
    app.run()

if __name__ == "__main__":
    main()    