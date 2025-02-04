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
from datetime import datetime, timedelta, timezone
import re
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, To, Content
from functools import lru_cache
from typing import Dict, Optional, Any

# Page configuration
st.set_page_config(
    page_title="WarehouseApp",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Hide Streamlit elements
st.markdown("""
    <style>
    .main { overflow: auto; }
    #MainMenu, footer, .stDeployButton, #stDecoration, [data-testid="collapsedControl"],
    .st-emotion-cache-hzo1qh, .stSidebar { display: none; }
    .stApp [data-testid="stToolbar"] { display: none; }
    .reportview-container { margin-top: -2em; }
    </style>
""", unsafe_allow_html=True)

class Config:
    """System configuration with environment variables and constants"""
    MONGO_USERNAME = urllib.parse.quote_plus(st.secrets["MONGO_USERNAME"])
    MONGO_PASSWORD = urllib.parse.quote_plus(st.secrets["MONGO_PASSWORD"])
    MONGO_CLUSTER = st.secrets["MONGO_CLUSTER"]
    MONGO_DB = st.secrets["MONGO_DB"]
    SENDGRID_API_KEY = st.secrets.get("SENDGRID_API_KEY")
    SENDER_NAME = "Sistema Warehouse"
    SENDER_EMAIL = "daniel.albuquerque@andritz.com"
    DEV_URL = "http://localhost:8501"
    PROD_URL = "https://warehouse-app.streamlit.app/"
    AUTH_TOKEN_EXPIRY_HOURS = 24  # Authentication token expiry (1 day)
    VALIDATION_TOKEN_EXPIRY_HOURS = 24 * 7  # Email validation token expiry (7 days)
    MIN_PASSWORD_LENGTH = 6
    ALLOWED_EMAIL_DOMAIN = "@andritz.com"

    @staticmethod
    def get_current_utc_time() -> datetime:
        return datetime.now(timezone.utc)

@lru_cache(maxsize=1)
def get_mongodb_client() -> pymongo.MongoClient:
    """Create and cache MongoDB client connection"""
    connection_string = (
        f"mongodb+srv://{Config.MONGO_USERNAME}:{Config.MONGO_PASSWORD}"
        f"@{Config.MONGO_CLUSTER}/{Config.MONGO_DB}?retryWrites=true&w=majority"
    )
    return pymongo.MongoClient(connection_string)

class Database:
    """Database connection and operations manager"""
    def __init__(self):
        self.client = get_mongodb_client()
        self.db = self.client[Config.MONGO_DB]
        self.users = self.db['users']
        self.tokens = self.db['tokens']

    def find_user(self, email: str) -> Optional[Dict]:
        return self.users.find_one({"email": email})

    def create_user(self, user_data: Dict) -> bool:
        try:
            user_data['created_at'] = Config.get_current_utc_time()
            self.users.insert_one(user_data)
            return True
        except Exception as e:
            st.error(f"Erro ao criar usuário: {e}")
            return False

    def create_token(self, token_data: Dict) -> bool:
        try:
            current_time = Config.get_current_utc_time()
            expiry_hours = (Config.AUTH_TOKEN_EXPIRY_HOURS 
                          if token_data.get('type') == 'auth' 
                          else Config.VALIDATION_TOKEN_EXPIRY_HOURS)
            
            token_data.update({
                'created_at': current_time,
                'expires_at': current_time + timedelta(hours=expiry_hours)
            })
            self.tokens.insert_one(token_data)
            return True
        except Exception as e:
            st.error(f"Erro ao criar token: {e}")
            return False

    def find_valid_token(self, token: str) -> Optional[Dict]:
        token_doc = self.tokens.find_one({
            "token": token,
            "expires_at": {"$gt": Config.get_current_utc_time()}
        })
        return token_doc

    def update_user(self, email: str, update_data: Dict) -> bool:
        try:
            result = self.users.update_one(
                {"email": email},
                {"$set": update_data}
            )
            return result.modified_count > 0
        except Exception as e:
            st.error(f"Erro ao atualizar o usuário: {e}")
            return False

    def delete_token(self, token: str) -> bool:
        try:
            result = self.tokens.delete_one({"token": token})
            return result.deleted_count > 0
        except Exception as e:
            st.error(f"Erro ao excluir o token: {e}")
            return False

    def cleanup_expired_tokens(self):
        """Remove expired tokens from database"""
        try:
            self.tokens.delete_many({
                "expires_at": {"$lt": Config.get_current_utc_time()}
            })
        except Exception as e:
            st.error(f"Erro ao limpar tokens: {e}")

class EmailService:
    """Email service using SendGrid"""
    def __init__(self):
        self.client = SendGridAPIClient(api_key=Config.SENDGRID_API_KEY)

    def send_validation_email(self, email: str, token: str, name: str) -> bool:
        try:
            base_url = Config.PROD_URL if not st.session_state.get('dev_mode') else Config.DEV_URL
            validation_url = f"{base_url}?token={token}"
            
            message = Mail(
                from_email=Email(Config.SENDER_EMAIL, Config.SENDER_NAME),
                to_emails=To(email, name),
                subject="Validação de conta - Warehouse App",
                html_content=Content("text/html", self._get_email_template(validation_url, name))
            )
            
            response = self.client.send(message)
            if response.status_code in [200, 202]:
                st.info(f"E-mail de validação enviado para {email}")
                return True
            
            st.error(f"Erro ao enviar e-mail: {response.body}")
            return False
        except Exception as e:
            st.error(f"Erro ao enviar e-mail: {e}")
            return False

    @staticmethod
    def _get_email_template(validation_url: str, name: str) -> str:
        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <style>
                body {{
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
                    margin: 0;
                    padding: 20px;
                    line-height: 1.5;
                    color: #000;
                }}
                .logo-container {{
                    display: flex;
                    align-items: center;
                    gap: 8px;
                    margin-bottom: 20px;
                }}
                .checkmark {{
                    color: #22c55e;
                    font-size: 24px;
                }}
                .system-name {{
                    color: #0066cc;
                    font-size: 24px;
                    font-weight: normal;
                    margin: 0;
                }}
                .validation-button {{
                    display: inline-block;
                    background-color: #0066cc;
                    color: white;
                    text-decoration: none;
                    padding: 8px 16px;
                    border-radius: 4px;
                    margin: 20px 0;
                }}
                .footer {{
                    margin-top: 20px;
                    color: #666;
                    font-size: 14px;
                }}
                .footer p {{
                    margin: 8px 0;
                }}
            </style>
        </head>
        <body>
            <div class="logo-container">
                <span class="checkmark">✓</span>
                <span class="system-name">Warehouse System</span>
            </div>

            <p>Olá {name},</p>
            
            <p>Para ativar sua conta no Warehouse System, clique no botão abaixo:</p>
            
            <a href="{validation_url}" class="validation-button">Validar Minha Conta</a>

            <div class="footer">
                <p>⏳ Este link é válido por 7 dias</p>
                <p>❗ Se você não reconhece esta solicitação, por favor ignore este e-mail</p>
            </div>
        </body>
        </html>
        """

class UserService:
    """User management service"""
    def __init__(self, db: Database, email_service: EmailService):
        self.db = db
        self.email = email_service

    @staticmethod
    def get_initials(name: str) -> str:
        parts = name.split()
        return f"{parts[0][0]}{parts[-1][0]}".upper() if len(parts) >= 2 else parts[0][0].upper()

    def validate_registration(self, name: str, email: str, password: str, phone: str) -> bool:
        if not all([name, email, password, phone]):
            st.error("Todos os campos são obrigatórios")
            return False

        if not email.endswith(Config.ALLOWED_EMAIL_DOMAIN):
            st.error(f"O e-mail deve terminar com {Config.ALLOWED_EMAIL_DOMAIN}")
            return False

        if not password.isdigit() or len(password) != Config.MIN_PASSWORD_LENGTH:
            st.error(f"A senha deve ser exatamente {Config.MIN_PASSWORD_LENGTH} digitos")
            return False

        if self.db.find_user(email):
            st.error("E-mail já cadastrado")
            return False

        return True

    def create_user(self, name: str, email: str, password: str, phone: str) -> bool:
        if not self.validate_registration(name, email, password, phone):
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
            if self.db.create_token({
                "token": token, 
                "email": email,
                "type": "validation"
            }):
                if self.email.send_validation_email(email, token, name):
                    st.success("Cadastro realizado com sucesso! Verifique seu e-mail para validar sua conta.")
                    return True
        
        st.error("Erro ao criar usuário. Por favor, tente novamente.")
        return False

    def validate_token(self, token: str) -> bool:
        token_doc = self.db.find_valid_token(token)
        if not token_doc or token_doc.get('type') != 'validation':
            st.error("Token de validação inválido ou expirado")
            return False

        if self.db.update_user(token_doc['email'], {"verified": True}):
            self.db.delete_token(token)
            st.success("Conta validada com sucesso! Agora você pode fazer login.")
            return True
        return False

    def login(self, email: str, password: str) -> bool:
        if not email or not password:
            st.error("Preencha todos os campos")
            return False

        user = self.db.find_user(email)
        if not user or not user.get('verified', False):
            st.error("Credenciais inválidas ou conta não verificada")
            return False

        if user['password'] != hashlib.sha256(password.encode()).hexdigest():
            st.error("Credenciais inválidas")
            return False

        # Create new authentication token
        token = secrets.token_urlsafe(32)
        if self.db.create_token({
            "token": token,
            "email": email,
            "type": "auth"
        }):
            st.session_state.update({
                'auth_token': token,
                'user': {
                    'name': user['name'],
                    'email': user['email'],
                    'phone': user['phone'],
                    'initials': self.get_initials(user['name'])
                },
                'logged_in': True
            })
            return True

        st.error("Erro ao fazer login. Por favor, tente novamente.")
        return False

    def logout(self):
        if 'auth_token' in st.session_state:
            self.db.delete_token(st.session_state.pop('auth_token'))
        st.session_state.logged_in = False
        st.session_state.user = None

    def check_login(self):
        if 'auth_token' in st.session_state:
            token_doc = self.db.find_valid_token(st.session_state.auth_token)
            if token_doc and token_doc.get('type') == 'auth':
                user = self.db.find_user(token_doc['email'])
                if user:
                    st.session_state.update({
                        'user': {
                            'name': user['name'],
                            'email': user['email'],
                            'phone': user['phone'],
                            'initials': self.get_initials(user['name'])
                        },
                        'logged_in': True
                    })
                    return
        self.logout()

class WarehouseApp:
    """Main application class"""
    def __init__(self):
        self.db = Database()
        self.email_service = EmailService()
        self.user_service = UserService(self.db, self.email_service)
        
        if 'logged_in' not in st.session_state:
            st.session_state.logged_in = False
        
        # Clean up expired tokens periodically
        self.db.cleanup_expired_tokens()
        self.user_service.check_login()

    def render_login_page(self):
        col1, col2 = st.columns([2, 1])
        col1.title("WarehouseApp")
        
        token = st.query_params.get("token")
        if token:
            self.user_service.validate_token(token)
            st.query_params.clear()
            st.rerun()
        
        tab1, tab2 = col1.tabs(["Login", "Registro"])
        
        with tab1:
            with st.form("login_form"):
                email = st.text_input("Email").lower()
                password = st.text_input("Password", type="password", help="Numbers only")
                if st.form_submit_button("Login"):
                    if password.isdigit():
                        if self.user_service.login(email, password):
                            st.rerun()
                    else:
                        st.error("A senha deve conter apenas números")

        with tab2:
            with st.form("register_form"):
                name = st.text_input("Nome Completo")
                email = st.text_input("Email (@andritz.com)").lower()
                phone = st.text_input("N° Telefone", placeholder="(XX) XXXXX-XXXX")
                password = st.text_input("Senha (6 digits)", type="password", help="Numbers only")
                
                if st.form_submit_button("Salvar"):
                    if password.isdigit():
                        self.user_service.create_user(name, email, password, phone)
                    else:
                        st.error("A senha deve conter apenas números")
        
        col2.image("login (8).jpg", width=400)

    def render_main_page(self):
        col1, col2 = st.columns([3, 1], gap="large")
        
        with col1:
            st.title("Dashboard")
        
        with col2:
            initials = st.session_state.user['initials']
            st.markdown(f"""
                <div style='display: flex; justify-content: flex-end; padding-right: 10px;'>
                    <div style='display: flex; justify-content: center; background-color: #0075be;
                             color: white; border-radius: 50%; width: 40px; height: 40px;
                             align-items: center; font-size: 18px; font-weight: bold;'>
                        {initials}
                    </div>
                </div>
            """, unsafe_allow_html=True)

    def run(self):
        if not st.session_state.logged_in:
            self.render_login_page()
        else:
            current_path = st.query_params.get("page", "")
            # if current_path != "00_home":
            #     st.switch_page("pages/00_home.py")
            # if current_path != "06_editavel":
            #     st.switch_page("pages/06_editavel.py")
            if current_path != "01_epi_control":
                st.switch_page("pages/01_epi_control.py")
def main():
    WarehouseApp().run()

if __name__ == "__main__":
    main()