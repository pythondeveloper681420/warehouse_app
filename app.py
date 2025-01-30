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
    TOKEN_EXPIRY_HOURS = 24 * 7
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
            st.error(f"Error creating user: {e}")
            return False

    def create_token(self, token_data: Dict) -> bool:
        try:
            token_data['created_at'] = Config.get_current_utc_time()
            self.tokens.insert_one(token_data)
            return True
        except Exception as e:
            st.error(f"Error creating token: {e}")
            return False

    def find_token(self, token: str) -> Optional[Dict]:
        return self.tokens.find_one({"token": token})

    def update_user(self, email: str, update_data: Dict) -> bool:
        try:
            result = self.users.update_one(
                {"email": email},
                {"$set": update_data}
            )
            return result.modified_count > 0
        except Exception as e:
            st.error(f"Error updating user: {e}")
            return False

    def delete_token(self, token: str) -> bool:
        try:
            result = self.tokens.delete_one({"token": token})
            return result.deleted_count > 0
        except Exception as e:
            st.error(f"Error deleting token: {e}")
            return False

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
                subject="Account Validation - Warehouse System",
                html_content=Content("text/html", self._get_email_template(validation_url, name))
            )
            
            response = self.client.send(message)
            if response.status_code in [200, 202]:
                st.info(f"Validation email sent to {email}")
                return True
            
            st.error(f"Error sending email: {response.body}")
            return False
        except Exception as e:
            st.error(f"Error sending email: {e}")
            return False

    @staticmethod
    def _get_email_template(validation_url: str, name: str) -> str:
        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; }}
                .container {{ max-width: 600px; margin: 0 auto; background-color: #ffffff; padding: 20px; }}
                .button {{ display: inline-block; background-color: #0075be; color: white; text-decoration: none;
                          padding: 12px 24px; border-radius: 4px; transition: background-color 0.3s ease; }}
                .button:hover {{ background-color: #0056b3; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>Warehouse System - Account Validation</h1>
                <p>Hello {name},</p>
                <p>To validate your Warehouse System account, click the link below:</p>
                <p><a href="{validation_url}" class="button">Validate Account</a></p>
                <p>This link expires in 7 days.</p>
                <p>If you didn't request this validation, please ignore this email.</p>
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
            st.error("All fields are required")
            return False

        if not email.endswith(Config.ALLOWED_EMAIL_DOMAIN):
            st.error(f"Email must end with {Config.ALLOWED_EMAIL_DOMAIN}")
            return False

        if not password.isdigit() or len(password) != Config.MIN_PASSWORD_LENGTH:
            st.error(f"Password must be exactly {Config.MIN_PASSWORD_LENGTH} digits")
            return False

        if self.db.find_user(email):
            st.error("Email already registered")
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
            if self.db.create_token({"token": token, "email": email}):
                if self.email.send_validation_email(email, token, name):
                    st.success("Registration successful! Check your email to validate your account.")
                    return True
        
        st.error("Error creating user. Please try again.")
        return False

    def validate_token(self, token: str) -> bool:
        token_doc = self.db.find_token(token)
        if not token_doc:
            st.error("Invalid or expired token")
            return False

        token_creation = token_doc['created_at'].replace(tzinfo=timezone.utc)
        if Config.get_current_utc_time() > token_creation + timedelta(hours=Config.TOKEN_EXPIRY_HOURS):
            self.db.delete_token(token)
            st.error("Token expired. Please register again.")
            return False

        if self.db.update_user(token_doc['email'], {"verified": True}):
            self.db.delete_token(token)
            st.success("Account validated successfully! You can now login.")
            return True
        return False

    def login(self, email: str, password: str) -> bool:
        if not email or not password:
            st.error("Fill in all fields")
            return False

        user = self.db.find_user(email)
        if not user or not user.get('verified', False):
            st.error("Invalid credentials or unverified account")
            return False

        if user['password'] != hashlib.sha256(password.encode()).hexdigest():
            st.error("Invalid credentials")
            return False

        token = secrets.token_urlsafe(32)
        if self.db.create_token({"token": token, "email": email}):
            st.session_state.update({
                'auth_token': token,
                'user': {
                    'name': user['name'],
                    'email': user['email'],
                    'initials': self.get_initials(user['name'])
                },
                'logged_in': True
            })
            return True

        st.error("Login error. Please try again.")
        return False

    def logout(self):
        if 'auth_token' in st.session_state:
            self.db.delete_token(st.session_state.pop('auth_token'))
        st.session_state.logged_in = False
        st.session_state.user = None

    def check_login(self):
        if 'auth_token' in st.session_state:
            token_doc = self.db.find_token(st.session_state.auth_token)
            if token_doc:
                token_creation = token_doc['created_at'].replace(tzinfo=timezone.utc)
                if Config.get_current_utc_time() <= token_creation + timedelta(hours=Config.TOKEN_EXPIRY_HOURS):
                    user = self.db.find_user(token_doc['email'])
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
        
        self.user_service.check_login()

    def render_login_page(self):
        col1, col2 = st.columns([2, 1])
        col1.title("WarehouseApp")
        
        token = st.query_params.get("token")
        if token:
            self.user_service.validate_token(token)
            st.query_params.clear()
            st.rerun()
        
        tab1, tab2 = col1.tabs(["Login", "Register"])
        
        with tab1:
            with st.form("login_form"):
                email = st.text_input("Email").lower()
                password = st.text_input("Password", type="password", help="Numbers only")
                if st.form_submit_button("Login"):
                    if password.isdigit():
                        if self.user_service.login(email, password):
                            st.rerun()
                    else:
                        st.error("Password must contain only numbers")

        with tab2:
            with st.form("register_form"):
                name = st.text_input("Full Name")
                email = st.text_input("Email (@andritz.com)").lower()
                phone = st.text_input("Phone", placeholder="(XX) XXXXX-XXXX")
                password = st.text_input("Password (6 digits)", type="password", help="Numbers only")
                
                if st.form_submit_button("Register"):
                    if password.isdigit():
                        self.user_service.create_user(name, email, password, phone)
                    else:
                        st.error("Password must contain only numbers")
        
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
            if current_path != "00_home":
                st.switch_page("pages/00_home.py")

def main():
    WarehouseApp().run()

if __name__ == "__main__":
    main()