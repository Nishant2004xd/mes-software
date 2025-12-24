import os
from flask import Flask
from flask_login import LoginManager  # 1. Import LoginManager
from config import Config

# Define base directory
basedir = os.path.abspath(os.path.dirname(__file__))

# Initialize Flask App
app = Flask(__name__,
            template_folder=os.path.join(os.path.dirname(basedir), 'templates'),
            static_folder=os.path.join(os.path.dirname(basedir), 'static')
            )

# Load Config
app.config.from_object(Config)

# --- 2. Initialize Login Manager ---
login_manager = LoginManager(app)
login_manager.login_view = 'login'  # Redirects unauthorized users to the 'login' route
login_manager.login_message_category = 'error'

# --- Import Routes ---
# This must be at the bottom to avoid circular import errors.
from app import routes