import os
from flask import Flask
from config import Config
from flask_login import LoginManager  # 1. Import LoginManager

# Define base directory
basedir = os.path.abspath(os.path.dirname(__file__))

# Initialize Flask App
app = Flask(__name__,
            template_folder=os.path.join(os.path.dirname(basedir), 'templates'),
            static_folder=os.path.join(os.path.dirname(basedir), 'static')
            )

# Load Config
app.config.from_object(Config)

# 2. Initialize Login Manager (MUST be before importing routes)
login_manager = LoginManager(app)
login_manager.login_view = 'login'

# 3. Import Routes at the BOTTOM
from app import routes