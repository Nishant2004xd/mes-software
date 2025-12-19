import os
from flask import Flask
from config import Config

basedir = os.path.abspath(os.path.dirname(__file__))

app = Flask(__name__,
            template_folder=os.path.join(os.path.dirname(basedir), 'templates'),
            static_folder=os.path.join(os.path.dirname(basedir), 'static')
            )

app.config.from_object(Config)

from app import routes