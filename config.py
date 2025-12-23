import os

class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'default-key-for-dev'
    
    # Azure Configuration
    # We use os.environ.get so we can set these securely in the Azure Portal
    SERVER = os.environ.get('DB_SERVER') 
    DATABASE = os.environ.get('DB_NAME') 
    USERNAME = os.environ.get('DB_USER') 
    PASSWORD = os.environ.get('DB_PASS') 
    
    # Azure Linux App Service usually has Driver 17 or 18 installed
    DRIVER = '{ODBC Driver 18 for SQL Server}'