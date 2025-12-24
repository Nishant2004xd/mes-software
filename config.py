# config.py

class Config:
    """
    Configuration class for the Flask application.
    Stores database connection details and other settings.
    """
    SECRET_KEY = 'nishant'  # Change this to your own random string
    # --- DATABASE CONFIGURATION ---

    # The name of your SQL Server instance.
    # This is the value you use to connect in SQL Server Management Studio.
    SERVER = 'DESKTOP-29MHU7D\\MSSQLSERVER01'

    # The name of the specific database you want to connect to.
    DATABASE = 'ELGI_Dash'

    # The ODBC driver for SQL Server.
    # 'ODBC Driver 17 for SQL Server' is a common modern choice.
    # Ensure this driver is installed on the machine running the application.
    DRIVER = '{ODBC Driver 17 for SQL Server}'

    # Since we are using Windows Authentication (Integrated Security),
    # a username and password are not required in this file.
    # The connection string will specify 'Trusted_Connection=yes'.

