import os
import urllib.parse
import pyodbc
from dotenv import load_dotenv

env_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(env_path)

SQL_SERVER = os.getenv("AZURE_SQL_SERVER")
SQL_DB = os.getenv("AZURE_SQL_DATABASE")
SQL_USER = os.getenv("AZURE_SQL_USER")
SQL_PASSWORD = os.getenv("AZURE_SQL_PASSWORD")

odbc_str = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={SQL_SERVER};"
    f"DATABASE={SQL_DB};"
    f"UID={SQL_USER};"
    f"PWD={SQL_PASSWORD};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connect Timeout=60;"
    "Login Timeout=60;"
)

print("ODBC string (without password):")
print(odbc_str.replace(SQL_PASSWORD, "****"))

conn = pyodbc.connect(odbc_str)
cursor = conn.cursor()
cursor.execute("SELECT 1")
print("DB says:", cursor.fetchone())
conn.close()