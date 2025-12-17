import os
import json
from datetime import datetime
import logging
from azure.storage.blob import BlobServiceClient
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# --- Logging setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)



# Load environment variables from loader/.env
env_path = os.path.join(os.path.dirname(__file__), ".env")
if os.path.exists(env_path):
    load_dotenv(env_path)

ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")

SQL_SERVER = os.getenv("AZURE_SQL_SERVER")
SQL_DB = os.getenv("AZURE_SQL_DATABASE")
SQL_USER = os.getenv("AZURE_SQL_USER")
SQL_PASSWORD = os.getenv("AZURE_SQL_PASSWORD")

if not all([ACCOUNT_NAME, ACCOUNT_KEY, SQL_SERVER, SQL_DB, SQL_USER, SQL_PASSWORD]):
    raise RuntimeError("Missing one or more required environment variables.")


# ----- Storage connection -----
blob_service_client = BlobServiceClient(
    account_url=f"https://{ACCOUNT_NAME}.blob.core.windows.net",
    credential=ACCOUNT_KEY
)

# ----- SQL connection -----

import urllib.parse
from sqlalchemy import create_engine

# Build an ODBC connection string
odbc_str = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={SQL_SERVER};"        # we rely on default port 1433
    f"DATABASE={SQL_DB};"
    f"UID={SQL_USER};"
    f"PWD={SQL_PASSWORD};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connect Timeout=60;"          # correct ODBC keyword
    "Login Timeout=60;"
)

params = urllib.parse.quote_plus(odbc_str)

engine = create_engine(
    f"mssql+pyodbc:///?odbc_connect={params}",
    fast_executemany=True,
    pool_pre_ping=True,
    pool_recycle=1800,
    connect_args={"timeout": 60},
)


def list_json_blobs(container_name: str, prefix: str):
    """
    List all .json blobs under the given prefix (folder-like path).
    Example prefix: 'uk_football/season_2023_24/'
    """
    container_client = blob_service_client.get_container_client(container_name)
    blobs = container_client.list_blobs(name_starts_with=prefix)
    return [b.name for b in blobs if b.name.endswith(".json")]


def download_blob_text(container_name: str, blob_name: str) -> str:
    """
    Download a blob as text (UTF-8).
    """
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)
    data = blob_client.download_blob().readall()
    return data.decode("utf-8")


import time
from sqlalchemy.exc import OperationalError

def insert_raw_file_row(file_name: str, json_text: str, max_retries: int = 3, retry_delay: int = 5):
    """
    Insert one row into stg.raw_files for a single file, with simple retry logic
    on transient OperationalError (e.g. network / login timeouts).
    """
    insert_sql = text("""
        INSERT INTO stg.raw_files (file_name, json_body, load_timestamp)
        VALUES (:file_name, :json_body, :load_timestamp)
    """)

    row = {
        "file_name": file_name,
        "json_body": json_text,
        "load_timestamp": datetime.utcnow(),
    }

    attempt = 1
    while True:
        try:
            with engine.begin() as conn:
                conn.execute(insert_sql, [row])
            # success -> break out
            break

        except OperationalError as e:
            if attempt >= max_retries:
                # re-raise after final attempt so your outer try/except can log it
                raise

            print(
                f"OperationalError inserting {file_name} (attempt {attempt}/{max_retries}): {e}. "
                f"Retrying in {retry_delay} seconds..."
            )
            time.sleep(retry_delay)
            attempt += 1


def process_blob(container_name: str, blob_name: str):
    """
    Process a single blob:
      - download JSON
      - insert one row (file_name, json_body, load_timestamp)
    """
    print(f"Processing blob: {blob_name}")
    json_text = download_blob_text(container_name, blob_name)

    # Insert to DB
    insert_raw_file_row(blob_name, json_text)
    print(f"Inserted file {blob_name} into stg.raw_files.")


def main():
    container = os.getenv("ADLS_CONTAINER", "raw")
    prefix = os.getenv("ADLS_PREFIX", "2025_2026")  # default for local testing

    blobs = list_json_blobs(container, prefix)
    logger.info(f"Found {len(blobs)} JSON file(s) under '{prefix}'")

    for blob_name in blobs:
        try:
            process_blob(container, blob_name)
        except Exception as e:
            logger.exception(f"Error processing {blob_name}: {e}")


if __name__ == "__main__":
    main()
