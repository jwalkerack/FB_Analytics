# azure_storage.py
import os
import json
import logging
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

ACCOUNT_URL = os.environ["AZURE_STORAGE_ACCOUNT_URL"]
ACCOUNT_NAME = os.environ["AZURE_STORAGE_ACCOUNT_NAME"]
ACCOUNT_KEY = os.environ["AZURE_STORAGE_ACCOUNT_KEY"]
CONTAINER_NAME = os.environ.get("AZURE_CONTAINER_NAME", "raw")
BLOB_MATCH_ID_PATH = os.environ.get("MATCH_ID_BLOB_PATH", "KEYS/MATCH_ID.json")
MATCH_DATA_FOLDER = os.environ.get("MATCH_DATA_FOLDER", "2025_2026")

_blob_service_client = BlobServiceClient(
    account_url=ACCOUNT_URL,
    credential=ACCOUNT_KEY,   # 👈 this is your storage account key
)

def _get_blob_client(path: str):
    return _blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=path)

def save_match_data_to_adls(match_data, filename, foldername=MATCH_DATA_FOLDER):
    try:
        json_data = json.dumps(match_data, indent=2, ensure_ascii=False)
        path = f"{foldername}/{filename}.json"
        blob_client = _get_blob_client(path)
        blob_client.upload_blob(json_data.encode("utf-8"), overwrite=True)
        logger.info(f"Match data uploaded to ADLS: {path}")
        return True
    except Exception as e:
        logger.error(f"Error uploading match data: {e}")
        return False

def get_json_from_adls():
    try:
        blob_client = _get_blob_client(BLOB_MATCH_ID_PATH)
        downloader = blob_client.download_blob()
        data = downloader.readall().decode("utf-8")
        return json.loads(data)
    except Exception as e:
        logger.error(f"Error fetching JSON from ADLS: {e}")
        return None

def update_json_in_adls(updated_dict):
    if not updated_dict:
        logger.error("Attempted to update ADLS with empty JSON data.")
        return False
    try:
        json_data = json.dumps(updated_dict, indent=2, ensure_ascii=False)
        blob_client = _get_blob_client(BLOB_MATCH_ID_PATH)
        blob_client.upload_blob(json_data.encode("utf-8"), overwrite=True)
        logger.info(f"JSON updated in ADLS: {CONTAINER_NAME}/{BLOB_MATCH_ID_PATH}")
        return True
    except Exception as e:
        logger.error(f"Error updating JSON in ADLS: {e}")
        return False
