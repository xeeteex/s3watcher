import time
import tempfile
import requests
from supabase import create_client
from app.core.database import get_next_pending_document, update_document
from app.core.config import OCR_URL
from app.core.config import SUPABASE_URL, SUPABASE_SERVICE_KEY

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


def download_from_supabase(bucket: str, key: str) -> str:
    response = supabase.storage.from_(bucket).download(key)
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    tmp.write(response)
    tmp.close()
    return tmp.name


def send_to_ocr(file_path: str) -> dict:
    with open(file_path, "rb") as f:
        files = [("file_list", ("document.pdf", f, "application/pdf"))]
        data = {"prompt": ""}
        response = requests.post(
            OCR_URL,
            files=files,
            data=data,
            timeout=120,
        )
    response.raise_for_status()
    return response.json()


