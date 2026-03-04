import os
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


def process_document(doc: dict):
    doc_id = doc["id"]
    bucket = doc["bucket"]
    key = doc["key"]

    print(f"Processing: {bucket}/{key} (id={doc_id})")
    update_document(doc_id, "processing")

    file_path = None
    try:
        file_path = download_from_supabase(bucket, key)
        print(f"  Downloaded to {file_path} ({os.path.getsize(file_path)} bytes)")

        ocr_result = send_to_ocr(file_path)
        print(f"  OCR completed successfully")

        update_document(doc_id, "completed", ocr_result=ocr_result)
        print(f"  Document marked as completed")

    except Exception as e:
        print(f"  Error: {e}")
        update_document(doc_id, "error", error=str(e))

    finally:
        if file_path and os.path.exists(file_path):
            os.unlink(file_path)


def run_worker(poll_interval: int = 5):
    print("Worker started. Polling for pending documents...")
    while True:
        doc = get_next_pending_document()
        if doc:
            process_document(doc)
        else:
            print(f"  No pending documents. Waiting {poll_interval}s...")
            time.sleep(poll_interval)


if __name__ == "__main__":
    run_worker()


