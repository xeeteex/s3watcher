import os
import logging
import time
import tempfile
import requests
from supabase import create_client
from app.core.database import get_next_pending_document, update_document, insert_ocr_result
from app.core.config import OCR_URL
from app.core.config import SUPABASE_URL, SUPABASE_SERVICE_KEY

logger = logging.getLogger("app.worker")

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


def download_from_supabase(bucket: str, key: str) -> str:
    logger.info("Downloading from storage: bucket=%s key=%s", bucket, key)
    response = supabase.storage.from_(bucket).download(key)
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    tmp.write(response)
    tmp.close()
    logger.info("Downloaded %d bytes to %s", len(response), tmp.name)
    return tmp.name


def send_to_ocr(file_path: str) -> dict:
    file_size = os.path.getsize(file_path)
    logger.info("Sending to OCR: %s (%d bytes)", file_path, file_size)
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
    result = response.json()
    logger.info("OCR response status: %s, items: %d", result.get("status"), len(result.get("data", [])))
    return result


def process_document(doc: dict):
    doc_id = doc["id"]
    bucket = doc["bucket"]
    key = doc["key"]

    logger.info("=== Processing document id=%s bucket=%s key=%s ===", doc_id, bucket, key)
    update_document(doc_id, "processing")

    file_path = None
    try:
        file_path = download_from_supabase(bucket, key)

        ocr_result = send_to_ocr(file_path)

        # Store OCR results in separate table
        ocr_data = ocr_result.get("data", [])
        if ocr_data:
            insert_ocr_result(doc_id, ocr_data)

        update_document(doc_id, "completed")
        logger.info("=== Document id=%s completed ===", doc_id)

    except Exception as e:
        logger.exception("Error processing document id=%s: %s", doc_id, e)
        update_document(doc_id, "error", error=str(e))

    finally:
        if file_path and os.path.exists(file_path):
            os.unlink(file_path)
            logger.info("Cleaned up temp file: %s", file_path)


def run_worker(poll_interval: int = 5):
    logger.info("Worker started. Polling every %ds for pending documents...", poll_interval)
    while True:
        doc = get_next_pending_document()
        if doc:
            process_document(doc)
        else:
            logger.debug("No pending documents. Waiting %ds...", poll_interval)
            time.sleep(poll_interval)


if __name__ == "__main__":
    run_worker()


