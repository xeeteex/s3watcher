import os
import logging
import time
import tempfile
import requests
from supabase import create_client
from app.core.database import update_document, insert_ocr_result
from app.core.config import OCR_URL
from app.core.config import SUPABASE_URL, SUPABASE_SERVICE_KEY
from contextlib import contextmanager

logger = logging.getLogger("app.worker")

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


@contextmanager
def temp_pdf(data: bytes):

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    try:
        tmp.write(data)
        tmp.close()
        yield tmp.name
    finally:
        os.unlink(tmp.name)


def download_file_from_supabase(bucket: str, key: str):

   data = supabase.storage.from_(bucket).download(key)
   logger.info(f"Downloaded file size: {len(data)}")
   return data



def send_to_ocr(file_path: str, filename: str = "document.pdf") -> dict:
    file_size = os.path.getsize(file_path)
    logger.info("Sending to OCR: %s as '%s' (%d bytes)", file_path, filename, file_size)
    with open(file_path, "rb") as f:
        files = [("file_list", (filename, f, "application/pdf"))]
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

def process_document(doc_id: str, bucket: str, key: str):

    logger.info(f"Processing document {doc_id} from bucket {bucket} with key {key}")
    update_document(doc_id, "processing")

    try:
        file_bytes = download_file_from_supabase(bucket, key)

        with temp_pdf(file_bytes) as file_path:
            ocr_result = send_to_ocr(file_path, filename=key)
        logger.info(f"OCR processing completed for document {doc_id}")

        ocr_data = ocr_result.get("data", [])
        if ocr_data:
            insert_ocr_result(doc_id, ocr_data)
        logger.info(f"OCR data inserted for document {doc_id}")

        update_document(doc_id, "completed")

    except Exception as e:
        logger.error(f"Error processing document {doc_id}: {str(e)}")
        update_document(doc_id, "error")


# def process_one(doc: dict):
#     """Process a single claimed document (already status=processing)."""
#     doc_id = doc["id"]
#     bucket = doc["bucket"]
#     key = doc["key"]

#     logger.info("=== Processing document id=%s bucket=%s key=%s ===", doc_id, bucket, key)

#     file_path = None
#     try:
#         file_path = download_from_supabase(bucket, key)

#         ocr_result = send_to_ocr(file_path, filename=key)

#         ocr_data = ocr_result.get("data", [])
#         if ocr_data:
#             insert_ocr_result(doc_id, ocr_data)

#         update_document(doc_id, "completed")
#         logger.info("=== Document id=%s completed ===", doc_id)

#     except Exception as e:
#         logger.exception("Error processing document id=%s: %s", doc_id, e)
#         update_document(doc_id, "error", error=str(e))

#     finally:
#         if file_path and os.path.exists(file_path):
#             os.unlink(file_path)
#             logger.info("Cleaned up temp file: %s", file_path)


# def drain_queue() -> int:
#     """Claim and process pending documents one-by-one until none remain.
#     Returns the number of documents processed.
#     """
#     processed = 0
#     while True:
#         doc = get_next_pending_document()
#         if not doc:
#             logger.info("Queue empty, no more pending documents")
#             break
#         process_one(doc)
#         processed += 1
#     return processed


# def run_worker(poll_interval: int = 5):
#     """Standalone polling worker (for non-serverless environments)."""
#     logger.info("Worker started. Polling every %ds for pending documents...", poll_interval)
#     while True:
#         count = drain_queue()
#         if count == 0:
#             logger.debug("No pending documents. Waiting %ds...", poll_interval)
#             time.sleep(poll_interval)


# if __name__ == "__main__":
#     run_worker()


