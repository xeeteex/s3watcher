import os
import logging
import tempfile
import httpx
from supabase import AsyncClient
from app.core.database import update_document, insert_ocr_result, insert_mapper_result
from app.core.config import OCR_URL, MAPPER_URL, REVIEW_URL, SAP_PURCHASE_API_URL
from contextlib import asynccontextmanager

logger = logging.getLogger("app.worker")

# supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


@asynccontextmanager
async def temp_pdf(data: bytes):

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    try:
        tmp.write(data)
        tmp.close()
        yield tmp.name
    finally:
        os.unlink(tmp.name)


async def download_file_from_supabase(sbdb: AsyncClient,bucket: str, key: str):

   data = await sbdb.storage.from_(bucket).download(key)
   logger.info(f"Downloaded file size: {len(data)}")
   return data


async def send_to_ocr(file_path: str, filename: str = "document.pdf") -> dict:
    
    file_size = os.path.getsize(file_path)
    logger.info("Sending to OCR: %s as '%s' (%d bytes)", file_path, filename, file_size)
    async with httpx.AsyncClient(timeout=180) as client:
        with open(file_path, "rb") as f:
            files = [("file_list", (filename, f, "application/pdf"))]
            data = {"prompt": ""}
            response = await client.post(
                OCR_URL,
                files=files,
                data=data,
                timeout=120,
            )
    response.raise_for_status()
    result = response.json()
    logger.info("OCR response status: %s, items: %d", result.get("status"), len(result.get("data", [])))
    return result


async def approve_ocr_result(extracted_data:str):

    incoming_doc_id = str(extracted_data["data"][0]["document_id"]).strip()
    aproval_api_url = f"{REVIEW_URL.strip()}/{incoming_doc_id}/approve"
    async with httpx.AsyncClient(timeout=60) as client:
        response = await client.post(aproval_api_url)
    return response.json()


async def mapping_incoming_data(extracted_data: dict):

    incoming_doc_id = str(extracted_data["data"][0]["document_id"]).strip()
    mapping_api_url = f"{MAPPER_URL.strip()}/{incoming_doc_id}"
    async with httpx.AsyncClient(timeout=60) as client:
        response = await client.get(mapping_api_url)
    mapped_data = response.json()["mapped_result"]
    return mapped_data


async def post_to_sap(extracted_data: dict):
    incoming_doc_id = str(extracted_data["data"][0]["document_id"]).strip()
    sap_api_url = f"{SAP_PURCHASE_API_URL.strip()}?document_id={incoming_doc_id}"
    async with httpx.AsyncClient(timeout=60) as client:
        response = await client.post(sap_api_url)
    return response.json()


async def process_document(doc_id: str, bucket: str, key: str, sbdb: AsyncClient):

    await update_document(doc_id, "processing", sbdb)

    try:
        file_bytes = await download_file_from_supabase(sbdb, bucket, key)

        async with temp_pdf(file_bytes) as file_path:
            ocr_result = await send_to_ocr(file_path, filename=key)
        logger.info(f"OCR processing completed for document {doc_id}")

        ocr_data = ocr_result.get("data", [])
        if ocr_data:
            await insert_ocr_result(doc_id, ocr_data, sbdb)
        logger.info(f"OCR data inserted for document {doc_id}")

        await approve_ocr_result(ocr_result)
        logger.info(f"OCR result approved for document {doc_id}")

        mapped_data = await mapping_incoming_data(ocr_result)
        if mapped_data:
            await insert_mapper_result(mapped_data, sbdb)
        logger.info(f"Mapper data inserted for document {doc_id}")

        await post_to_sap(ocr_result)
        logger.info(f"Posted to SAP for document {doc_id}")

        await update_document(doc_id, "completed", sbdb)

    except Exception as e:
        logger.error(f"Error processing document {doc_id}: {str(e)}")
        await update_document(doc_id, "error", sbdb)


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


