import logging
from supabase import create_client
from app.core.config import SUPABASE_URL, SUPABASE_SERVICE_KEY

logger = logging.getLogger("app.database")

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
logger.info("Supabase client initialized")


def insert_document(bucket: str, key: str):
    logger.info("Inserting document: bucket=%s key=%s", bucket, key)
    result = supabase.table("documents").insert({
        "bucket": bucket,
        "key": key,
        "status": "pending"
    }).execute()
    doc = result.data[0] if result.data else None
    logger.info("Document inserted with status=pending, id=%s", doc.get("id") if doc else "unknown")
    return doc


def get_next_pending_document():
    logger.info("Claiming next pending document...")
    result = supabase.rpc("claim_next_document").execute()
    if result.data:
        doc = result.data[0]
        logger.info("Claimed document: id=%s bucket=%s key=%s", doc.get("id"), doc.get("bucket"), doc.get("key"))
        return doc
    logger.info("No pending documents found")
    return None


def update_document(id: str, status: str, error=None):
    logger.info("Updating document id=%s status=%s", id, status)
    payload = {"status": status}
    if error:
        payload["error"] = error
        logger.error("Document id=%s error: %s", id, error)
    supabase.table("documents").update(payload).eq("id", id).execute()
    logger.info("Document id=%s updated", id)


def insert_ocr_result(document_id: str, ocr_data: list):
    """Insert OCR results into the ocr_results table.
    ocr_data is the 'data' array from the OCR response.
    Each item has file_name, content, and extracted_text.
    """
    logger.info("Inserting %d OCR result(s) for document_id=%s", len(ocr_data), document_id)
    rows = []
    for item in ocr_data:
        rows.append({
            "document_id": document_id,
            "file_name": item.get("file_name"),
            "content": item.get("content"),
            "extracted_text": item.get("extracted_text"),
        })
    supabase.table("ocr_results").insert(rows).execute()
    logger.info("OCR results inserted for document_id=%s", document_id)