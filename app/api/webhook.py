import logging
from fastapi import APIRouter, Depends
from supabase import AsyncClient
from app.core.database import insert_document, get_supabase_client
from app.worker.worker import process_document

logger = logging.getLogger("app.webhook")

router = APIRouter()

@router.post("/storage-webhook")
async def storage_webhook(payload: dict, sbdb: AsyncClient = Depends(get_supabase_client)):

    logger.info("Received webhook payload: %s", payload)

    record = payload.get("record", {})

    bucket = record.get("bucket_id")
    key = record.get("name")

    if not bucket or not key:
        logger.warning("Ignored: missing bucket_id or name in record: %s", record)
        return {"status": "ignored"}

    # 1. Insert as pending
    logger.info("Inserting document: bucket=%s key=%s", bucket, key)
    doc = await insert_document(bucket, key, sbdb)
    doc_id = doc.get("id") if doc else None
    logger.info("Document queued: id=%s", doc_id)

    if not doc_id:
        return {"status": "error", "detail": "Failed to insert document"}

    # update_document(doc_id, "processing")
    # file_path = None
    # try:
    #     logger.info("Downloading: bucket=%s key=%s", bucket, key)
    #     file_path = download_from_supabase(bucket, key)
    #     logger.info("Downloaded %d bytes", os.path.getsize(file_path))

    #     logger.info("Sending to OCR: %s", key)
    #     ocr_result = send_to_ocr(file_path, filename=key)
    #     logger.info("OCR completed for %s", key)

    #     ocr_data = ocr_result.get("data", [])
    #     if ocr_data:
    #         insert_ocr_result(doc_id, ocr_data)
    #         logger.info("Inserted %d OCR result(s) for id=%s", len(ocr_data), doc_id)

    #     update_document(doc_id, "completed")
    #     logger.info("Document id=%s completed", doc_id)
    #     return {"status": "completed", "id": doc_id}

    # except Exception as e:
    #     logger.exception("Error processing document id=%s: %s", doc_id, e)
    #     update_document(doc_id, "error", error=str(e))
    #     return {"status": "error", "id": doc_id, "detail": str(e)}

    # finally:
    #     if file_path and os.path.exists(file_path):
    #         os.unlink(file_path)
    #         logger.info("Cleaned up temp file")

    await process_document(doc_id, bucket, key, sbdb)
    return {"status": "success", "document_id": doc_id}


