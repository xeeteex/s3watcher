import logging
from fastapi import APIRouter, BackgroundTasks
from app.core.database import insert_document
from app.worker.worker import process_document_by_key

logger = logging.getLogger("app.webhook")

router = APIRouter()

@router.post("/storage-webhook")
async def storage_webhook(payload: dict, background_tasks: BackgroundTasks):

    logger.info("Received webhook payload: %s", payload)

    record = payload.get("record", {})
    
    bucket = record.get("bucket_id")
    key = record.get("name")

    if not bucket or not key:
        logger.warning("Ignored: missing bucket_id or name in record: %s", record)
        return {"status": "ignored"}

    logger.info("Inserting document: bucket=%s key=%s", bucket, key)
    doc = insert_document(bucket, key)
    logger.info("Document inserted: id=%s", doc.get("id") if doc else "unknown")

    # Trigger processing immediately in the background
    if doc:
        background_tasks.add_task(process_document_by_key, doc["id"], bucket, key)
        logger.info("Background task queued for document id=%s", doc["id"])

    return {"status": "accepted"}
    return {"status": "accepted"}


