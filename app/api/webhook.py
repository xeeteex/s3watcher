import logging
from fastapi import APIRouter
from app.core.database import insert_document

logger = logging.getLogger("app.webhook")

router = APIRouter()

@router.post("/storage-webhook")
async def storage_webhook(payload: dict):

    logger.info("Received webhook payload: %s", payload)

    record = payload.get("record", {})
    
    bucket = record.get("bucket_id")
    key = record.get("name")

    if not bucket or not key:
        logger.warning("Ignored: missing bucket_id or name in record: %s", record)
        return {"status": "ignored"}

    logger.info("Inserting document: bucket=%s key=%s", bucket, key)
    insert_document(bucket, key)
    logger.info("Document inserted successfully")
    return {"status": "accepted"}


