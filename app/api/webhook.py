from fastapi import APIRouter
from app.core.database import insert_document

router = APIRouter()

@router.post("/storage-webhook")
async def storage_webhook(payload: dict):

    record = payload.get("record", {})
    bucket = record.get("bucket_id")
    key = record.get("name")

    if not bucket or not key:
        return {"status": "ignored"}

    insert_document(bucket, key)
    return {"status": "accepted"}


