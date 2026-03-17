import logging
from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from supabase import AsyncClient
from app.core.database import get_supabase_client, insert_document
from app.worker.worker import process_document, continue_after_review

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

router = APIRouter(prefix="/webhook", tags=["Webhook"])

@router.post("/webhook", summary="Handle file upload webhook in Supabase", description="Endpoint to handle file upload webhook from Supabase storage.")
async def handle_webhook(payload: dict, supabasedb: AsyncClient = Depends(get_supabase_client)):

    try:
        record = payload.get("record", {})
        bucket = record.get("bucket", "")
        name = record.get("name", "")

        doc = await insert_document(bucket, name, supabasedb)
        doc_id = doc["id"] if doc else None

        if not doc_id:
            return JSONResponse(status_code=500, content={"status": "error", "detail": "Failed to insert document"})

        mongo_doc_id = await process_document(doc_id, bucket, name, supabasedb)
        logger.info(f"Document processed of id: {doc_id}, mongo_doc_id: {mongo_doc_id}")
        return JSONResponse(status_code=200, content={"document_id": doc_id, "mongo_doc_id": mongo_doc_id, "status": "pending_review"})

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": str(e)
            }
        )


@router.post("/continue/{mongo_doc_id}", summary="Continue processing after human review", description="Triggers mapper and SAP posting for an approved document.")
async def handle_continue_after_review(mongo_doc_id: int, supabasedb: AsyncClient = Depends(get_supabase_client)):

    try:
        await continue_after_review(mongo_doc_id, supabasedb)
        logger.info(f"Post-review processing completed for mongo_doc_id: {mongo_doc_id}")
        return JSONResponse(status_code=200, content={"mongo_doc_id": mongo_doc_id, "status": "completed"})

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "mongo_doc_id": mongo_doc_id,
                "message": str(e)
            }
        )
    
    





