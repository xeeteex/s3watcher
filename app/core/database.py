import logging
from fastapi import Request
from supabase import AsyncClient
from app.core.config import SUPABASE_URL, SUPABASE_SERVICE_KEY

logger = logging.getLogger("app.database")

# supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
# logger.info("Supabase client initialized")

async def get_supabase_client(request: Request)-> AsyncClient:
    return request.app.state.supabase


async def insert_document(bucket: str, key: str, sbdb: AsyncClient):
    logger.info("Inserting document: bucket=%s key=%s", bucket, key)
    result = await sbdb.table("documents").insert({
        "bucket": bucket,
        "key": key,
        "status": "pending"
    }).execute()
    doc = result.data[0] if result.data else None
    logger.info("Document inserted with status=pending, id=%s", doc.get("id") if doc else "unknown")
    return doc


# def get_next_pending_document():
#     logger.info("Claiming next pending document...")
#     result = supabase.rpc("claim_next_document").execute()
#     if result.data:
#         doc = result.data[0]
#         logger.info("Claimed document: id=%s bucket=%s key=%s", doc.get("id"), doc.get("bucket"), doc.get("key"))
#         return doc
#     logger.info("No pending documents found")
#     return None


async def update_document(id: str, status: str, sbdb: AsyncClient, error=None):
    logger.info("Updating document id=%s status=%s", id, status)
    payload = {"status": status}
    # if error:
    #     payload["error"] = error
    #     logger.error("Document id=%s error: %s", id, error)
    await sbdb.table("documents").update(payload).eq("id", id).execute()
    logger.info("Document id=%s updated and error = %s", id, error)


async def insert_ocr_result(document_id: str, ocr_data: list, sbdb: AsyncClient):
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
    await sbdb.table("ocr_results").insert(rows).execute()
    logger.info("OCR results inserted for document_id=%s", document_id)

async def insert_mapper_result(mapped_data: dict, sbdb: AsyncClient):

    rows = []
    for item in mapped_data["DocumentLines"]:
        rows.append({
            "CardName": mapped_data.get("CardName") or None,
            "CardCode": mapped_data.get("CardCode") or None,
            "DocDate": mapped_data.get("DocDate") or None,
            "DocLines": mapped_data.get("DocumentLines", []) or None,
            "ItemCode": item.get("ItemCode") or None,
            "Description": item.get("Description") or None,
            "Quantity": item.get("Quantity") or None,
            "TaxCode": item.get("TaxCode") or None,
            "UnitPrice": item.get("UnitPrice") or None
        })


    await sbdb.table("mapped_results").insert(rows).execute()
    logger.info("Mapped results inserted ")