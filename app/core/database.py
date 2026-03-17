import logging
from fastapi import Request
from supabase import AsyncClient

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


async def update_document_status_in_supabase(id: str, status: str, sbdb: AsyncClient, error=None):
    logger.info("Updating document id=%s status=%s", id, status)
    payload = {"status": status}
    # if error:
    #     payload["error"] = error
    #     logger.error("Document id=%s error: %s", id, error)
    await sbdb.table("documents").update(payload).eq("id", id).execute()
    logger.info("Document id=%s updated and error = %s", id, error)


async def insert_ocr_result_to_supabase(document_id: str, ocr_data: list, sbdb: AsyncClient):
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

async def insert_mapper_result_to_supabase(mapped_data: dict, sbdb: AsyncClient, document_id: str, mongo_doc_id: int):

    rows = []
    for item in mapped_data["DocumentLines"]:
        rows.append({
            "document_id": document_id,
            "mongo_doc_id": mongo_doc_id,
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
    logger.info("Mapped results inserted for document_id=%s, mongo_doc_id=%s", document_id, mongo_doc_id)


async def get_document_by_id(document_id: str, sbdb: AsyncClient):
    """Get a single document by ID."""
    logger.info("Fetching document by id=%s", document_id)
    result = await sbdb.table("documents").select("*").eq("id", document_id).single().execute()
    return result.data


async def get_all_documents(sbdb: AsyncClient, status: str = None, search: str = None, limit: int = 100, offset: int = 0, date_from: str = None, date_to: str = None):
    """Get all documents with optional status filter, filename search, and date range."""
    logger.info("Fetching documents: status=%s, search=%s, limit=%d, offset=%d, date_from=%s, date_to=%s", status, search, limit, offset, date_from, date_to)

    # Count query (with same filters, but no pagination)
    count_query = sbdb.table("documents").select("id", count="exact")
    if status:
        count_query = count_query.eq("status", status)
    if search:
        count_query = count_query.ilike("key", f"%{search}%")
    if date_from:
        count_query = count_query.gte("created_at", date_from)
    if date_to:
        count_query = count_query.lte("created_at", date_to)
    count_result = await count_query.execute()
    total = count_result.count

    # Data query (with pagination)
    query = sbdb.table("documents").select("*")
    if status:
        query = query.eq("status", status)
    if search:
        query = query.ilike("key", f"%{search}%")
    if date_from:
        query = query.gte("created_at", date_from)
    if date_to:
        query = query.lte("created_at", date_to)
    query = query.order("created_at", desc=True).range(offset, offset + limit - 1)
    result = await query.execute()
    return result.data, total


async def get_ocr_results_by_document_id(document_id: str, sbdb: AsyncClient):
    """Get OCR results for a document."""
    logger.info("Fetching OCR results for document_id=%s", document_id)
    result = await sbdb.table("ocr_results").select("*").eq("document_id", document_id).execute()
    return result.data


async def get_mapped_results_by_document_id(document_id: str, sbdb: AsyncClient):
    """Get mapped results for a document."""
    logger.info("Fetching mapped results for document_id=%s", document_id)
    result = await sbdb.table("mapped_results").select("*").eq("document_id", document_id).execute()
    return result.data


async def get_document_signed_url(document_id: str, sbdb: AsyncClient, expires_in: int = 3600):
    """Get a signed URL for downloading a document's file."""
    logger.info("Generating signed URL for document_id=%s", document_id)
    doc = await get_document_by_id(document_id, sbdb)
    if not doc:
        return None
    bucket = doc.get("bucket")
    key = doc.get("key")
    signed = await sbdb.storage.from_(bucket).create_signed_url(key, expires_in)
    return signed.get("signedURL") or signed.get("signedUrl")


async def upload_file_to_storage(sbdb: AsyncClient, bucket: str, file_path: str, file_content: bytes, content_type: str = "application/pdf"):
    """Upload a file directly to Supabase storage."""
    logger.info("Uploading file to bucket=%s, path=%s", bucket, file_path)
    result = await sbdb.storage.from_(bucket).upload(file_path, file_content, {"content-type": content_type})
    logger.info("File uploaded successfully to bucket=%s, path=%s", bucket, file_path)
    return result
