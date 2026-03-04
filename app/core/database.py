from supabase import create_client
from app.core.config import SUPABASE_URL, SUPABASE_SERVICE_KEY

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

def insert_document(bucket: str, key: str):

    supabase.table("documents").insert({
        "bucket": bucket,
        "key": key,
        "status": "pending"
    }).execute()


def get_next_pending_document():

    result = supabase.rpc("claim_next_document").execute()
    if result.data:
        return result.data[0]
    return None


def update_document(id: str, status: str, error=None):

    payload = {"status": status}
    if error:
        payload["error"] = error
    supabase.table("documents").update(payload).eq("id", id).execute()


def insert_ocr_result(document_id: str, ocr_data: list):
    """Insert OCR results into the ocr_results table.
    ocr_data is the 'data' array from the OCR response.
    Each item has file_name, content, and extracted_text.
    """
    rows = []
    for item in ocr_data:
        rows.append({
            "document_id": document_id,
            "file_name": item.get("file_name"),
            "content": item.get("content"),
            "extracted_text": item.get("extracted_text"),
        })
    supabase.table("ocr_results").insert(rows).execute()