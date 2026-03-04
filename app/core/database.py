from supabase import create_client
from app.core.config import SUPABASE_URL, SUPABASE_SERVICE_KEY

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

def insert_document(bucket: str, key: str):
    pass
    # supabase.table("documents").insert({
    #     "bucket": bucket,
    #     "key": key,
    #     "status": "pending"
    # }).execute()


def get_next_pending_document():
    pass
    # result = supabase.rpc("claim_next_document").execute()
    # if result.data:
    #     return result.data[0]
    # return None


def update_document(id: str, status: str, ocr_result=None, error=None):
    pass
    # payload = {"status": status}
    # if ocr_result:
    #     payload["ocr_result"] = ocr_result
    # if error:
    #     payload["error"] = error
    # supabase.table("documents").update(payload).eq("id", id).execute()