import logging
import tempfile
import os
import httpx
from supabase import AsyncClient
from contextlib import asynccontextmanager
from ..core.config import settings
from ..core.database import update_document_status_in_supabase, insert_mapper_result_to_supabase, insert_ocr_result_to_supabase

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def temp_pdf(data: bytes):
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    try:
        tmp.write(data)
        tmp.close()
        yield tmp.name
    finally:
        os.unlink(tmp.name)
    

async def download_file_from_supabase(supabasedb: AsyncClient, bucket: str, name: str):

    file = await supabasedb.storage.from_(bucket).download(name)
    logger.info(f"Downloaded file of size {len(file)} bytes from Supabase storage: bucket={bucket}, name={name}")
    return file


async def upload_file_to_ocr(filepath: str, filename: str):

    async with httpx.AsyncClient(timeout=180) as client:
        with open(filepath, "rb") as f:
            files= [("filelist",(filename, f, "application/pdf"))]
            data = {"prompt": ""}
            response = await client.post(
                settings.OCR_URL.strip(),
                files = files,
                data = data,
                timeout = 180
            )
        response.raise_for_status()
        ocr_data = response.json()
        return ocr_data
    

# async def approve_ocr_result(ocr_data: dict):

#     document_id = str(ocr_data["data"][0]["document_id"]).strip()
#     aproval_api_url = f"{settings.REVIEW_URL.strip()}/{document_id}/approve"
#     async with httpx.AsyncClient(timeout=60) as client:
#         response = await client.get(aproval_api_url)
#     return response.json()


async def mapping_incoming_data(document_id: int):

    mapping_api_url = f"{settings.MAPPER_URL.strip()}/{document_id}"
    async with httpx.AsyncClient(timeout=60) as client:
        response = await client.get(mapping_api_url)
    mapped_data = response.json()["mapped_result"]
    return mapped_data


async def post_to_sap(document_id: int):
    
    sap_api_url = f"{settings.SAP_PURCHASE_API_URL.strip()}?document_id={document_id}"
    async with httpx.AsyncClient(timeout=60) as client:
        response = await client.post(sap_api_url)
    return response.json()


async def continue_after_review(mongo_doc_id: int, supabasedb: AsyncClient):

    try:
        result = await supabasedb.table("documents").select("id").eq("mongo_doc_id", mongo_doc_id).single().execute()
        supabase_doc_id = result.data["id"]
        await update_document_status_in_supabase(supabase_doc_id, "mapper_processing", supabasedb)

        mapped_data = await mapping_incoming_data(mongo_doc_id)
        if mapped_data:
            await insert_mapper_result_to_supabase(mapped_data, supabasedb, document_id=supabase_doc_id, mongo_doc_id=mongo_doc_id)
        logger.info(f"Mapper completed for document {mongo_doc_id}")

        await update_document_status_in_supabase(supabase_doc_id, "sap_processing", supabasedb)
        await post_to_sap(mongo_doc_id)
        logger.info(f"SAP post completed for document {mongo_doc_id}")

        await update_document_status_in_supabase(supabase_doc_id, "completed", supabasedb)

    except Exception as e:
        logger.error(f"Error in post-review processing for document {mongo_doc_id}: {str(e)}", exc_info=True)
        raise


async def process_document(doc_id: str, bucket: str, key: str, supabasedb: AsyncClient):

    try:
        
        # OCR Processing
        await update_document_status_in_supabase(doc_id, "ocr_processing", supabasedb)

        file_bytes = await download_file_from_supabase(supabasedb, bucket, key)

        async with temp_pdf(file_bytes) as file_path:
            ocr_result = await upload_file_to_ocr(file_path, filename=key)
        logger.info(f"OCR processing completed for document {doc_id}")

        ocr_data = ocr_result.get("data", [])
        if ocr_data:
            await insert_ocr_result_to_supabase(doc_id, ocr_data, supabasedb)
        logger.info(f"OCR data inserted for document {doc_id}")


        mongo_doc_id = ocr_result["data"][0]["document_id"]
        await supabasedb.table("documents").update({
           "status": "pending_review",
            "mongo_doc_id": mongo_doc_id
        }).eq("id", doc_id).execute()
        logger.info(f"Document {doc_id} set to pending_review with mongo_doc_id={mongo_doc_id}")
        return mongo_doc_id

        # Pending Review
        # await supabasedb.table("documents").update({"mongo_uid": ocr_result.get("document_id", "")}).eq("id", doc_id).execute()
        # await update_document_status_in_supabase(supabasedb, doc_id, "pending_review")




        # await approve_ocr_result(ocr_result)
        # logger.info(f"OCR result approved for document {doc_id}")

        # #Mapper Processing
        # await update_document_status_in_supabase(supabasedb, doc_id, "mapper_processing")

        # mapped_data = await mapping_incoming_data(ocr_result)
        # if mapped_data:
        #     await insert_mapper_result_to_supabase(mapped_data, supabasedb)
        # logger.info(f"Mapper data inserted for document {doc_id}")

        # #Sap Processing
        # await update_document_status_in_supabase(supabasedb, doc_id, "sap_processing")

        # await post_to_sap(ocr_result)
        # logger.info(f"Posted to SAP for document {doc_id}")

        # await update_document_status_in_supabase(supabasedb, doc_id, "completed")



    except Exception as e:
        # fetch the current status to know which step failed
        # response = await supabasedb.table("documents").select("status").eq("id", doc_id).single().execute()
        # failed_step = response.data.get("status", "unknown").replace("_processing", "_error")


        result = await supabasedb.table("documents").select("status").eq("id", doc_id).single().execute()
        failed_step = result.data.get("status", "unknown").replace("_processing", "_error")

        logger.error(f"Failed at {failed_step} for doc={doc_id}: {e}", exc_info=True)
        await update_document_status_in_supabase(doc_id, failed_step, supabasedb)
        raise