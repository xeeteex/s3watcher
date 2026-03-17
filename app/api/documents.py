import logging
from typing import Optional
from fastapi import APIRouter, Depends, UploadFile, File, Query, HTTPException
from fastapi.responses import JSONResponse
from supabase import AsyncClient
from app.core.database import (
    get_supabase_client,
    get_document_by_id,
    get_all_documents,
    get_document_signed_url,
    upload_file_to_storage,
    insert_document,
    get_ocr_results_by_document_id,
    get_mapped_results_by_document_id,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

router = APIRouter(prefix="/documents", tags=["Documents"])


@router.post("/upload", summary="Upload file to Supabase storage", description="Upload a file directly to Supabase storage bucket and create a document record.")
async def upload_document(
    file: UploadFile = File(...),
    bucket: str = Query(default="documents", description="Storage bucket name"),
    supabasedb: AsyncClient = Depends(get_supabase_client)
):
    """
    Upload a file to Supabase storage and insert a document record.
    """
    try:
        file_content = await file.read()
        file_path = file.filename

        await upload_file_to_storage(supabasedb, bucket, file_path, file_content, file.content_type or "application/pdf")

        doc = await insert_document(bucket, file_path, supabasedb)
        if not doc:
            raise HTTPException(status_code=500, detail="Failed to create document record")

        logger.info(f"Document uploaded: bucket={bucket}, path={file_path}, id={doc['id']}")
        return JSONResponse(
            status_code=201,
            content={
                "status": "success",
                "message": "File uploaded successfully",
                "document_id": doc["id"],
                "bucket": bucket,
                "key": file_path,
            }
        )
    except Exception as e:
        logger.error(f"Upload failed: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/", summary="List all documents", description="Get a list of all documents with optional status and date filtering.")
async def list_documents(
    status: Optional[str] = Query(default=None, description="Filter by status (e.g., pending, completed, pending_review)"),
    search: Optional[str] = Query(default=None, description="Search by filename (case-insensitive partial match)"),
    date_from: Optional[str] = Query(default=None, description="Filter documents created on or after this date (ISO format, e.g. 2026-03-01)"),
    date_to: Optional[str] = Query(default=None, description="Filter documents created on or before this date (ISO format, e.g. 2026-03-17)"),
    limit: int = Query(default=100, ge=1, le=500, description="Maximum number of documents to return"),
    offset: int = Query(default=0, ge=0, description="Number of documents to skip"),
    supabasedb: AsyncClient = Depends(get_supabase_client)
):
    """
    Retrieve all documents with optional filters.
    """
    try:
        documents, total = await get_all_documents(supabasedb, status=status, search=search, limit=limit, offset=offset, date_from=date_from, date_to=date_to)
        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "count": len(documents),
                "total": total,
                "limit": limit,
                "offset": offset,
                "documents": documents,
            }
        )
    except Exception as e:
        logger.error(f"Failed to list documents: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{document_id}", summary="Get document details", description="Get details of a specific document by ID, including OCR and mapped results.")
async def get_document(
    document_id: str,
    supabasedb: AsyncClient = Depends(get_supabase_client)
):
    """
    Retrieve a single document by ID with its OCR and mapped results.
    """
    try:
        document = await get_document_by_id(document_id, supabasedb)
        if not document:
            raise HTTPException(status_code=404, detail="Document not found")

        ocr_results = await get_ocr_results_by_document_id(document_id, supabasedb)
        mapped_results = await get_mapped_results_by_document_id(document_id, supabasedb)

        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "document": document,
                "ocr_results": ocr_results,
                "mapped_results": mapped_results,
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get document {document_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{document_id}/url", summary="Get document download URL", description="Generate a signed URL for downloading the document file.")
async def get_document_url(
    document_id: str,
    expires_in: int = Query(default=3600, ge=60, le=86400, description="URL expiration time in seconds"),
    supabasedb: AsyncClient = Depends(get_supabase_client)
):
    """
    Generate a signed URL to download the document's file from Supabase storage.
    """
    try:
        signed_url = await get_document_signed_url(document_id, supabasedb, expires_in)
        if not signed_url:
            raise HTTPException(status_code=404, detail="Document not found or file missing")
        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "document_id": document_id,
                "url": signed_url,
                "expires_in": expires_in,
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get URL for document {document_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
