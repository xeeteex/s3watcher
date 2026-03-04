"""
Test: Download a file from Supabase storage and send it to OCR.
Usage: uv run python -m tests.test_download_and_ocr
"""

import os
import sys
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.worker.worker import download_from_supabase, send_to_ocr

BUCKET = "sap-pdfs"
KEY = "Image0031.PDF"


def main():
    print(f"  Bucket:{BUCKET}")
    print(f"  Key:{KEY}")

    file_path = download_from_supabase(BUCKET, KEY)
    file_size = os.path.getsize(file_path)
    print(f"{file_path}  {file_size}")

    print(f"\nSending to OCR ")
    result = send_to_ocr(file_path)
    print(json.dumps(result, indent=2, ensure_ascii=False))
    os.unlink(file_path)



if __name__ == "__main__":
    main()
