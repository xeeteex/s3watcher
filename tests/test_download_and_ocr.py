"""
Test: Download a file from Supabase storage and send it to OCR.
Usage: uv run python -m tests.test_download_and_ocr
"""

import os
import sys
import json
import logging
import tempfile
from contextlib import contextmanager

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logger = logging.getLogger("test_download_and_ocr")
logging.basicConfig(level=logging.INFO)

from app.worker.worker import download_file_from_supabase, send_to_ocr, mapping_incoming_data

BUCKET = "sap-pdfs"
KEY = "ap_invoice.pdf"

@contextmanager
def temp_pdf(data: bytes):

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    try:
        tmp.write(data)
        tmp.close()
        yield tmp.name
    finally:
        os.unlink(tmp.name)

def main():
    # print(f"  Bucket:{BUCKET}")
    # print(f"  Key:{KEY}")

    # file_bytes = download_file_from_supabase(BUCKET, KEY)

    # with temp_pdf(file_bytes) as file_path:
    #     file_size = os.path.getsize(file_path)
    #     print(f"  Downloaded to: {file_path}  ({file_size} bytes)")

    #     print(f"\nSending to OCR ")
    #     ocr_result = send_to_ocr(file_path, filename=KEY)

    # print(json.dumps(ocr_result, indent=2, ensure_ascii=False))
    # print("Done.")

    # extraction_result = mapping_incoming_data(ocr_result)
    # print(extraction_result)

    ex_result = {'CardName': 'SARBOTTAM STEELS P LTD', 'CardCode': 'V0586', 'NumAtCard': 'SI-02527', 'GSTTranTyp': 'Cash', 'Series': None, 'DocNum': None, 'DocDate': '2026-03-09', 'TaxDate': '2024-09-09', 'U_NPMI': '2081-05-24', 'U_LC_NO': '', 'JrnlMemo': 'SERVICE AND MAINTENANCE', 'Address2': 'SUBIDHANAGAR,TINKUNE', 'Address': 'kha 14, Harati Bhawan, Putalisadak, Kathmandu,Nepal', 'DocumentLines': [{'ItemCode': '', 'Description': 'SERVICE AND MAINTENANCE', 'Quantity': 1.0, 'TaxCode': 'VAT13', 'UnitPrice': 442.48}]}

    cardname = ex_result.get("CardName")
    cardcode = ex_result.get("CardCode")
    docdate = ex_result.get("DocDate")
    doclines = ex_result.get("DocumentLines", [])
    ItemCode = doclines[0].get("ItemCode")
    Descripton = doclines[0].get("Description")
    Quantity = doclines[0].get("Quantity")
    TaxCode = doclines[0].get("TaxCode")
    UnitPrice = doclines[0].get("UnitPrice")

    print(cardname)
    print(cardcode)
    print(docdate)          
    print(ItemCode)
    print(Descripton)
    print(Quantity)
    print(TaxCode)

        


if __name__ == "__main__":
    main()
