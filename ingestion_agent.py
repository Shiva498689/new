
# import time
# import requests
# from datetime import datetime
# from edgar import set_identity, Company
# import psycopg2
# from psycopg2.extras import execute_values

# # CONFIGURATION & CREDENTIALS
# # ==========================================

# set_identity("Shiva Dubey 123shivadubey@gmail.com")

# self.MODAL_ENDPOINT_URL = "https://shivadubey--qwen-7b-summarizer-qwenmodel-summarize.modal.run"

# self.DB_CONFIG = {
#     "dbname": "financial_db",
#     "user": "postgres",
#     "password": "123shivadubey@gmail.com",
#     "host": "localhost",
#     "port": 5432
# }

# self.TARGET_ITEMS = ["Item 1", "Item 1A", "Item 7", "Item 2", "Item 3", "Item 8", "Item 9A"]
# # FINANCIAL DUE DILIGENCE SEGREGATION
# # ==========================================
# def categorize_to_due_diligence_segment(item_name: str, item_text: str, form_type: str) -> dict:
#     """Allocates filing pieces cleanly into 6 custom diligence buckets."""
#     segments = {
#         "Company & Operational Risks": "",
#         "Supply Chain & Infrastructure Health": "",
#         "Consumer Health & Market Share": "",
#         "Legal & Regulatory Risks": "",
#         "Financial Performance & Solvency": "",
#         "Corporate Governance & Structure": ""
#     }
    
#     if form_type == "DEF 14A":
#         segments["Corporate Governance & Structure"] = item_text
#         return segments

#     if item_name == "Item 1A":
#         segments["Company & Operational Risks"] = item_text
#     elif item_name == "Item 1":
#         segments["Supply Chain & Infrastructure Health"] = "Business Scope / Sourcing context: " + item_text[:len(item_text)//2]
#         segments["Consumer Health & Market Share"] = "Market Strategy context: " + item_text[len(item_text)//2:]
#     elif item_name == "Item 3":
#         segments["Legal & Regulatory Risks"] = item_text
#     elif item_name in ["Item 7", "Item 8"]:
#         segments["Financial Performance & Solvency"] = item_text
#     elif item_name in ["Item 2", "Item 9A"]:
#         segments["Company & Operational Risks"] = f"[{item_name} Context]: " + item_text
        
#     return segments
# # PROCESSING UTILITIES
# # ==========================================
# def chunk_text(text: str, max_chars: int = 4000, overlap: int = 400):
#     chunks = []
#     start = 0
#     while start < len(text):
#         end = start + max_chars
#         chunks.append(text[start:end])
#         start += (max_chars - overlap)
#     return chunks

# def call_qwen_summarizer(chunk_text: str) -> str:
#     try:
#         response = requests.post(self.MODAL_ENDPOINT_URL, json={"text": chunk_text}, timeout=600)
#         if response.status_code == 200:
#             return response.json().get("summary", "Summary processing error.")
#         return "[Error: Dynamic reduction skipped]"
#     except Exception as e:
#         print(f"Failed to communicate with Modal endpoint: {e}")
#         return "[Error: Pipeline Connection Failure]"

# def save_chunks_to_db(data_rows):
#     query = """
#         INSERT INTO financial_due_diligence_chunks 
#         (ticker, cik, filing_type, filing_date, segment_name, sec_item, original_chunk, summary_bullet_points)
#         VALUES %s;
#     """
#     conn = None
#     try:
#         conn = psycopg2.connect(**self.DB_CONFIG)
#         cur = conn.cursor()
#         execute_values(cur, query, data_rows)
#         conn.commit()
#         cur.close()
#     except Exception as e:
#         print(f"Database insertion crash error occurred: {e}")
#         if conn: conn.rollback()
#     finally:
#         if conn: conn.close()
# # REFACTORIZED EDGARTOOLS ORCHESTRATOR
# # ==========================================
# def run_ingestion_pipeline(ticker: str):
#     print(f"[*] Initializing edgartools tracking for: {ticker}")
#     company = Company(ticker)
#     cik = company.cik
#     current_year = datetime.now().year
    
#     # Fetch all targeted forms via edgartools API
#     filings_10k = company.get_filings(form="10-K")
#     filings_10q = company.get_filings(form="10-Q")
#     filings_def14a = company.get_filings(form="DEF 14A")
    
#     target_filings = []

#     # 1. Gather 2 Years of 10-K
# # 1. Gather 2 Years of 10-K
#     for f in filings_10k:
#         f_year = f.filing_date.year  # Directly access the year property
#         if (current_year - f_year) <= 2:
#             target_filings.append(f)
            
#     # 2. Gather 1 Year of 10-Q
#     for f in filings_10q:
#         f_year = f.filing_date.year  # Directly access the year property
#         if (current_year - f_year) <= 1:
#             target_filings.append(f)

#     # 3. Gather 1 Year of DEF 14A (Proxy)
#     for f in filings_def14a:
#         f_year = f.filing_date.year  # Directly access the year property
#         if (current_year - f_year) <= 1:
#             target_filings.append(f)

#     print(f"[*] Found {len(target_filings)} pristine filings via edgartools API.")

#     for f in target_filings:
#         print(f"[+] Processing {f.form} filed on {f.filing_date}")
#         rows_to_insert = []
        
#         # edgartools automatically fetches, handles rate limiting, and extracts structural objects
#         filing_obj = f.obj()
        
#         if f.form == "DEF 14A":
#             # For proxy statements, pull text directly
#             text_content = f.text()
#             mapped_segments = categorize_to_due_diligence_segment("Proxy", text_content, f.form)
            
#             for segment_name, segment_content in mapped_segments.items():
#                 if not segment_content: continue
#                 chunks = chunk_text(segment_content)
#                 for chunk in chunks:
#                     summary_bullets = call_qwen_summarizer(chunk)
#                     rows_to_insert.append((ticker, cik, f.form, f.filing_date, segment_name, "Proxy", chunk, summary_bullets))
        
#         else:
#             # For 10-K and 10-Q, use edgartools native section map extraction
#             for item in self.TARGET_ITEMS:
#                 try:
#                     # Extracts structural sections natively by name safely without Regex hacks!
#                     item_text = filing_obj.extract_section(item)
#                     if not item_text: continue
                    
#                     mapped_segments = categorize_to_due_diligence_segment(item, item_text, f.form)
#                     for segment_name, segment_content in mapped_segments.items():
#                         if not segment_content: continue
#                         chunks = chunk_text(segment_content)
#                         print(f"    -> {item} mapped to {segment_name} ({len(chunks)} chunks)")
                        
#                         for chunk in chunks:
#                             summary_bullets = call_qwen_summarizer(chunk)
#                             rows_to_insert.append((ticker, cik, f.form, f.filing_date, segment_name, item, chunk, summary_bullets))
#                 except Exception as ex:
#                     # Soft bypass if a specific sub-item isn't packaged in that quarter's filing 
#                     continue
                    
#         if rows_to_insert:
#             print(f"[**] Batch inserting {len(rows_to_insert)} records into local Database.")
#             save_chunks_to_db(rows_to_insert)
#         time.sleep(0.1) # Courteous breathing room for your Modal endpoint tasks

# if __name__ == "__main__":
#     run_ingestion_pipeline("AAPL")


import os
import time
import requests
from datetime import datetime
from edgar import set_identity, Company
import psycopg2
from psycopg2.extras import execute_values

# CONFIGURATION & CREDENTIALS
# ==========================================

class IngestionAgent():
    def __init__(self , ticker):
        self.MODAL_ENDPOINT_URL = os.getenv("MODAL_ENDPOINT_URL", "")
        set_identity(os.getenv("EDGAR_IDENTITY", "Dev User dev@example.com"))

        self.DB_CONFIG = {
            "dbname": os.getenv("POSTGRES_DB", "financial_db"),
            "user": os.getenv("POSTGRES_USER", "postgres"),
            "password": os.getenv("POSTGRES_PASSWORD", ""),
            "host": os.getenv("POSTGRES_HOST", "localhost"),
            "port": int(os.getenv("POSTGRES_PORT", "5432")),
        }

        self.TARGET_ITEMS = ["Item 1", "Item 1A", "Item 7", "Item 2", "Item 3", "Item 8", "Item 9A"]
    # FINANCIAL DUE DILIGENCE SEGREGATION
    # ==========================================
    def categorize_to_due_diligence_segment(self, item_name: str, item_text: str, form_type: str) -> dict:
        """Allocates filing pieces cleanly into 6 custom diligence buckets."""
        segments = {
            "Company & Operational Risks": "",
            "Supply Chain & Infrastructure Health": "",
            "Consumer Health & Market Share": "",
            "Legal & Regulatory Risks": "",
            "Financial Performance & Solvency": "",
            "Corporate Governance & Structure": ""
        }
        
        if form_type == "DEF 14A":
            segments["Corporate Governance & Structure"] = item_text
            return segments

        if item_name == "Item 1A":
            segments["Company & Operational Risks"] = item_text
        elif item_name == "Item 1":
            segments["Supply Chain & Infrastructure Health"] = "Business Scope / Sourcing context: " + item_text[:len(item_text)//2]
            segments["Consumer Health & Market Share"] = "Market Strategy context: " + item_text[len(item_text)//2:]
        elif item_name == "Item 3":
            segments["Legal & Regulatory Risks"] = item_text
        elif item_name in ["Item 7", "Item 8"]:
            segments["Financial Performance & Solvency"] = item_text
        elif item_name in ["Item 2", "Item 9A"]:
            segments["Company & Operational Risks"] = f"[{item_name} Context]: " + item_text
            
        return segments
    # PROCESSING UTILITIES
    # ==========================================
    def chunk_text(self, text: str, max_chars: int = 4000, overlap: int = 400):
        chunks = []
        start = 0
        while start < len(text):
            end = start + max_chars
            chunks.append(text[start:end])
            start += (max_chars - overlap)
        return chunks

    def call_qwen_summarizer(self, chunk_text: str) -> str:
        try:
            response = requests.post(self.MODAL_ENDPOINT_URL, json={"text": chunk_text}, timeout=3.0)
            if response.status_code == 200:
                return response.json().get("summary", "Summary processing error.")
            return "[Error: Dynamic reduction skipped]"
        except Exception as e:
            print(f"Failed to communicate with Modal endpoint ({e}). Using local baseline extraction.")
            return f"[Local Extraction Baseline]: Summary generated from block: {chunk_text[:120]}..."

    def save_chunks_to_db(self, data_rows):
        query = """
            INSERT INTO financial_due_diligence_chunks 
            (ticker, cik, filing_type, filing_date, segment_name, sec_item, original_chunk, summary_bullet_points)
            VALUES %s;
        """
        conn = None
        try:
            conn = psycopg2.connect(**self.DB_CONFIG)
            cur = conn.cursor()
            execute_values(cur, query, data_rows)
            conn.commit()
            cur.close()
        except Exception as e:
            print(f"Database insertion crash error occurred: {e}")
            if conn: conn.rollback()
        finally:
            if conn: conn.close()
    # REFACTORIZED EDGARTOOLS ORCHESTRATOR
    # ==========================================
def run_ingestion_pipeline( ticker: str):
        obj = IngestionAgent(ticker)
        print(f"[*] Initializing edgartools tracking for: {ticker}")
        company = Company(ticker)
        cik = company.cik
        current_year = datetime.now().year
        
        # Fetch all targeted forms via edgartools API
        filings_10k = company.get_filings(form="10-K")
        filings_10q = company.get_filings(form="10-Q")
        filings_def14a = company.get_filings(form="DEF 14A")
        
        target_filings = []

        # 1. Gather 2 Years of 10-K
    # 1. Gather 2 Years of 10-K
        for f in filings_10k:
            f_year = f.filing_date.year  # Directly access the year property
            if (current_year - f_year) <= 2:
                target_filings.append(f)
                
        # 2. Gather 1 Year of 10-Q
        for f in filings_10q:
            f_year = f.filing_date.year  # Directly access the year property
            if (current_year - f_year) <= 1:
                target_filings.append(f)

        # 3. Gather 1 Year of DEF 14A (Proxy)
        for f in filings_def14a:
            f_year = f.filing_date.year  # Directly access the year property
            if (current_year - f_year) <= 1:
                target_filings.append(f)

        print(f"[*] Found {len(target_filings)} pristine filings via edgartools API.")

        for f in target_filings:
            print(f"[+] Processing {f.form} filed on {f.filing_date}")
            rows_to_insert = []
            
            # edgartools automatically fetches, handles rate limiting, and extracts structural objects
            filing_obj = f.obj()
            
            if f.form == "DEF 14A":
                # For proxy statements, pull text directly
                text_content = f.text()
                mapped_segments = obj.categorize_to_due_diligence_segment("Proxy", text_content, f.form)
                
                for segment_name, segment_content in mapped_segments.items():
                    if not segment_content: continue
                    chunks = obj.chunk_text(segment_content)
                    for chunk in chunks:
                        summary_bullets = obj.call_qwen_summarizer(chunk)
                        rows_to_insert.append((ticker, cik, f.form, f.filing_date, segment_name, "Proxy", chunk, summary_bullets))
            
            else:
                # For 10-K and 10-Q, use edgartools native section map extraction
                for item in obj.TARGET_ITEMS:
                    try:
                        # Extracts structural sections natively by name safely without Regex hacks!
                        item_text = filing_obj.extract_section(item)
                        if not item_text: continue
                        
                        mapped_segments = obj.categorize_to_due_diligence_segment(item, item_text, f.form)
                        for segment_name, segment_content in mapped_segments.items():
                            if not segment_content: continue
                            chunks = obj.chunk_text(segment_content)
                            print(f"    -> {item} mapped to {segment_name} ({len(chunks)} chunks)")
                            
                            for chunk in chunks:
                                summary_bullets = obj.call_qwen_summarizer(chunk)
                                rows_to_insert.append((ticker, cik, f.form, f.filing_date, segment_name, item, chunk, summary_bullets))
                    except Exception as ex:
                        # Soft bypass if a specific sub-item isn't packaged in that quarter's filing 
                        continue
                        
            if rows_to_insert:
                print(f"[**] Batch inserting {len(rows_to_insert)} records into local Database.")
                obj.save_chunks_to_db(rows_to_insert)
            time.sleep(0.1) # Courteous breathing room for your Modal endpoint tasks

# if __name__ == "__main__":
#         run_ingestion_pipeline("AAPL")