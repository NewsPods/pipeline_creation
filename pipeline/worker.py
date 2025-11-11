# pipeline/worker.py
import time
import traceback
from pipeline.config import Config
from pipeline.ssml_creator import article_to_double_ssml
from pipeline.azure_tts import synthesize_ssml_to_tempfile
from pipeline.b2_uploader import upload_file
from retrying import retry

def exponential_backoff_sleep(attempt, base_seconds):
    # simple exponential sleep
    time.sleep(base_seconds ** attempt)

def process_single_article(article_row: dict, attempt_limit: int = None):
    """
    article_row: dict with keys e.g. title, description, source, topic, published_date (optional)
    returns: dict with success status and audio info if success
    """
    attempt_limit = attempt_limit or Config.MAX_RETRIES
    base = Config.RETRY_BACKOFF_BASE

    title = article_row.get("title", "untitled")
    description = article_row.get("description") or article_row.get("content") or ""
    filename_prefix = (title[:30].replace(" ", "_") or "news").strip()

    last_err = None
    for attempt in range(1, attempt_limit+1):
        try:
            # 1. Create SSML
            ssml = article_to_double_ssml(description)
            if not ssml:
                raise RuntimeError("SSML generation returned empty string.")

            # 2. Synthesize SSML -> audio file (Azure)
            out_path = synthesize_ssml_to_tempfile(ssml, prefix=filename_prefix + "_")
            # 3. Upload to B2
            object_name = f"audio/{filename_prefix}_{time.time():.0f}.mp3"
            upload_info = upload_file(out_path, object_name)
            # 4. Return success payload including audio info
            return {
                "success": True,
                "article_row": article_row,
                "audio": {
                    "local_path": out_path,
                    "object_name": upload_info["object_name"],
                    "file_id": upload_info["file_id"],
                    # B2 doesn't directly provide a simple public URL without bucket settings; store object_name
                }
            }
        except Exception as e:
            last_err = e
            # log
            print(f"[Attempt {attempt}/{attempt_limit}] Error processing article '{title}': {e}")
            traceback.print_exc()
            if attempt < attempt_limit:
                backoff = base ** attempt
                print(f"Retrying in {backoff:.1f}s...")
                time.sleep(backoff)
            else:
                print(f"Max retries reached for article '{title}'. Skipping.")
    return {"success": False, "article_row": article_row, "error": str(last_err)}

print("Worker module loaded.")

if __name__ == "__main__":
    print("Testing worker...")
    test_article = {
        "title": "AI News: ChatGPT Upgraded",
        "description": "OpenAI releases a new model with improved reasoning."
    }
    result = process_single_article(test_article)
    print("Result:", result)
