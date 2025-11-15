import time
import traceback
from pipeline.config import Config
from pipeline.ssml_creator import article_to_double_ssml
from pipeline.azure_tts import synthesize_ssml_to_tempfile
# MODIFIED: We now import the new HLS uploader function
from pipeline.b2_uploader import upload_as_hls
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

    # MODIFIED: Create a unique prefix for all HLS segments.
    # We use time to ensure it's unique, e.g., "AI_News_169987...""
    clean_title = (title[:30].replace(" ", "_") or "news").strip()
    unique_prefix = f"{clean_title}_{time.time():.0f}"

    last_err = None
    for attempt in range(1, attempt_limit + 1):
        try:
            # 1. Create SSML
            ssml = article_to_double_ssml(description)
            if not ssml:
                raise RuntimeError("SSML generation returned empty string.")

            # 2. Synthesize SSML -> audio file (Azure)
            # We still need the original MP3 as a source for FFmpeg
            out_path = synthesize_ssml_to_tempfile(ssml, prefix=unique_prefix + "_")

            # 3. Upload to B2 as HLS
            # MODIFIED: This is the main change.
            # We are calling upload_as_hls instead of upload_file.
            # This function handles the FFmpeg conversion AND uploads all segments.

            # This will be the "folder" on B2, e.g., "audio/hls/AI_News_169987..."
            b2_hls_prefix = f"audio/hls/{unique_prefix}"

            uploaded_segments = upload_as_hls(
                local_mp3_path=out_path,
                b2_object_prefix=b2_hls_prefix
            )

            # MODIFIED: The return payload is updated to be HLS-aware.
            # The most important piece of info to save to your database is the
            # path to the master playlist (index.m3u8).
            master_playlist_path = f"{b2_hls_prefix}/index.m3u8"

            # 4. Return success payload including HLS info
            return {
                "success": True,
                "article_row": article_row,
                "audio": {
                    "original_local_path": out_path,
                    "hls_prefix": b2_hls_prefix,
                    "hls_playlist_object": master_playlist_path,
                    "segment_count": len(uploaded_segments)
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


# --- The test block remains the same, it will now test the full HLS pipeline ---
print("Worker module loaded.")

if __name__ == "__main__":
    print("Testing worker (HLS pipeline)...")
    test_article = {
        "title": "AI News: ChatGPT Upgraded",
        "description": "OpenAI releases a new model with improved reasoning."
    }
    result = process_single_article(test_article)
    print("\n--- Worker Test Result ---")
    import json

    print(json.dumps(result, indent=2))
    print("--------------------------")