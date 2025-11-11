# pipeline/orchestrator.py
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from pipeline.config import Config
from pipeline.worker import process_single_article
from pipeline.db_pusher import push_articles_to_db
import threading
import time

sem = threading.Semaphore(Config.RATE_LIMIT_CONCURRENCY)

def _worker_wrapper(row_dict):
    with sem:
        return process_single_article(row_dict, attempt_limit=Config.MAX_RETRIES)

def run_pipeline_from_csv(csv_path: str, chunk_size: int = 50):
    """
    csv must have columns like: title, description (or content), source (or news_source), topic (optional), published_date (optional)
    Returns: dict with lists of success/failure and DB insertion ids
    """
    df = pd.read_csv(csv_path)
    # Normalize expected column names
    df.rename(columns={'content': 'description', 'source': 'news_source'}, inplace=True)
    records = df.to_dict('records')
    successes = []
    failures = []
    with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as ex:
        futures = {ex.submit(_worker_wrapper, rec): rec for rec in records}
        for fut in as_completed(futures):
            res = fut.result()
            if res.get("success"):
                # append audio metadata and content into a record to later insert into DB.
                rec = res["article_row"].copy()
                # attach audio URL / key (we store object_name)
                rec["audio_url"] = res["audio"]["object_name"]
                successes.append(rec)
            else:
                failures.append({"record": res["article_row"], "error": res.get("error")})

    # Push successes to DB as a batch DataFrame
    success_df = pd.DataFrame(successes)
    inserted_ids = []
    if not success_df.empty:
        inserted_ids = push_articles_to_db(success_df)

    return {
        "num_total": len(records),
        "num_success_audio": len(successes),
        "num_failures": len(failures),
        "failures": failures,
        "inserted_article_ids": inserted_ids
    }

print("Orchestrator module loaded.")