# pipeline/config.py
import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
    AZURE_SPEECH_KEY = os.getenv("AZURE_SPEECH_KEY")
    AZURE_SPEECH_REGION = os.getenv("AZURE_SPEECH_REGION", "centralindia")
    B2_KEY_ID = os.getenv("B2_KEY_ID")
    B2_APP_KEY = os.getenv("B2_APP_KEY")
    B2_BUCKET_NAME = os.getenv("B2_BUCKET_NAME", "Newspods")
    COCKROACHDB_CONN_STRING = os.getenv("COCKROACHDB_CONN_STRING")
    MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
    RETRY_BACKOFF_BASE = float(os.getenv("RETRY_BACKOFF_BASE", "2"))
    MAX_WORKERS = int(os.getenv("MAX_WORKERS", "4"))
    RATE_LIMIT_CONCURRENCY = int(os.getenv("RATE_LIMIT_CONCURRENCY", "2"))
    OUTPUT_AUDIO_DIR = os.getenv("OUTPUT_AUDIO_DIR", "./output_audio")
