# 📰 Newspods Pipeline

### 🎧 Turn text news into fully automated AI-powered audio podcasts

The **Newspods Pipeline** takes a CSV of article data (title, description, source, etc.) and automatically:
1. Generates a conversational **SSML** script using Google Gemini.
2. Synthesizes it into lifelike speech with **Azure Text-to-Speech**.
3. Uploads the audio to **Backblaze B2 cloud storage**.
4. Inserts the processed article metadata (title, source, audio key, etc.) into a **CockroachDB** database.
5. Retries failed articles safely before skipping.

---
## 📁 Project Layout
newspods_pipeline/
├─ README.md
├─ .env.example
├─ requirements.txt
├─ run_pipeline.py # Entry point to run pipeline on a CSV
├─ pipeline/
│ ├─ init.py
│ ├─ config.py # Loads and manages environment configuration
│ ├─ ssml_creator.py # Uses Gemini to generate SSML podcast scripts
│ ├─ azure_tts.py # Converts SSML to audio (MP3) via Azure TTS
│ ├─ b2_uploader.py # Uploads MP3 files to Backblaze B2
│ ├─ db_pusher.py # Inserts records into CockroachDB
│ ├─ worker.py # Handles per-article pipeline with retry logic
│ └─ orchestrator.py # Manages concurrency and final DB push
└─ tests/
├─ sample_articles.csv # Example input file
└─ run_test.sh # Local test runner

## 🚀 Features

✅ **Automated Pipeline** — Converts text news into natural-sounding speech podcasts  
✅ **AI-Generated SSML** — Uses Gemini (`gemini-2.5-flash`) to create conversational dialogue  
✅ **Multi-Voice Narration** — Alternates between `en-IN-NeerjaNeural` and `en-IN-PrabhatNeural` voices  
✅ **Cloud Audio Storage** — Saves MP3 files to Backblaze B2  
✅ **Database Sync** — Inserts metadata and audio keys into CockroachDB  
✅ **Retries & Error Handling** — Retries failed tasks (Gemini, Azure, DB) with exponential backoff  
✅ **Configurable** — All parameters managed via `.env`  

---

## ⚙️ Setup Instructions

### 1️⃣ Clone the repository

```bash
git clone https://github.com/<your-username>/newspods_pipeline.git
cd newspods_pipeline

## 2️⃣ Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate        # On macOS/Linux
.venv\Scripts\activate           # On Windows

## 3️⃣ Install dependencies
pip install -r requirements.txt

## 4️⃣ Configure environment variables
cp .env.example .env

## Then open .env and fill in:
# Google Gemini
GOOGLE_API_KEY=your_google_api_key_here

# Azure Speech
AZURE_SPEECH_KEY=your_azure_speech_key_here
AZURE_SPEECH_REGION=centralindia

# Backblaze B2
B2_KEY_ID=your_b2_key_id_here
B2_APP_KEY=your_b2_app_key_here
B2_BUCKET_NAME=Newspods

# CockroachDB
COCKROACHDB_CONN_STRING=postgresql+psycopg2://<user>:<password>@<host>:<port>/<db>?sslmode=require

# Pipeline Config
MAX_RETRIES=3
RETRY_BACKOFF_BASE=2
MAX_WORKERS=4
RATE_LIMIT_CONCURRENCY=2
OUTPUT_AUDIO_DIR=./output_audio

## 🧩 Example Input CSV
title,description,news_source,topic,published_date
"Small town festival","A description about a small town festival with details and quotes.","Daily Gazette","culture|local","2025-11-04"
"Politics today","A short report about political events.","Capitol News","politics","2025-11-04"
"Tech release","Company X announced a new gadget that ...","TechWire","technology","2025-11-04"

## ▶️ Running the Pipeline
python run_pipeline.py --csv tests/sample_articles.csv
