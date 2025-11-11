#!/bin/bash
set -e
# create virtualenv and install
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt

# copy .env.example to .env and fill with real values (do NOT commit)
cp .env.example .env
echo "Edit .env with your keys, then press Enter to continue..."
read

python run_pipeline.py --csv tests/sample_articles.csv
