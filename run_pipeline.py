# run_pipeline.py
import argparse
from pipeline.orchestrator import run_pipeline_from_csv
import json

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=False, default="tests/sample_articles.csv",
                        help="Path to input CSV of articles")
    args = parser.parse_args()

    summary = run_pipeline_from_csv(args.csv)
    print("\nPipeline Summary:")
    print(json.dumps(summary, indent=2))

if __name__ == "__main__":
    main()
