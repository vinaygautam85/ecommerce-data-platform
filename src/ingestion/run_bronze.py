"""CLI entry point: `python -m src.ingestion.run_bronze`"""
from src.ingestion.bronze import run_bronze_ingestion

if __name__ == "__main__":
    run_bronze_ingestion()