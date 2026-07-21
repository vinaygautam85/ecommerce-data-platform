"""
End-to-end pipeline: bronze -> silver -> gold dims -> gold facts -> gold views.
Run with: python -m src.run_pipeline
"""
import sys
import time

from loguru import logger

from src.ingestion.bronze import run_bronze_ingestion
from src.transformations.silver import run_silver_transformations
from src.transformations.gold_dims import run_gold_dimensions
from src.transformations.gold_facts import run_gold_facts
from src.transformations.gold_views import build_gold_views

STAGES = [
    ("Bronze ingestion",  run_bronze_ingestion),
    ("Silver transforms", run_silver_transformations),
    ("Gold dimensions",   run_gold_dimensions),
    ("Gold facts",        run_gold_facts),
    ("Gold views",        build_gold_views),
]


def run_pipeline(fail_fast: bool = True) -> int:
    overall = time.time()
    results = []
    for name, fn in STAGES:
        logger.info(f"{'#' * 12} STAGE: {name} {'#' * 12}")
        start = time.time()
        try:
            run_id = fn()
            results.append((name, "SUCCESS", run_id, time.time() - start))
            logger.success(f"Stage '{name}' OK in {time.time() - start:.1f}s")
        except Exception as e:
            results.append((name, "FAILED", str(e)[:200], time.time() - start))
            logger.exception(f"Stage '{name}' FAILED")
            if fail_fast:
                logger.error("fail_fast=True -> aborting remaining stages.")
                break

    logger.info("=" * 60)
    logger.info("PIPELINE SUMMARY")
    for name, status, info, elapsed in results:
        logger.info(f"  {status:<8} {name:<20} {elapsed:6.1f}s  {info}")
    logger.info(f"  TOTAL ELAPSED: {time.time() - overall:.1f}s")
    logger.info("=" * 60)

    return 1 if any(r[1] == "FAILED" for r in results) else 0


if __name__ == "__main__":
    sys.exit(run_pipeline())