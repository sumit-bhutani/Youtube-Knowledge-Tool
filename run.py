#!/usr/bin/env python3
"""
The Briefing — Personal YouTube Knowledge Compounding System

Entry point. Run daily via launchd (Mac) or GitHub Actions (Phase 2).

Usage:
    python run.py             # full pipeline run
    python run.py --ingest    # ingest only (Phase 1)
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import yaml
from dotenv import load_dotenv

from src.db import get_connection, init_schema
from src.ingestor import Ingestor


def load_config(path: str = "config.yaml") -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def setup_logging(config: dict) -> None:
    log_dir = Path(config.get("logging", {}).get("dir", "logs"))
    log_dir.mkdir(parents=True, exist_ok=True)
    level_name = config.get("logging", {}).get("level", "INFO")
    level = getattr(logging, level_name, logging.INFO)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"run_{timestamp}.log"

    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout),
        ],
    )


def start_run_log(db, started_at: str) -> int:
    cursor = db.cursor()
    cursor.execute(
        "INSERT INTO run_log (started_at, status) VALUES (?, 'running')",
        (started_at,),
    )
    db.commit()
    return cursor.lastrowid


def finish_run_log(db, run_id: int, summary: dict, status: str = "completed") -> None:
    db.execute(
        """
        UPDATE run_log
        SET completed_at     = CURRENT_TIMESTAMP,
            status           = ?,
            videos_found     = ?,
            videos_new       = ?,
            errors           = ?
        WHERE id = ?
        """,
        (
            status,
            summary.get("videos_found", 0),
            summary.get("videos_new", 0),
            json.dumps(summary.get("errors", [])),
            run_id,
        ),
    )
    db.commit()


def main() -> None:
    parser = argparse.ArgumentParser(description="The Briefing pipeline")
    parser.add_argument("--ingest", action="store_true", help="Run ingest step only")
    args = parser.parse_args()

    load_dotenv()
    config = load_config()
    setup_logging(config)
    logger = logging.getLogger(__name__)

    started_at = datetime.now(timezone.utc).isoformat()
    logger.info("=" * 60)
    logger.info("The Briefing — pipeline starting")
    logger.info(f"Started at: {started_at}")
    logger.info("=" * 60)

    db_path = config.get("database", {}).get("path", "data/briefing.db")
    db = get_connection(db_path)
    init_schema(db)

    run_id = start_run_log(db, started_at)
    all_errors: list[str] = []

    try:
        # ── Step 1: Ingest ──────────────────────────────────────────────
        youtube_api_key = os.environ.get("YOUTUBE_API_KEY")
        if not youtube_api_key:
            raise EnvironmentError(
                "YOUTUBE_API_KEY not set. Add it to .env and try again."
            )

        ingestor = Ingestor(config, db, youtube_api_key)
        ingest_summary = ingestor.run()
        all_errors.extend(ingest_summary.errors)

        logger.info(
            f"Ingest: {ingest_summary.videos_new} new videos, "
            f"{ingest_summary.transcripts_fetched} transcribed"
        )

        if args.ingest:
            logger.info("--ingest flag set, stopping after ingestion.")
            finish_run_log(
                db, run_id,
                {
                    "videos_found": ingest_summary.videos_found,
                    "videos_new": ingest_summary.videos_new,
                    "errors": all_errors,
                },
            )
            db.close()
            logger.info("Done.")
            return

        # ── Step 2: Analyze (Phase 1 next step) ────────────────────────
        logger.info("Analyzer: not yet implemented — skipping")

        # ── Step 3: Deliver (Phase 1 next step) ────────────────────────
        logger.info("Delivery: not yet implemented — skipping")

        # ── Step 4: Archive to Notion (Phase 1 next step) ──────────────
        logger.info("Notion: not yet implemented — skipping")

        finish_run_log(
            db, run_id,
            {
                "videos_found": ingest_summary.videos_found,
                "videos_new": ingest_summary.videos_new,
                "errors": all_errors,
            },
        )

    except Exception as exc:
        logger.exception(f"Pipeline failed: {exc}")
        finish_run_log(db, run_id, {"errors": [str(exc)]}, status="failed")
        db.close()
        sys.exit(1)

    db.close()
    logger.info("=" * 60)
    logger.info("The Briefing — pipeline complete")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
