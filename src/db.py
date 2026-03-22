"""
Database connection and schema initialization.
All four src/ modules import get_connection() from here.
Schema is created once at startup via init_schema().
"""

import logging
import sqlite3
from pathlib import Path

logger = logging.getLogger(__name__)

# Video status state machine:
#
#   FETCHED → TRANSCRIBED → ANALYZED → DELIVERED → ARCHIVED
#      ↓            ↓           ↓
#  FETCH_ERR  TRANSCRIPT_ERR  ANALYSIS_ERR
#
# run.py queries for videos NOT in terminal states at startup,
# so interrupted runs resume automatically without reprocessing.

_SCHEMA = """
CREATE TABLE IF NOT EXISTS videos (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    video_id            TEXT    UNIQUE NOT NULL,
    channel_id          TEXT    NOT NULL,
    channel_name        TEXT    NOT NULL,
    title               TEXT    NOT NULL,
    description         TEXT,
    published_at        TEXT,
    duration_seconds    INTEGER DEFAULT 0,
    source_type         TEXT    DEFAULT 'youtube',

    -- Transcript fields (set by ingestor)
    transcript          TEXT,
    transcript_quality  TEXT    CHECK(transcript_quality IN ('GOOD', 'POOR', 'MISSING')),
    transcript_chunks   INTEGER DEFAULT 1,

    -- Analysis fields (set by analyzer)
    relevance_score     REAL,
    relevance_tag       TEXT    CHECK(relevance_tag IN ('CORE', 'PERIPHERAL', 'FLAGGED')),
    domain_tags         TEXT,   -- JSON array, e.g. '["AI / LLMs / Agents", "Startups & Founding"]'
    brief               TEXT,   -- Full markdown brief

    -- Delivery / archive fields
    notion_url          TEXT,
    digest_id           INTEGER,

    -- Error tracking
    error_message       TEXT,

    -- Status (the state machine above)
    status              TEXT    NOT NULL DEFAULT 'FETCHED'
                        CHECK(status IN (
                            'FETCHED', 'FETCH_ERR',
                            'TRANSCRIBED', 'TRANSCRIPT_ERR',
                            'ANALYZED', 'ANALYSIS_ERR',
                            'DELIVERED', 'ARCHIVED'
                        )),

    created_at          TEXT    DEFAULT CURRENT_TIMESTAMP,
    updated_at          TEXT    DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS channels (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_id  TEXT    UNIQUE NOT NULL,
    name        TEXT    NOT NULL,
    active      INTEGER DEFAULT 1,
    source_type TEXT    DEFAULT 'youtube',   -- extensibility hook for V2 (podcast, substack)
    created_at  TEXT    DEFAULT CURRENT_TIMESTAMP
);

-- Compounding layer: connections between briefs
CREATE TABLE IF NOT EXISTS connections (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    video_id            TEXT    NOT NULL REFERENCES videos(video_id),
    connected_video_id  TEXT    NOT NULL REFERENCES videos(video_id),
    connection_type     TEXT,   -- 'confirms', 'contradicts', 'extends'
    summary             TEXT,
    created_at          TEXT    DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS digests (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    type             TEXT    NOT NULL CHECK(type IN ('daily', 'weekly')),
    date_range_start TEXT,
    date_range_end   TEXT,
    email_content    TEXT,
    sent_at          TEXT,
    created_at       TEXT    DEFAULT CURRENT_TIMESTAMP
);

-- One row per pipeline run — used for cost tracking and resumability
CREATE TABLE IF NOT EXISTS run_log (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at       TEXT    NOT NULL,
    completed_at     TEXT,
    status           TEXT    CHECK(status IN ('running', 'completed', 'failed')),
    videos_found     INTEGER DEFAULT 0,
    videos_new       INTEGER DEFAULT 0,
    videos_processed INTEGER DEFAULT 0,
    -- Cost tracking (populated by analyzer using SDK usage fields)
    input_tokens     INTEGER DEFAULT 0,
    output_tokens    INTEGER DEFAULT 0,
    cost_usd         REAL    DEFAULT 0.0,
    errors           TEXT,   -- JSON array of error strings
    created_at       TEXT    DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for the access patterns we use most
CREATE INDEX IF NOT EXISTS idx_videos_status      ON videos(status);
CREATE INDEX IF NOT EXISTS idx_videos_channel     ON videos(channel_id);
CREATE INDEX IF NOT EXISTS idx_videos_published   ON videos(published_at);
CREATE INDEX IF NOT EXISTS idx_videos_domain_tags ON videos(domain_tags);
CREATE INDEX IF NOT EXISTS idx_connections_video  ON connections(video_id);
"""


def get_connection(db_path: str) -> sqlite3.Connection:
    """Return a SQLite connection. Creates the database file if needed."""
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")   # safe for concurrent reads
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    """Create all tables and indexes. Safe to call on an existing database."""
    conn.executescript(_SCHEMA)
    conn.commit()
    logger.info("Database schema ready")
