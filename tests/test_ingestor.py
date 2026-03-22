"""
Unit tests for ingestor.py — pure functions only.
All external APIs (YouTube, youtube-transcript-api) are mocked at the boundary.

Run with: pytest tests/
"""

import sqlite3
from unittest.mock import MagicMock, patch

import pytest

from src.db import get_connection, init_schema
from src.ingestor import (
    Ingestor,
    _chunk_transcript,
    _format_chunks,
    _get_best_transcript,
    _parse_iso8601_duration,
)


# ── Fixtures ────────────────────────────────────────────────────────────── #


@pytest.fixture
def db():
    """In-memory SQLite database with full schema."""
    conn = get_connection(":memory:")
    init_schema(conn)
    yield conn
    conn.close()


@pytest.fixture
def config():
    return {
        "channels": [
            {"id": "UCabc123", "name": "Test Channel"},
        ],
        "youtube": {"lookback_hours": 24},
        "database": {"path": ":memory:"},
    }


@pytest.fixture
def ingestor(config, db):
    youtube_mock = MagicMock()
    return Ingestor(config, db, youtube_api_key="fake_key"), youtube_mock, db


# ── _parse_iso8601_duration ──────────────────────────────────────────────── #


class TestParseIso8601Duration:
    def test_full_duration(self):
        assert _parse_iso8601_duration("PT1H30M45S") == 5445

    def test_hours_only(self):
        assert _parse_iso8601_duration("PT3H") == 10800

    def test_minutes_only(self):
        assert _parse_iso8601_duration("PT45M") == 2700

    def test_seconds_only(self):
        assert _parse_iso8601_duration("PT30S") == 30

    def test_days_and_time(self):
        assert _parse_iso8601_duration("P1DT2H") == 86400 + 7200

    def test_zero(self):
        assert _parse_iso8601_duration("PT0S") == 0

    def test_empty_string(self):
        assert _parse_iso8601_duration("") == 0

    def test_malformed(self):
        assert _parse_iso8601_duration("not-a-duration") == 0


# ── _chunk_transcript ────────────────────────────────────────────────────── #


def _make_segments(count: int, seconds_per_segment: int = 60) -> list[dict]:
    """Helper: build a fake transcript with `count` segments."""
    return [
        {"text": f"Word {i}.", "start": i * seconds_per_segment, "duration": seconds_per_segment}
        for i in range(count)
    ]


class TestChunkTranscript:
    def test_short_video_single_chunk(self):
        """Video under 30 min → one chunk."""
        segments = _make_segments(20, seconds_per_segment=60)  # 20 min
        chunks = _chunk_transcript(segments, "vid123")
        assert len(chunks) == 1
        assert chunks[0]["chunk_index"] == 0
        assert chunks[0]["start_seconds"] == 0

    def test_long_video_multiple_chunks(self):
        """Video over 60 min → at least 2 chunks."""
        segments = _make_segments(70, seconds_per_segment=60)  # 70 min
        chunks = _chunk_transcript(segments, "vid123")
        assert len(chunks) >= 2

    def test_chunk_boundary_at_30_min(self):
        """New chunk starts at/after 1800 seconds."""
        segments = _make_segments(40, seconds_per_segment=60)  # 40 min
        chunks = _chunk_transcript(segments, "vid123")
        assert len(chunks) == 2
        # Second chunk must start at or after 1800s
        assert chunks[1]["start_seconds"] >= 1800

    def test_chunk_indices_sequential(self):
        """Chunk indices must be 0, 1, 2 ..."""
        segments = _make_segments(100, seconds_per_segment=60)  # 100 min
        chunks = _chunk_transcript(segments, "vid123")
        for i, chunk in enumerate(chunks):
            assert chunk["chunk_index"] == i

    def test_no_timestamps_returns_single_chunk(self):
        """Transcript without timestamps → single chunk, no crash."""
        segments = [{"text": "Hello world."}, {"text": "No timestamps here."}]
        chunks = _chunk_transcript(segments, "vid_no_ts")
        assert len(chunks) == 1
        assert "Hello world." in chunks[0]["text"]

    def test_empty_segments_returns_empty(self):
        assert _chunk_transcript([], "vid_empty") == []

    def test_chunks_contain_all_text(self):
        """All text from segments must appear in some chunk."""
        segments = _make_segments(50, seconds_per_segment=60)
        chunks = _chunk_transcript(segments, "vid123")
        all_chunk_text = " ".join(c["text"] for c in chunks)
        for i in range(50):
            assert f"Word {i}." in all_chunk_text

    def test_skips_empty_text_segments(self):
        """Segments with empty text are silently skipped."""
        segments = [
            {"text": "Real text.", "start": 0, "duration": 10},
            {"text": "", "start": 10, "duration": 10},
            {"text": "  ", "start": 20, "duration": 10},
            {"text": "More text.", "start": 30, "duration": 10},
        ]
        chunks = _chunk_transcript(segments, "vid123")
        assert len(chunks) == 1
        assert "Real text." in chunks[0]["text"]
        assert "More text." in chunks[0]["text"]


# ── _format_chunks ───────────────────────────────────────────────────────── #


class TestFormatChunks:
    def test_single_chunk_no_header(self):
        """Single chunk is returned as plain text — no chunk header."""
        chunks = [{"chunk_index": 0, "start_seconds": 0, "end_seconds": 600, "text": "Hello."}]
        result = _format_chunks(chunks)
        assert result == "Hello."
        assert "[CHUNK" not in result

    def test_multi_chunk_has_headers(self):
        chunks = [
            {"chunk_index": 0, "start_seconds": 0, "end_seconds": 1800, "text": "Part one."},
            {"chunk_index": 1, "start_seconds": 1800, "end_seconds": 3600, "text": "Part two."},
        ]
        result = _format_chunks(chunks)
        assert "[CHUNK 1:" in result
        assert "[CHUNK 2:" in result
        assert "Part one." in result
        assert "Part two." in result

    def test_empty_chunks(self):
        assert _format_chunks([]) == ""

    def test_chunk_with_no_end_seconds(self):
        """end_seconds=None (no-timestamp transcript) must not crash."""
        chunks = [{"chunk_index": 0, "start_seconds": 0, "end_seconds": None, "text": "Text."}]
        result = _format_chunks(chunks)
        assert result == "Text."


# ── Idempotency ──────────────────────────────────────────────────────────── #


class TestIdempotency:
    def test_duplicate_video_id_ignored(self, db, config):
        """Inserting the same video_id twice must not create a duplicate row."""
        ingestor = Ingestor(config, db, youtube_api_key="fake")
        video = {
            "video_id": "abc123",
            "channel_id": "UCabc",
            "channel_name": "Test",
            "title": "Test Video",
            "description": "",
            "published_at": "2026-03-22T00:00:00Z",
            "duration_seconds": 3600,
            "source_type": "youtube",
        }

        first = ingestor._save_video(video)
        second = ingestor._save_video(video)

        assert first is True
        assert second is False

        cursor = db.cursor()
        cursor.execute("SELECT COUNT(*) FROM videos WHERE video_id = 'abc123'")
        count = cursor.fetchone()[0]
        assert count == 1


# ── Quality gate ─────────────────────────────────────────────────────────── #


class TestQualityGate:
    def test_missing_transcript_sets_status(self, db, config):
        """If transcripts are disabled, video status → TRANSCRIPT_ERR, quality → MISSING."""
        ingestor = Ingestor(config, db, youtube_api_key="fake")

        # Insert a video first
        ingestor._save_video(
            {
                "video_id": "vid_no_transcript",
                "channel_id": "UCabc",
                "channel_name": "Test",
                "title": "No Transcript Video",
                "description": "",
                "published_at": "2026-03-22T00:00:00Z",
                "duration_seconds": 1800,
                "source_type": "youtube",
            }
        )

        from youtube_transcript_api._errors import TranscriptsDisabled

        # Mock the instance's transcript_api directly (new API: instantiated, not static)
        ingestor.transcript_api = MagicMock()
        ingestor.transcript_api.list.side_effect = TranscriptsDisabled("vid_no_transcript")

        result = ingestor._fetch_and_save_transcript("vid_no_transcript")

        assert result is False

        cursor = db.cursor()
        cursor.execute(
            "SELECT status, transcript_quality FROM videos WHERE video_id = 'vid_no_transcript'"
        )
        row = cursor.fetchone()
        assert row["status"] == "TRANSCRIPT_ERR"
        assert row["transcript_quality"] == "MISSING"

    def test_poor_transcript_classified_correctly(self):
        """Auto-generated transcript with < 10 segments → POOR."""
        mock_list = MagicMock()
        mock_list.find_manually_created_transcript.side_effect = Exception("no manual")

        few_segments = [{"text": f"w{i}", "start": float(i), "duration": 1.0} for i in range(5)]
        mock_fetched = MagicMock()
        mock_fetched.to_raw_data.return_value = few_segments
        mock_transcript = MagicMock()
        mock_transcript.fetch.return_value = mock_fetched
        mock_list.find_generated_transcript.return_value = mock_transcript

        segments, quality = _get_best_transcript(mock_list, "vid_poor")
        assert quality == "POOR"
        assert segments == few_segments

    def test_good_manual_transcript(self):
        """Manually created transcript → GOOD, regardless of segment count."""
        mock_list = MagicMock()
        many_segments = [{"text": f"w{i}", "start": float(i), "duration": 1.0} for i in range(50)]
        mock_fetched = MagicMock()
        mock_fetched.to_raw_data.return_value = many_segments
        mock_transcript = MagicMock()
        mock_transcript.fetch.return_value = mock_fetched
        mock_list.find_manually_created_transcript.return_value = mock_transcript

        segments, quality = _get_best_transcript(mock_list, "vid_good")
        assert quality == "GOOD"
        assert len(segments) == 50


# ── Status state machine ─────────────────────────────────────────────────── #


class TestStatusMachine:
    def test_new_video_starts_at_fetched(self, db, config):
        ingestor = Ingestor(config, db, youtube_api_key="fake")
        ingestor._save_video(
            {
                "video_id": "state_test",
                "channel_id": "UCabc",
                "channel_name": "Test",
                "title": "State Test",
                "description": "",
                "published_at": "2026-03-22T00:00:00Z",
                "duration_seconds": 600,
                "source_type": "youtube",
            }
        )

        cursor = db.cursor()
        cursor.execute("SELECT status FROM videos WHERE video_id = 'state_test'")
        assert cursor.fetchone()["status"] == "FETCHED"

    def test_transcribed_video_advances_state(self, db, config):
        ingestor = Ingestor(config, db, youtube_api_key="fake")
        ingestor._save_video(
            {
                "video_id": "trans_test",
                "channel_id": "UCabc",
                "channel_name": "Test",
                "title": "Transcript State Test",
                "description": "",
                "published_at": "2026-03-22T00:00:00Z",
                "duration_seconds": 600,
                "source_type": "youtube",
            }
        )

        good_segments = [
            {"text": f"Word {i}.", "start": float(i * 10), "duration": 10.0}
            for i in range(20)
        ]
        # Mock FetchedTranscript: .fetch() returns object with .to_raw_data()
        mock_fetched = MagicMock()
        mock_fetched.to_raw_data.return_value = good_segments
        mock_transcript = MagicMock()
        mock_transcript.fetch.return_value = mock_fetched
        mock_list = MagicMock()
        mock_list.find_manually_created_transcript.return_value = mock_transcript

        # Mock the instance's transcript_api directly (new API: instantiated, not static)
        ingestor.transcript_api = MagicMock()
        ingestor.transcript_api.list.return_value = mock_list

        ingestor._fetch_and_save_transcript("trans_test")

        cursor = db.cursor()
        cursor.execute(
            "SELECT status, transcript_quality FROM videos WHERE video_id = 'trans_test'"
        )
        row = cursor.fetchone()
        assert row["status"] == "TRANSCRIBED"
        assert row["transcript_quality"] == "GOOD"
