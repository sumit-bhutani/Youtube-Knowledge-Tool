"""
Ingestor — Phase 1

Responsibilities:
  1. Read channel list from config.yaml
  2. Fetch videos published in the last N hours via YouTube playlistItems.list
     (not search.list — 100x cheaper on API quota)
  3. Fetch transcripts via youtube-transcript-api
  4. Classify transcript quality: GOOD / POOR / MISSING
  5. Chunk transcripts into 30-minute segments with timestamps
  6. Save everything to SQLite with the video status state machine
  7. Log every failure — never crash, never silently drop a video

YouTube API quota notes:
  - playlistItems.list:  1 unit/page  (vs search.list: 100 units/call)
  - videos.list:         1 unit/call  (batch up to 50 IDs per call)
  - Daily quota:         10,000 units
  - Typical daily cost:  ~30-50 units for 10 channels + 20 videos

State machine (see db.py for full diagram):
  FETCHED → TRANSCRIBED → ... (analyzer picks up from here)
     ↓            ↓
  FETCH_ERR  TRANSCRIPT_ERR
"""

import json
import logging
import re
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api._errors import (
    NoTranscriptFound,
    TranscriptsDisabled,
)

logger = logging.getLogger(__name__)

# Chunk transcripts every 30 minutes
_CHUNK_SECONDS = 1800

# Transcript quality thresholds
_POOR_SEGMENT_THRESHOLD = 10  # auto-generated with fewer segments than this = POOR


@dataclass
class IngestSummary:
    """Returned by Ingestor.run() — used by run.py to write run_log."""
    channels_scanned: int = 0
    videos_found: int = 0
    videos_new: int = 0
    transcripts_fetched: int = 0
    errors: list = field(default_factory=list)


class Ingestor:
    def __init__(self, config: dict, db: sqlite3.Connection, youtube_api_key: str):
        self.config = config
        self.db = db
        self.youtube = build("youtube", "v3", developerKey=youtube_api_key)
        self.transcript_api = YouTubeTranscriptApi()
        self.lookback_hours = config.get("youtube", {}).get("lookback_hours", 24)

    # ------------------------------------------------------------------ #
    # Public entry point                                                   #
    # ------------------------------------------------------------------ #

    def run(self) -> IngestSummary:
        """
        Process all configured channels.
        A failure in one channel never stops the others.
        Returns IngestSummary for run_log.
        """
        summary = IngestSummary()
        channels = self.config.get("channels", [])

        if not channels:
            logger.warning("No channels configured in config.yaml")
            return summary

        cutoff = datetime.now(timezone.utc) - timedelta(hours=self.lookback_hours)
        logger.info(
            f"Scanning {len(channels)} channel(s) for videos since "
            f"{cutoff.strftime('%Y-%m-%d %H:%M UTC')}"
        )

        for channel in channels:
            channel_name = channel.get("name", channel["id"])
            try:
                result = self._process_channel(channel, cutoff)
                summary.channels_scanned += 1
                summary.videos_found += result["found"]
                summary.videos_new += result["new"]
                summary.transcripts_fetched += result["transcribed"]
            except Exception as exc:
                msg = f"Channel '{channel_name}' failed entirely: {exc}"
                logger.error(msg, exc_info=True)
                summary.errors.append(msg)

        logger.info(
            f"Ingestion done — "
            f"{summary.channels_scanned} channels, "
            f"{summary.videos_found} videos found, "
            f"{summary.videos_new} new, "
            f"{summary.transcripts_fetched} transcribed, "
            f"{len(summary.errors)} error(s)"
        )
        return summary

    # ------------------------------------------------------------------ #
    # Channel processing                                                   #
    # ------------------------------------------------------------------ #

    def _process_channel(self, channel: dict, cutoff: datetime) -> dict:
        channel_id = channel["id"]
        channel_name = channel.get("name", channel_id)
        logger.info(f"  Channel: {channel_name}")

        # Uploads playlist ID is always 'UU' + channel_id[2:]
        # (UC... → UU...) — this is a stable YouTube convention
        uploads_playlist_id = "UU" + channel_id[2:]

        videos = self._fetch_recent_videos(
            playlist_id=uploads_playlist_id,
            channel_id=channel_id,
            channel_name=channel_name,
            cutoff=cutoff,
        )

        new_count = 0
        transcribed_count = 0

        for video in videos:
            video_id = video["video_id"]
            try:
                is_new = self._save_video(video)
                if is_new:
                    new_count += 1
                    logger.info(f"    + {video['title']} [{video_id}]")
                    transcribed = self._fetch_and_save_transcript(video_id)
                    if transcribed:
                        transcribed_count += 1
                else:
                    logger.debug(f"    ~ Already seen: {video_id}")
            except Exception as exc:
                msg = f"Video {video_id} failed: {exc}"
                logger.error(msg, exc_info=True)
                self._set_status(video_id, "FETCH_ERR", error_message=str(exc))

        return {"found": len(videos), "new": new_count, "transcribed": transcribed_count}

    # ------------------------------------------------------------------ #
    # YouTube API: fetch recent videos                                     #
    # ------------------------------------------------------------------ #

    def _fetch_recent_videos(
        self,
        playlist_id: str,
        channel_id: str,
        channel_name: str,
        cutoff: datetime,
    ) -> list[dict]:
        """
        Fetch videos from the channel's uploads playlist published after cutoff.

        Uses playlistItems.list (1 unit/page) not search.list (100 units/call).
        The uploads playlist is ordered newest-first, so we stop as soon as we
        see a video older than the cutoff — no need to page through all history.

        Then batch-fetches contentDetails for all found video IDs in one call.
        """
        video_ids: list[str] = []
        snippets: dict[str, dict] = {}
        page_token: Optional[str] = None

        while True:
            try:
                params = {
                    "part": "snippet",
                    "playlistId": playlist_id,
                    "maxResults": 50,
                }
                if page_token:
                    params["pageToken"] = page_token

                response = self.youtube.playlistItems().list(**params).execute()

            except HttpError as exc:
                status = exc.resp.status
                if status == 404:
                    logger.warning(
                        f"Uploads playlist not found for {channel_name} "
                        f"(playlist_id={playlist_id}). "
                        "Verify the channel ID in config.yaml."
                    )
                elif status == 403:
                    logger.error(
                        f"YouTube API quota exceeded or access denied "
                        f"for {channel_name}. Will retry next run."
                    )
                else:
                    logger.error(f"YouTube API error {status} for {channel_name}: {exc}")
                return []

            stop_paging = False
            for item in response.get("items", []):
                snippet = item["snippet"]

                # Skip private/deleted videos (title is "[Private video]" etc.)
                if snippet.get("title") in ("[Private video]", "[Deleted video]"):
                    continue

                published_str = snippet["publishedAt"]
                published_at = datetime.fromisoformat(
                    published_str.replace("Z", "+00:00")
                )

                if published_at < cutoff:
                    # Playlist is newest-first — everything from here is older
                    stop_paging = True
                    break

                video_id = snippet["resourceId"]["videoId"]
                video_ids.append(video_id)
                snippets[video_id] = snippet

            if stop_paging or not response.get("nextPageToken"):
                break
            page_token = response["nextPageToken"]

        if not video_ids:
            logger.info(f"    No new videos for {channel_name}")
            return []

        # Batch-fetch duration for all video IDs (up to 50 per API call = 1 unit each)
        durations = self._batch_fetch_durations(video_ids)

        videos = []
        for video_id in video_ids:
            snippet = snippets[video_id]
            videos.append(
                {
                    "video_id": video_id,
                    "channel_id": channel_id,
                    "channel_name": channel_name,
                    "title": snippet["title"],
                    "description": snippet.get("description", "")[:2000],
                    "published_at": snippet["publishedAt"],
                    "duration_seconds": durations.get(video_id, 0),
                    "source_type": "youtube",
                }
            )

        logger.info(f"    Found {len(videos)} video(s) for {channel_name}")
        return videos

    def _batch_fetch_durations(self, video_ids: list[str]) -> dict[str, int]:
        """
        Fetch video durations in batches of 50.
        Uses videos.list with contentDetails (1 unit per call, up to 50 IDs).
        """
        durations: dict[str, int] = {}
        for i in range(0, len(video_ids), 50):
            batch = video_ids[i : i + 50]
            try:
                response = (
                    self.youtube.videos()
                    .list(part="contentDetails", id=",".join(batch))
                    .execute()
                )
                for item in response.get("items", []):
                    vid_id = item["id"]
                    iso_duration = item["contentDetails"].get("duration", "PT0S")
                    durations[vid_id] = _parse_iso8601_duration(iso_duration)
            except HttpError as exc:
                logger.warning(f"Could not fetch durations for batch: {exc}")
        return durations

    # ------------------------------------------------------------------ #
    # SQLite: save video                                                   #
    # ------------------------------------------------------------------ #

    def _save_video(self, video: dict) -> bool:
        """
        Insert video record at FETCHED status.
        Returns True if new, False if already exists (idempotent via IGNORE).
        """
        cursor = self.db.cursor()
        try:
            cursor.execute(
                """
                INSERT OR IGNORE INTO videos
                    (video_id, channel_id, channel_name, title, description,
                     published_at, duration_seconds, source_type, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'FETCHED')
                """,
                (
                    video["video_id"],
                    video["channel_id"],
                    video["channel_name"],
                    video["title"],
                    video["description"],
                    video["published_at"],
                    video["duration_seconds"],
                    video["source_type"],
                ),
            )
            self.db.commit()
            return cursor.rowcount > 0
        except Exception:
            self.db.rollback()
            raise

    # ------------------------------------------------------------------ #
    # Transcript: fetch, classify, chunk, save                            #
    # ------------------------------------------------------------------ #

    def _fetch_and_save_transcript(self, video_id: str) -> bool:
        """
        Fetch the best available transcript, chunk it, and save to DB.
        Updates status to TRANSCRIBED or TRANSCRIPT_ERR.
        Returns True if a usable transcript was saved.
        """
        try:
            transcript_list = self.transcript_api.list(video_id)
        except TranscriptsDisabled:
            logger.warning(f"    Transcripts disabled for {video_id} — flagged MISSING")
            self._set_status(
                video_id, "TRANSCRIPT_ERR", transcript_quality="MISSING"
            )
            return False
        except Exception as exc:
            logger.error(f"    list_transcripts failed for {video_id}: {exc}")
            self._set_status(
                video_id, "TRANSCRIPT_ERR",
                transcript_quality="MISSING",
                error_message=str(exc),
            )
            return False

        segments, quality = _get_best_transcript(transcript_list, video_id)

        if segments is None:
            self._set_status(
                video_id, "TRANSCRIPT_ERR", transcript_quality="MISSING"
            )
            return False

        chunks = _chunk_transcript(segments, video_id)
        transcript_text = _format_chunks(chunks)

        cursor = self.db.cursor()
        try:
            cursor.execute(
                """
                UPDATE videos
                SET transcript          = ?,
                    transcript_quality  = ?,
                    transcript_chunks   = ?,
                    status              = 'TRANSCRIBED',
                    updated_at          = CURRENT_TIMESTAMP
                WHERE video_id = ?
                """,
                (transcript_text, quality, len(chunks), video_id),
            )
            self.db.commit()
        except Exception:
            self.db.rollback()
            raise

        duration_min = len(segments) and (
            (segments[-1].get("start", 0) + segments[-1].get("duration", 0)) / 60
        )
        logger.info(
            f"    Transcript saved [{quality}, {len(chunks)} chunk(s), "
            f"~{duration_min:.0f} min]: {video_id}"
        )
        return True

    # ------------------------------------------------------------------ #
    # Status helper                                                        #
    # ------------------------------------------------------------------ #

    def _set_status(
        self,
        video_id: str,
        status: str,
        transcript_quality: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> None:
        """Update video status. Never raises — errors are logged and swallowed."""
        sets = ["status = ?", "updated_at = CURRENT_TIMESTAMP"]
        values: list = [status]

        if transcript_quality is not None:
            sets.append("transcript_quality = ?")
            values.append(transcript_quality)
        if error_message is not None:
            sets.append("error_message = ?")
            values.append(error_message[:1000])  # cap length

        values.append(video_id)
        cursor = self.db.cursor()
        try:
            cursor.execute(
                f"UPDATE videos SET {', '.join(sets)} WHERE video_id = ?",
                values,
            )
            self.db.commit()
        except Exception as exc:
            self.db.rollback()
            logger.error(f"Failed to set status {status} for {video_id}: {exc}")


# ------------------------------------------------------------------ #
# Pure functions — easier to unit-test in isolation                   #
# ------------------------------------------------------------------ #


def _get_best_transcript(
    transcript_list, video_id: str
) -> tuple[Optional[list], str]:
    """
    Return (segments, quality) for the best available transcript.
    Prefers manually-created over auto-generated.
    quality is one of: 'GOOD', 'POOR', 'MISSING'
    segments is None on MISSING.
    """
    # 1. Try manually-created English transcript (always GOOD)
    for lang in ["en", "en-US", "en-GB", "en-CA", "en-AU"]:
        try:
            transcript = transcript_list.find_manually_created_transcript([lang])
            segments = transcript.fetch().to_raw_data()  # → list[dict]
            logger.debug(f"Manual transcript ({lang}) for {video_id}")
            return segments, "GOOD"
        except NoTranscriptFound:
            continue
        except Exception as exc:
            logger.warning(f"Manual transcript fetch error for {video_id}: {exc}")

    # 2. Try auto-generated English transcript
    for lang in ["en", "en-US", "en-GB"]:
        try:
            transcript = transcript_list.find_generated_transcript([lang])
            segments = transcript.fetch().to_raw_data()  # → list[dict]
            quality = "POOR" if len(segments) < _POOR_SEGMENT_THRESHOLD else "GOOD"
            logger.debug(
                f"Auto-generated transcript ({lang}) [{quality}] for {video_id}"
            )
            return segments, quality
        except NoTranscriptFound:
            continue
        except Exception as exc:
            logger.warning(f"Auto transcript fetch error for {video_id}: {exc}")

    # 3. Last resort: any available transcript (non-English)
    try:
        available = list(transcript_list)
        if available:
            segments = available[0].fetch().to_raw_data()  # → list[dict]
            logger.warning(
                f"Non-English transcript ({available[0].language_code}) "
                f"for {video_id} — quality flagged POOR"
            )
            return segments, "POOR"
    except Exception as exc:
        logger.warning(f"Fallback transcript failed for {video_id}: {exc}")

    logger.warning(f"No usable transcript found for {video_id}")
    return None, "MISSING"


def _chunk_transcript(segments: list[dict], video_id: str) -> list[dict]:
    """
    Split transcript segments into 30-minute chunks.

    Handles two cases:
      - Segments with timestamps: chunk by time boundary (normal case)
      - Segments without timestamps: return the full transcript as one chunk
        (some auto-generated transcripts omit start times)

    Each chunk dict: {chunk_index, start_seconds, end_seconds, text}
    """
    if not segments:
        return []

    # Check whether timestamps are present (start=0 on first item is valid)
    has_timestamps = all(
        item.get("start") is not None for item in segments[:5]
    )

    if not has_timestamps:
        logger.warning(
            f"Transcript for {video_id} has no timestamps — "
            "treating as a single chunk"
        )
        full_text = " ".join(
            item.get("text", "").strip() for item in segments if item.get("text")
        )
        return [
            {
                "chunk_index": 0,
                "start_seconds": 0,
                "end_seconds": None,
                "text": full_text,
            }
        ]

    chunks: list[dict] = []
    current_texts: list[str] = []
    current_start: float = segments[0].get("start", 0)
    chunk_index = 0

    for item in segments:
        start = item.get("start", 0)
        text = item.get("text", "").strip()
        if not text:
            continue

        # Start a new chunk when we've accumulated 30 minutes
        if (start - current_start) >= _CHUNK_SECONDS and current_texts:
            chunks.append(
                {
                    "chunk_index": chunk_index,
                    "start_seconds": current_start,
                    "end_seconds": start,
                    "text": " ".join(current_texts),
                }
            )
            chunk_index += 1
            current_start = start
            current_texts = []

        current_texts.append(text)

    # Flush the final chunk
    if current_texts:
        last = segments[-1]
        end = last.get("start", 0) + last.get("duration", 0)
        chunks.append(
            {
                "chunk_index": chunk_index,
                "start_seconds": current_start,
                "end_seconds": end,
                "text": " ".join(current_texts),
            }
        )

    return chunks


def _format_chunks(chunks: list[dict]) -> str:
    """
    Format chunks as a single string.
    Single-chunk transcripts are stored as plain text (no header noise).
    Multi-chunk transcripts get [CHUNK N: Xmin–Ymin] headers for the analyzer.
    """
    if not chunks:
        return ""

    if len(chunks) == 1:
        return chunks[0]["text"]

    parts = []
    for chunk in chunks:
        start_min = int(chunk["start_seconds"] / 60)
        end_seconds = chunk["end_seconds"]
        end_min = f"{int(end_seconds / 60)}" if end_seconds is not None else "?"
        header = f"[CHUNK {chunk['chunk_index'] + 1}: {start_min}–{end_min} min]"
        parts.append(f"{header}\n{chunk['text']}")

    return "\n\n".join(parts)


def _parse_iso8601_duration(duration: str) -> int:
    """
    Parse ISO 8601 duration string to total seconds.
    Examples: 'PT1H30M45S' → 5445, 'PT45M' → 2700, 'PT30S' → 30
    Returns 0 for unparseable input.
    """
    match = re.fullmatch(
        r"P(?:(\d+)D)?T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", duration
    )
    if not match:
        return 0
    days = int(match.group(1) or 0)
    hours = int(match.group(2) or 0)
    minutes = int(match.group(3) or 0)
    seconds = int(match.group(4) or 0)
    return days * 86400 + hours * 3600 + minutes * 60 + seconds
