# TODOS

Items deferred from the initial V1 build review. Ordered by likely sequence.

---

## TODO-1: Upgrade compounding to vector embeddings

**What:** Replace the hybrid domain-tag filter with semantic vector search (sqlite-vec + text-embedding-3-small) when the archive exceeds ~100 briefs.

**Why:** The domain filter degrades as each domain accumulates 50+ briefs — Claude can't read all of them in context. Vector search retrieves the top-K truly relevant briefs regardless of archive size.

**Pros:** Compounding stays accurate at 200, 500, 1000 briefs. The 'Connects To' sections remain high-quality at Month 3 targets.

**Cons:** Requires an OpenAI API key (or embedding via Claude), sqlite-vec extension setup, and a one-time re-embedding of existing briefs.

**Context:** V1 uses hybrid domain-tag filter (query prior briefs sharing ≥1 domain with new video, limit 20-30, send to Haiku for connection detection). This is correct for V1 but has a known ceiling. The upgrade trigger is ~100 archived briefs or when 'Connects To' quality noticeably degrades. To upgrade: install sqlite-vec, embed each brief on save, add similarity query to analyzer.py compounding step.

**Depends on / blocked by:** V1 compounding fully working; archive reaching ~100 briefs.

---

## TODO-2: Natural language archive query

**What:** CLI command `python run.py --query "What do I know about AI agents?"` that searches the archive and returns a synthesized answer drawn from relevant briefs.

**Why:** V1 only pushes to Notion. This adds a pull layer — the ability to actively query accumulated knowledge, not just read it passively. Closes the loop on the compounding vision.

**Pros:** Makes the archive actively useful. Surfaces knowledge across creators that would otherwise require manually reading 200 Notion pages.

**Cons:** Requires vector search (TODO-1) for semantic queries, or SQLite FTS5 for keyword queries. FTS5 is built into SQLite — a keyword-only V1 of this feature is achievable without embeddings.

**Context:** Explicitly V2 scope in the PRD. A simpler first step: add FTS5 index to the briefs column in the videos table, implement `--query` as a keyword search with Claude synthesis of results. Upgrade to semantic search after TODO-1.

**Depends on / blocked by:** V1 stable; TODO-1 for semantic search quality.

---

## TODO-3: GitHub Actions migration (Phase 2)

**What:** Move the daily pipeline from Mac launchd to GitHub Actions scheduled workflow. Replace Mac-local SQLite with hosted SQLite (Turso) or Postgres.

**Why:** Mac is a single point of failure. If the lid is closed, it's traveling, or it's asleep, the 7am brief doesn't arrive. GitHub Actions runs on GitHub infrastructure regardless of machine state — hits the 99% reliability NFR.

**Pros:** True 99% reliability. Enables full Phase 2 as planned in the PRD. No longer dependent on Mac being awake and online.

**Cons:** SQLite can't be used as-is (Actions is stateless between runs). Requires a hosted SQLite solution (Turso is the simplest drop-in) or migration to Postgres. One-time data migration from local DB.

**Context:** Phase 2 explicitly planned in the PRD. The analyzer.py is already using Anthropic SDK (decided in V1 review — no migration needed there). Key decisions for this migration: (1) Turso vs self-hosted SQLite on a $5 VPS, (2) whether to add parallel processing (ThreadPoolExecutor) at the same time, (3) secrets management via GitHub Actions secrets.

**Depends on / blocked by:** V1 validated for 2+ weeks with consistent output quality and daily email delivery.
