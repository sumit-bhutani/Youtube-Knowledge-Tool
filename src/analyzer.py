"""
Analyzer — Phase 1 (stub)

Responsibilities (to be built):
  - Relevance scoring via Claude Haiku (tool_use → score int + domain tags list)
  - Full 5-step brief via Claude Sonnet (tool_use for structured fields + markdown body)
  - Compounding: hybrid domain-tag filter against prior briefs, connection detection via Haiku
  - Cost tracking: log input/output tokens per task to run_log

This is the ONLY module that touches Claude (anthropic SDK).
Model selection comes from config.yaml:
  models.scoring     → Haiku (cheap, fast)
  models.brief       → Sonnet (high quality)
  models.compounding → Haiku (cheap, fast)
"""


class Analyzer:
    def __init__(self, config: dict, db, anthropic_client):
        self.config = config
        self.db = db
        self.client = anthropic_client

    def run(self) -> dict:
        """Process all TRANSCRIBED videos. Returns cost summary."""
        raise NotImplementedError("Analyzer coming in next phase")
