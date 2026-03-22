"""
Delivery — Phase 1 (stub)

Responsibilities (to be built):
  - Assemble daily email from ANALYZED videos
    Order: CORE (score ≥7) → PERIPHERAL (4–6) → FLAGGED (<4)
    FLAGGED videos are always included, never dropped
  - Include "Connects To" section per video from connections table
  - Send via Gmail API with OAuth2 token refresh handling
  - On Sunday: also trigger weekly synthesis email
  - Log sent_at to digests table; never silently fail
"""


class Delivery:
    def __init__(self, config: dict, db, gmail_credentials):
        self.config = config
        self.db = db
        self.credentials = gmail_credentials

    def run(self) -> dict:
        """Assemble and send daily email. Returns send summary."""
        raise NotImplementedError("Delivery coming in next phase")
