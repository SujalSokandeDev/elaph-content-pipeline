"""
Configuration Module for Elaph Crawler
Loads settings from environment variables.
"""

import os
from pathlib import Path
from dataclasses import dataclass, field
from typing import List
from dotenv import load_dotenv


@dataclass
class Config:
    """Configuration for the Elaph Crawler"""

    base_dir: Path = field(default_factory=lambda: Path(__file__).parent.parent)

    supabase_url: str = ""
    supabase_key: str = ""

    gcp_project_id: str = "ztudiumplatforms"
    bigquery_dataset: str = "elaph_content"
    bigquery_table: str = "elaph_pages"
    google_credentials_path: str = ""

    sitemap_main: str = "https://elaph.com/sitemaps/allsitemaps.xml"
    sitemap_google_news: str = "https://elaph.com/sitemaps/google_news.xml"

    batch_size: int = 5
    request_delay: float = 1.0
    max_retries: int = 3
    max_url_failures: int = 5

    log_level: str = "INFO"
    log_file: str = ""

    @classmethod
    def load(cls) -> "Config":
        """Load configuration from environment variables"""
        base_dir = Path(__file__).parent.parent.parent

        env_path = base_dir / ".env"
        if env_path.exists():
            load_dotenv(env_path)

        data_dir = base_dir / "data"
        data_dir.mkdir(exist_ok=True)

        logs_dir = base_dir / "logs"
        logs_dir.mkdir(exist_ok=True)

        return cls(
            base_dir=base_dir,
            supabase_url=os.getenv("SUPABASE_URL", ""),
            supabase_key=os.getenv("SUPABASE_KEY", ""),
            gcp_project_id=os.getenv("GCP_PROJECT_ID", "ztudiumplatforms"),
            bigquery_dataset=os.getenv("BIGQUERY_DATASET", "elaph_content"),
            bigquery_table=os.getenv("BIGQUERY_TABLE", "elaph_pages"),
            google_credentials_path=os.getenv(
                "GOOGLE_CREDENTIALS_PATH",
                str(base_dir / "data" / "service-account.json")
            ),
            sitemap_main=os.getenv(
                "SITEMAP_MAIN",
                "https://elaph.com/sitemaps/allsitemaps.xml"
            ),
            sitemap_google_news=os.getenv(
                "SITEMAP_GOOGLE_NEWS",
                "https://elaph.com/sitemaps/google_news.xml"
            ),
            batch_size=int(os.getenv("BATCH_SIZE", "5")),
            request_delay=float(os.getenv("REQUEST_DELAY", "1.0")),
            max_retries=int(os.getenv("MAX_RETRIES", "3")),
            max_url_failures=int(os.getenv("MAX_URL_FAILURES", "5")),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            log_file=os.getenv("LOG_FILE", str(logs_dir / "elaph_crawler.log")),
        )

    def validate(self) -> List[str]:
        """Validate required configuration"""
        errors = []

        if not self.supabase_url:
            errors.append("SUPABASE_URL is required")
        if not self.supabase_key:
            errors.append("SUPABASE_KEY is required")
        if not self.gcp_project_id:
            errors.append("GCP_PROJECT_ID is required")

        creds_path = Path(self.google_credentials_path)
        if not creds_path.exists():
            errors.append(f"Service account file not found: {self.google_credentials_path}")

        return errors

    @property
    def data_dir(self) -> Path:
        return self.base_dir / "data"

    @property
    def logs_dir(self) -> Path:
        return self.base_dir / "logs"