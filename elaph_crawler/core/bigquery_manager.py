"""
BigQuery Manager for Elaph Crawler
Handles BigQuery table creation and batched record insertion.
"""

import json
from datetime import datetime
from typing import List, Dict, Tuple, Optional

try:
    from google.cloud import bigquery
    from google.oauth2 import service_account
    BIGQUERY_AVAILABLE = True
except ImportError:
    BIGQUERY_AVAILABLE = False


class BigQueryManager:
    """
    Manages BigQuery operations for Elaph content storage.
    Each row contains a batch of scraped pages optimized for Vertex AI retrieval.
    """

    def __init__(self, project_id: str, dataset: str, table: str,
                 credentials_path: str, logger=None):
        """
        Initialize BigQuery manager.

        Args:
            project_id: GCP project ID
            dataset: BigQuery dataset name
            table: BigQuery table name
            credentials_path: Path to service account JSON
            logger: Optional logger instance
        """
        if not BIGQUERY_AVAILABLE:
            raise ImportError("google-cloud-bigquery package is not installed")

        self.project_id = project_id
        self.dataset = dataset
        self.table = table
        self.table_id = f"{project_id}.{dataset}.{table}"
        self.logger = logger

        self.client = self._init_client(credentials_path)
        self._ensure_table_exists()

    def _log(self, level: str, message: str):
        if self.logger:
            getattr(self.logger, level)(message)
        else:
            print(f"[{level.upper()}] {message}")

    def _init_client(self, credentials_path: str) -> "bigquery.Client":
        """Initialize BigQuery client with service account credentials."""
        try:
            credentials = service_account.Credentials.from_service_account_file(
                credentials_path
            )
            client = bigquery.Client(
                credentials=credentials,
                project=self.project_id,
                location='EU'
            )
            self._log('info', f"BigQuery client initialized for project: {self.project_id}")
            return client
        except Exception as e:
            self._log('error', f"Failed to initialize BigQuery client: {e}")
            raise

    def _ensure_table_exists(self):
        """Create table with schema optimized for Vertex AI Data Store if it doesn't exist."""
        schema = [
            bigquery.SchemaField("batch_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("page_count", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("pages", "JSON", mode="REQUIRED"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
        ]

        try:
            self.client.get_table(self.table_id)
            self._log('info', f"Table exists: {self.table_id}")
        except Exception:
            table = bigquery.Table(self.table_id, schema=schema)
            self.client.create_table(table)
            self._log('info', f"Created table: {self.table_id}")

    def insert_batch(self, pages: List[Dict], batch_id: str) -> Tuple[int, int, List[str]]:
        """
        Insert a batch of scraped pages as a single BigQuery row.

        Args:
            pages: List of page dicts
            batch_id: Unique batch identifier

        Returns:
            Tuple of (success_count, error_count, error_messages)
        """
        if not pages:
            return 0, 0, []

        cleaned_pages = []
        for page in pages:
            cleaned = {
                "url": page.get("url", ""),
                "title": page.get("title", ""),
                "content": page.get("content", ""),
                "author": page.get("author"),
                "published_date": page.get("published_date"),
                "category": page.get("category"),
                "tags": page.get("tags"),
                "images": page.get("images"),
                "description": page.get("description"),
                "language": page.get("language", "ar"),
                "word_count": page.get("word_count", 0),
            }
            cleaned_pages.append(cleaned)

        try:
            rows = [{
                "batch_id": batch_id,
                "page_count": len(cleaned_pages),
                "pages": json.dumps(cleaned_pages, ensure_ascii=False),
                "created_at": datetime.now().isoformat(),
            }]

            errors = self.client.insert_rows_json(self.table_id, rows)

            if errors:
                self._log('warning', f"Batch insert completed with {len(errors)} errors")
                return 0, len(rows), [str(e) for e in errors[:5]]
            else:
                self._log('info', f"Inserted batch {batch_id} with {len(pages)} pages")
                return len(pages), 0, []

        except Exception as e:
            self._log('error', f"Batch insert failed: {e}")
            return 0, len(pages), [str(e)]

    def test_connection(self) -> Tuple[bool, str]:
        """Test BigQuery connection."""
        try:
            self.client.get_table(self.table_id)
            return True, f"Connected to table: {self.table_id}"
        except Exception as e:
            return False, f"Connection failed: {e}"


class DryRunBigQueryManager:
    """Mock BigQuery manager for dry run mode."""

    def __init__(self, logger=None):
        self.logger = logger
        self.inserted_batches = []

    def _log(self, level: str, message: str):
        if self.logger:
            getattr(self.logger, level)(message)
        else:
            print(f"[DRY RUN] [{level.upper()}] {message}")

    def insert_batch(self, pages: List[Dict], batch_id: str) -> Tuple[int, int, List[str]]:
        self.inserted_batches.append({"batch_id": batch_id, "page_count": len(pages)})
        self._log('info', f"[DRY RUN] Would insert batch {batch_id} with {len(pages)} pages")
        return len(pages), 0, []

    def test_connection(self) -> Tuple[bool, str]:
        return True, "Dry run mode - no BigQuery connection"