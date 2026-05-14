"""
Supabase State Manager for Elaph Crawler
Manages crawl state tracking and checkpoint tracking in Supabase.
"""

import hashlib
import logging
import os
from datetime import datetime
from typing import List, Dict, Optional, Any
from supabase import create_client, Client


class SupabaseManager:
    """
    Manages pipeline state in Supabase for the Elaph crawler.
    """

    STATE_TABLE = "elaph_crawl_state"
    CHECKPOINTS_TABLE = "elaph_crawl_checkpoints"

    def __init__(self, supabase_url: str = None, supabase_key: str = None):
        """
        Initialize Supabase client.

        Args:
            supabase_url: Supabase project URL (falls back to env SUPABASE_URL)
            supabase_key: Supabase API key (falls back to env SUPABASE_KEY)
        """
        url = supabase_url or os.getenv("SUPABASE_URL", "")
        key = supabase_key or os.getenv("SUPABASE_KEY", "")

        if not url or not key:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set")

        self.client: Client = create_client(url, key)

    def upsert_urls(self, urls: List[Dict[str, Any]], batch_size: int = 1000) -> int:
        """
        Insert new URLs into crawl state table.
        Only genuinely new URLs are inserted as pending.
        Existing URLs (already done/pending/error) are NOT modified.

        Args:
            urls: List of dicts with 'url' and optional 'lastmod'
            batch_size: Number of URLs per batch (default 1000)

        Returns:
            Count of new URLs inserted
        """
        if not urls:
            return 0

        total_inserted = 0
        total_batches = (len(urls) + batch_size - 1) // batch_size
        for i in range(0, len(urls), batch_size):
            batch = urls[i:i + batch_size]
            rows = [
                {
                    "url": item["url"],
                    "lastmod": item.get("lastmod"),
                    "first_seen": datetime.now().isoformat(),
                    "status": "pending",
                }
                for item in batch
            ]

            try:
                resp = self.client.table(self.STATE_TABLE).upsert(
                    rows,
                    on_conflict="url",
                    ignore_duplicates=True,
                ).execute()
                batch_count = len(resp.data) if resp.data else 0
                total_inserted += batch_count
                batch_num = i // batch_size + 1
                if batch_num % 10 == 0 or batch_num == total_batches:
                    logging.info(f"Progress: {batch_num}/{total_batches} batches ({total_inserted} new URLs)")
            except Exception as e:
                batch_num = i // batch_size + 1
                logging.error(f"Batch {batch_num} failed: {e}")
                continue

        return total_inserted

    def get_pending_urls(self, limit: int = 1000) -> List[Dict]:
        """
        Get URLs with status='pending'.

        Args:
            limit: Maximum number of URLs to return

        Returns:
            List of URL records
        """
        try:
            resp = (
                self.client.table(self.STATE_TABLE)
                .select("*")
                .eq("status", "pending")
                .order("first_seen", desc=False)
                .limit(limit)
                .execute()
            )
            return resp.data or []
        except Exception:
            return []

    def get_pending_count(self) -> int:
        """Get count of pending URLs."""
        try:
            resp = (
                self.client.table(self.STATE_TABLE)
                .select("url", count="exact")
                .eq("status", "pending")
                .execute()
            )
            return resp.count or 0
        except Exception:
            return 0

    def get_stats(self) -> Dict[str, Any]:
        """Get crawl state statistics."""
        stats = {
            "total": 0,
            "pending": 0,
            "done": 0,
            "error": 0,
        }

        try:
            resp = self.client.table(self.STATE_TABLE).select(
                "status", count="exact"
            ).execute()
            stats["total"] = resp.count or 0
        except Exception:
            pass

        for status in ["pending", "done", "error"]:
            try:
                resp = self.client.table(self.STATE_TABLE).select(
                    "url", count="exact"
                ).eq("status", status).execute()
                stats[status] = resp.count or 0
            except Exception:
                pass

        return stats

    def mark_url_done(
        self,
        url: str,
        batch_id: str,
        content_hash: str,
    ) -> bool:
        """
        Mark a URL as successfully crawled and inserted to BigQuery.

        Args:
            url: The URL that was crawled
            batch_id: BigQuery batch ID
            content_hash: Hash of the content

        Returns:
            True if update succeeded
        """
        update_data = {
            "status": "done",
            "crawled_at": datetime.now().isoformat(),
            "batch_id": batch_id,
            "content_hash": content_hash,
            "error_message": None,
        }

        try:
            self.client.table(self.STATE_TABLE).update(update_data).eq(
                "url", url
            ).execute()
            return True
        except Exception as e:
            logging.error(f"Failed to mark URL done: {url}: {e}")
            return False

    def mark_url_error(self, url: str, error_message: str) -> bool:
        """
        Mark a URL as failed with an error message.

        Args:
            url: The URL that failed
            error_message: Description of what went wrong

        Returns:
            True if update succeeded
        """
        try:
            self.client.table(self.STATE_TABLE).update({
                "status": "error",
                "error_message": error_message[:500],
                "crawled_at": datetime.now().isoformat(),
            }).eq("url", url).execute()
            return True
        except Exception as e:
            logging.error(f"Failed to mark URL error: {url}: {e}")
            return False

    def reset_to_pending(self) -> int:
        """Reset all done URLs to pending (for full re-crawl)."""
        try:
            resp = (
                self.client.table(self.STATE_TABLE)
                .update({"status": "pending", "crawled_at": None, "batch_id": None})
                .eq("status", "done")
                .execute()
            )
            return len(resp.data) if resp.data else 0
        except Exception:
            return 0

    def reset_error_urls(self, max_failures: int = 5) -> int:
        """Reset error URLs to pending if under failure threshold."""
        try:
            resp = (
                self.client.table(self.STATE_TABLE)
                .select("url, scrape_count")
                .eq("status", "error")
                .execute()
            )

            reset_count = 0
            for row in resp.data or []:
                if (row.get("scrape_count") or 0) < max_failures:
                    self.client.table(self.STATE_TABLE).update({
                        "status": "pending",
                        "error_message": None,
                    }).eq("url", row["url"]).execute()
                    reset_count += 1

            return reset_count
        except Exception:
            return 0

    def get_recent_crawled(self, limit: int = 50) -> List[Dict]:
        """Get recently crawled URLs."""
        try:
            resp = (
                self.client.table(self.STATE_TABLE)
                .select("url, status, crawled_at, content_hash")
                .not_.is_("crawled_at", "null")
                .order("crawled_at", desc=True)
                .limit(limit)
                .execute()
            )
            return resp.data or []
        except Exception:
            return []

    def create_checkpoint(
        self,
        crawl_id: str,
        mode: str,
        total_urls: int = 0,
    ) -> bool:
        """Create a new checkpoint for a crawl run."""
        try:
            self.client.table(self.CHECKPOINTS_TABLE).insert({
                "crawl_id": crawl_id,
                "start_time": datetime.now().isoformat(),
                "total_urls": total_urls,
                "status": "running",
                "mode": mode,
            }).execute()
            return True
        except Exception:
            return False

    def update_checkpoint(
        self,
        crawl_id: str,
        processed: int = None,
        successful: int = None,
        failed: int = None,
        status: str = None,
    ):
        """Update an existing checkpoint."""
        update_data = {}
        if processed is not None:
            update_data["processed_urls"] = processed
        if successful is not None:
            update_data["successful_urls"] = successful
        if failed is not None:
            update_data["failed_urls"] = failed
        if status:
            update_data["status"] = status
            if status in ("completed", "failed", "cancelled"):
                update_data["end_time"] = datetime.now().isoformat()

        if update_data:
            try:
                self.client.table(self.CHECKPOINTS_TABLE).update(
                    update_data
                ).eq("crawl_id", crawl_id).execute()
            except Exception:
                pass

    def get_recent_checkpoints(self, limit: int = 10) -> List[Dict]:
        """Get recent checkpoints."""
        try:
            resp = (
                self.client.table(self.CHECKPOINTS_TABLE)
                .select("*")
                .order("start_time", desc=True)
                .limit(limit)
                .execute()
            )
            return resp.data or []
        except Exception:
            return []