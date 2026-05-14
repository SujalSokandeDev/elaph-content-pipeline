#!/usr/bin/env python3
"""
Elaph Crawler - Main Pipeline Orchestrator
Crawls elaph.com website incrementally and stores content in BigQuery.

Usage:
    python pipeline.py --mode incremental        # Crawl pending URLs only (default)
    python pipeline.py --mode full                # Reset and re-crawl everything
    python pipeline.py --stats                   # Show crawl statistics
    python pipeline.py --check-new                # Parse sitemap, show new URLs
    python pipeline.py --dry-run                  # Show what would be crawled
    python pipeline.py --max-urls N               # Stop after N URLs
    python pipeline.py --time-limit SECONDS       # Stop after N seconds
"""

import argparse
import json
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, Tuple, List

sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from scrapling.fetchers import Fetcher, FetcherSession
    SCRAPLING_AVAILABLE = True
except ImportError:
    SCRAPLING_AVAILABLE = False

from elaph_crawler.utils.config import Config
from elaph_crawler.utils.logger import setup_logging, get_logger
from elaph_crawler.core import (
    SupabaseManager,
    SitemapParser,
    ArticleScraper,
    BigQueryManager,
    DryRunBigQueryManager,
)


class ElaphPipeline:
    """Main pipeline orchestrator for Elaph web crawling."""

    def __init__(self, config: Config = None, dry_run: bool = False):
        self.config = config or Config.load()
        self.dry_run = dry_run

        self.logger = setup_logging(
            log_level=self.config.log_level,
            log_file=self.config.log_file
        )

        self.db = SupabaseManager(
            self.config.supabase_url,
            self.config.supabase_key
        )
        self.sitemap_parser = SitemapParser(self.logger)
        self.article_scraper = ArticleScraper(self.logger)

        self._bq_manager = None
        self.fetcher = None

        self.stop_requested = False
        self.start_time = None

    @property
    def bq_manager(self):
        """Lazy initialization of BigQuery manager."""
        if self._bq_manager is None:
            if self.dry_run:
                self._bq_manager = DryRunBigQueryManager(self.logger)
            else:
                self._bq_manager = BigQueryManager(
                    project_id=self.config.gcp_project_id,
                    dataset=self.config.bigquery_dataset,
                    table=self.config.bigquery_table,
                    credentials_path=self.config.google_credentials_path,
                    logger=self.logger
                )
        return self._bq_manager

    def _init_fetcher(self):
        """Initialize the Scrapling fetcher."""
        if not SCRAPLING_AVAILABLE:
            raise ImportError("scrapling package is not installed")

        from scrapling.fetchers import Fetcher
        self.fetcher = Fetcher

    def run_diagnostics(self) -> bool:
        """Test all connections and show status."""
        self.logger.info("")
        self.logger.info("=" * 70)
        self.logger.info("ELAPH CRAWLER - DIAGNOSTICS")
        self.logger.info("=" * 70)
        self.logger.info(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info("")

        all_ok = True

        self.logger.info("-" * 70)
        self.logger.info("STEP 1: Configuration Validation")
        self.logger.info("-" * 70)
        errors = self.config.validate()
        if errors:
            self.logger.error("Configuration errors:")
            for e in errors:
                self.logger.error(f"  - {e}")
            return False
        self.logger.info("Configuration validated successfully")
        self.logger.info("")

        self.logger.info("-" * 70)
        self.logger.info("STEP 2: Supabase Connection")
        self.logger.info("-" * 70)
        try:
            stats = self.db.get_stats()
            self.logger.info(f"Supabase URL: {self.config.supabase_url[:30]}...")
            self.logger.info(f"Total URLs tracked: {stats['total']:,}")
            self.logger.info(f"  Pending: {stats['pending']:,}")
            self.logger.info(f"  Done: {stats['done']:,}")
            self.logger.info(f"  Error: {stats['error']:,}")
            self.logger.info("Supabase connection successful")
        except Exception as e:
            self.logger.error(f"Supabase connection failed: {e}")
            all_ok = False
        self.logger.info("")

        self.logger.info("-" * 70)
        self.logger.info("STEP 3: BigQuery Connection")
        self.logger.info("-" * 70)
        if self.dry_run:
            self.logger.info("Skipped (dry run mode)")
        else:
            try:
                success, message = self.bq_manager.test_connection()
                if success:
                    self.logger.info(f"Project: {self.config.gcp_project_id}")
                    self.logger.info(f"Dataset: {self.config.bigquery_dataset}")
                    self.logger.info(f"Table: {self.config.bigquery_table}")
                    self.logger.info(f"BigQuery: {message}")
                else:
                    self.logger.error(f"BigQuery: {message}")
                    all_ok = False
            except Exception as e:
                self.logger.error(f"BigQuery connection failed: {e}")
                all_ok = False
        self.logger.info("")

        self.logger.info("-" * 70)
        self.logger.info("STEP 4: Scrapling Module")
        self.logger.info("-" * 70)
        if SCRAPLING_AVAILABLE:
            self.logger.info("Scrapling available")
            self.logger.info("Fetcher will use Chrome impersonation")
        else:
            self.logger.error("Scrapling not installed")
            all_ok = False
        self.logger.info("")

        self.logger.info("=" * 70)
        if all_ok:
            self.logger.info("DIAGNOSTICS COMPLETE: ALL CHECKS PASSED")
        else:
            self.logger.info("DIAGNOSTICS COMPLETE: SOME CHECKS FAILED")
        self.logger.info("=" * 70)

        return all_ok

    def check_new_urls(self) -> Dict:
        """Parse sitemaps and show new/changed URLs without crawling."""
        self.logger.info("=" * 70)
        self.logger.info("CHECKING FOR NEW URLS")
        self.logger.info("=" * 70)

        self._init_fetcher()

        self.logger.info("")
        self.logger.info("--- Fetching Main Sitemap Index ---")
        main_urls = self.sitemap_parser.fetch_and_parse_all_sitemaps(
            self.fetcher,
            self.config.sitemap_main
        )
        self.logger.info(f"URLs from main sitemap: {len(main_urls):,}")

        self.logger.info("")
        self.logger.info("--- Fetching Google News Sitemap ---")
        news_urls = self.sitemap_parser.fetch_google_news_sitemap(
            self.fetcher,
            self.config.sitemap_google_news
        )
        self.logger.info(f"URLs from Google News sitemap: {len(news_urls):,}")

        all_urls = self.sitemap_parser.merge_sitemap_results(main_urls, news_urls)

        self.logger.info("")
        self.logger.info("=" * 70)
        self.logger.info("SUMMARY")
        self.logger.info("=" * 70)
        self.logger.info(f"Total URLs from main sitemap: {len(main_urls):,}")
        self.logger.info(f"Total URLs from Google News: {len(news_urls):,}")
        self.logger.info(f"Total unique URLs (after dedup): {len(all_urls):,}")

        existing_stats = self.db.get_stats()
        existing_urls = set()
        try:
            from elaph_crawler.core import SupabaseManager
            recent = self.db.get_recent_crawled(limit=100000)
            existing_urls = set(r['url'] for r in recent)
        except:
            pass

        new_urls = sum(1 for u in all_urls if u['url'] not in existing_urls)
        already_done = sum(1 for u in all_urls if u['url'] in existing_urls)

        self.logger.info("")
        self.logger.info("Breakdown:")
        self.logger.info(f"  New (pending): {new_urls:,}")
        self.logger.info(f"  Already done: {already_done:,}")

        self.logger.info("")
        self.logger.info("First 10 URLs:")
        for url in all_urls[:10]:
            self.logger.info(f"  {url['url']}")

        return {
            "total_found": len(all_urls),
            "tracked": existing_stats["total"],
            "urls": all_urls,
            "main_count": len(main_urls),
            "news_count": len(news_urls),
            "new_count": new_urls,
            "done_count": already_done
        }

    def update_sitemap_state(self) -> int:
        """Fetch sitemaps and update Supabase crawl state."""
        self.logger.info("=" * 70)
        self.logger.info("UPDATING SITEMAP STATE")
        self.logger.info("=" * 70)

        self._init_fetcher()

        main_urls = self.sitemap_parser.fetch_and_parse_all_sitemaps(
            self.fetcher,
            self.config.sitemap_main
        )

        news_urls = self.sitemap_parser.fetch_google_news_sitemap(
            self.fetcher,
            self.config.sitemap_google_news
        )

        all_urls = self.sitemap_parser.merge_sitemap_results(main_urls, news_urls)

        if all_urls:
            self.logger.info(f"Upserting {len(all_urls)} URLs to Supabase...")
            count = self.db.upsert_urls(all_urls, batch_size=5000)
            self.logger.info(f"Upserted {count} URLs to crawl state")
            self.logger.debug(f"Sample URL: {all_urls[0] if all_urls else 'none'}")
        else:
            self.logger.warning("No URLs found in sitemaps")

        return len(all_urls)

    def run_crawl(self, mode: str, max_urls: int = None, time_limit: int = None) -> Dict:
        """Run the crawl in specified mode."""
        crawl_id = f"elaph_crawl_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

        self.logger.info("=" * 70)
        self.logger.info(f"ELAPH CRAWLER - {mode.upper()} MODE")
        self.logger.info(f"Crawl ID: {crawl_id}")
        self.logger.info("=" * 70)

        if mode == "full":
            self.logger.info("Resetting all done URLs to pending...")
            reset_count = self.db.reset_to_pending()
            self.logger.info(f"Reset {reset_count} URLs to pending")

        self.db.reset_error_urls(self.config.max_url_failures)

        self._init_fetcher()

        self.db.create_checkpoint(crawl_id, mode, total_urls=0)

        batch = []
        batch_urls = []  # track URLs in current batch for deferred marking
        batch_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        processed = 0
        successful = 0
        failed = 0

        self.start_time = time.time()

        pending_urls = self.db.get_pending_urls(limit=max_urls or 10000)
        total_pending = len(pending_urls)

        self.logger.info(f"Found {total_pending} pending URLs to crawl")
        self.logger.info("")

        for idx, url_record in enumerate(pending_urls):
            if self.stop_requested:
                self.logger.info("Stop requested, stopping crawl...")
                break

            if max_urls and idx >= max_urls:
                self.logger.info(f"Reached max URLs limit: {max_urls}")
                break

            if time_limit and (time.time() - self.start_time) >= time_limit:
                self.logger.info(f"Reached time limit: {time_limit}s")
                break

            url = url_record["url"]

            if (idx + 1) % 10 == 0 or idx == 0:
                elapsed = time.time() - self.start_time
                rate = (idx + 1) / elapsed if elapsed > 0 else 0
                remaining = (total_pending - idx) / rate if rate > 0 else 0
                self.logger.info(
                    f"Progress: {idx + 1}/{total_pending} | "
                    f"Success: {successful} | Failed: {failed} | "
                    f"Rate: {rate:.1f}/s | ETA: {remaining/60:.1f}min"
                )

            try:
                response = self.fetcher.get(url, impersonate='chrome')

                if response and response.status == 200:
                    article = self.article_scraper.extract(response)

                    if article and article.get("content"):
                        content_hash = self.article_scraper.compute_content_hash(
                            article["content"]
                        )

                        batch.append(article)
                        batch_urls.append({"url": url, "content_hash": content_hash})
                        processed += 1
                        successful += 1

                        if len(batch) >= self.config.batch_size:
                            self._flush_and_mark(batch, batch_urls, batch_id)
                            batch = []
                            batch_urls = []
                            batch_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

                    else:
                        failed += 1
                        self.db.mark_url_error(url, "No content extracted")
                else:
                    failed += 1
                    self.db.mark_url_error(
                        url, f"HTTP {response.status if response else 'No response'}"
                    )

            except Exception as e:
                failed += 1
                self.db.mark_url_error(url, str(e))
                self.logger.debug(f"Error crawling {url}: {e}")

            time.sleep(self.config.request_delay)

        # Flush remaining batch
        if batch:
            self._flush_and_mark(batch, batch_urls, batch_id)

        self.db.update_checkpoint(
            crawl_id,
            processed=processed,
            successful=successful,
            failed=failed,
            status="completed"
        )

        stats = self.db.get_stats()

        self.logger.info("")
        self.logger.info("=" * 70)
        self.logger.info("CRAWL COMPLETE")
        self.logger.info("=" * 70)
        self.logger.info(f"Processed: {processed}")
        self.logger.info(f"Successful: {successful}")
        self.logger.info(f"Failed: {failed}")
        self.logger.info(f"Remaining pending: {stats['pending']}")
        self.logger.info("=" * 70)

        return {
            "crawl_id": crawl_id,
            "processed": processed,
            "successful": successful,
            "failed": failed,
            "pending": stats["pending"],
        }

    def _flush_and_mark(self, batch: List[Dict], batch_urls: List[Dict], batch_id: str):
        """Flush batch to BigQuery, then mark URLs as done only on success."""
        if not batch:
            return

        try:
            inserted, failed_count, error_list = self.bq_manager.insert_batch(batch, batch_id)
            if error_list:
                self.logger.warning(f"Batch insert had {failed_count} errors: {error_list[:2]}")

            if failed_count == 0:
                # BigQuery insert succeeded — now mark all batch URLs as done
                for item in batch_urls:
                    self.db.mark_url_done(item["url"], batch_id, item["content_hash"])
            else:
                # BigQuery insert failed — mark URLs as error so they get retried
                for item in batch_urls:
                    self.db.mark_url_error(item["url"], f"BigQuery batch insert failed: {error_list[:1]}")
        except Exception as e:
            self.logger.error(f"Batch insert failed: {e}")
            # Mark all URLs in failed batch as error so they get retried
            for item in batch_urls:
                self.db.mark_url_error(item["url"], f"BigQuery exception: {str(e)[:200]}")

    def show_stats(self):
        """Display crawl statistics."""
        stats = self.db.get_stats()

        self.logger.info("=" * 70)
        self.logger.info("CRAWL STATISTICS")
        self.logger.info("=" * 70)
        self.logger.info(f"Total URLs: {stats['total']:,}")
        self.logger.info(f"  Pending: {stats['pending']:,}")
        self.logger.info(f"  Done: {stats['done']:,}")
        self.logger.info(f"  Error: {stats['error']:,}")

        checkpoints = self.db.get_recent_checkpoints(limit=5)
        if checkpoints:
            self.logger.info("")
            self.logger.info("Recent Checkpoints:")
            for cp in checkpoints:
                status_icon = "[OK]" if cp.get("status") == "completed" else "[..]" if cp.get("status") == "running" else "[!!]"
                self.logger.info(
                    f"  {status_icon} {cp.get('crawl_id', 'N/A')[:40]} | "
                    f"Mode: {cp.get('mode', 'N/A')} | "
                    f"Processed: {cp.get('processed_urls', 0)}"
                )


def main():
    parser = argparse.ArgumentParser(description="Elaph Crawler Pipeline")
    parser.add_argument(
        "--mode",
        choices=["incremental", "full"],
        default="incremental",
        help="Crawling mode"
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Show crawl statistics"
    )
    parser.add_argument(
        "--check-new",
        action="store_true",
        help="Check for new URLs without crawling"
    )
    parser.add_argument(
        "--update-sitemap",
        action="store_true",
        help="Fetch sitemaps and update crawl state"
    )
    parser.add_argument(
        "--diagnostics",
        action="store_true",
        help="Run connection diagnostics"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Dry run (no BigQuery insert)"
    )
    parser.add_argument(
        "--max-urls",
        type=int,
        help="Stop after N URLs"
    )
    parser.add_argument(
        "--time-limit",
        type=int,
        help="Stop after N seconds"
    )

    args = parser.parse_args()

    pipeline = ElaphPipeline(dry_run=args.dry_run)

    if args.diagnostics:
        success = pipeline.run_diagnostics()
        sys.exit(0 if success else 1)
    elif args.stats:
        pipeline.show_stats()
    elif args.check_new:
        pipeline.check_new_urls()
    elif args.update_sitemap:
        pipeline.update_sitemap_state()
    elif args.mode:
        pipeline.run_crawl(args.mode, args.max_urls, args.time_limit)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()