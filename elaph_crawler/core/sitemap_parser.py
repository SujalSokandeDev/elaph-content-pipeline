"""
Sitemap Parser for Elaph Crawler
Fetches and parses XML sitemaps from elaph.com.
"""

import re
import time
from datetime import datetime
from typing import List, Dict, Optional, Set
from xml.etree import ElementTree as ET


class SitemapParser:
    """
    Parses XML sitemaps and sitemap indexes from elaph.com.
    """

    NON_ARTICLE_PATTERNS = [
        r"/tag/",
        r"/author/",
        r"/page/",
        r"/feed",
        r"/feed/",
        r"/comment-page",
        r"\?",
        r"#",
        r"/category/",
        r"/search/",
        r"/archives/",
        r"/sitemap",
        r"/wp-json",
    ]

    ARTICLE_URL_PATTERNS = [
        r"/\d{4}/\d{2}/",  # Date-based URLs: /2024/01/
        r"/news/",
        r"/politics/",
        r"/business/",
        r"/sports/",
        r"/culture/",
        r"/technology/",
    ]

    def __init__(self, logger=None):
        self.logger = logger
        self.session = None

    def _log(self, level: str, message: str):
        if self.logger:
            getattr(self.logger, level)(message)
        else:
            print(f"[{level.upper()}] {message}")

    def is_article_url(self, url: str) -> bool:
        """
        Filter URLs to keep only article/content pages.
        Exclude tags, authors, pagination, feeds, etc.
        """
        url_lower = url.lower()

        for pattern in self.NON_ARTICLE_PATTERNS:
            if re.search(pattern, url_lower):
                return False

        for pattern in self.ARTICLE_URL_PATTERNS:
            if re.search(pattern, url_lower):
                return True

        return False

    def parse_sitemap_index(self, xml_content: str) -> List[str]:
        """
        Parse a sitemap index XML and return list of sub-sitemap URLs.

        Args:
            xml_content: XML content of sitemap index

        Returns:
            List of sub-sitemap URLs
        """
        try:
            root = ET.fromstring(xml_content)
            namespaces = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

            sitemap_urls = []
            for elem in root.findall(".//sm:loc", namespaces):
                if elem.text:
                    sitemap_urls.append(elem.text)

            if not sitemap_urls:
                for elem in root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc"):
                    if elem.text:
                        sitemap_urls.append(elem.text)

            self._log("info", f"Found {len(sitemap_urls)} sub-sitemaps in index")
            return sitemap_urls

        except Exception as e:
            self._log("error", f"Failed to parse sitemap index: {e}")
            return []

    def parse_sitemap(self, xml_content: str) -> List[Dict[str, Optional[str]]]:
        """
        Parse a standard sitemap XML or video sitemap and return list of URL entries.

        Args:
            xml_content: XML content of sitemap

        Returns:
            List of dicts with 'url' and 'lastmod' keys
        """
        try:
            root = ET.fromstring(xml_content)
            namespaces = {
                "sm": "http://www.sitemaps.org/schemas/sitemap/0.9",
                "video": "http://www.google.com/schemas/sitemap-video/1.1",
                "news": "http://www.google.com/schemas/sitemap-news/0.9"
            }

            entries = []

            for url_elem in root.findall(".//sm:url", namespaces):
                loc_elem = url_elem.find("sm:loc", namespaces)
                if loc_elem is None:
                    loc_elem = url_elem.find("{http://www.sitemaps.org/schemas/sitemap/0.9}loc")

                if loc_elem is None or not loc_elem.text:
                    continue

                url = loc_elem.text

                if not self.is_article_url(url):
                    continue

                lastmod_elem = url_elem.find("sm:lastmod", namespaces)
                if lastmod_elem is None:
                    lastmod_elem = url_elem.find("{http://www.sitemaps.org/schemas/sitemap/0.9}lastmod")

                lastmod = None
                if lastmod_elem is not None and lastmod_elem.text:
                    lastmod = lastmod_elem.text

                entries.append({
                    "url": url,
                    "lastmod": lastmod,
                })

            for video_elem in root.findall(".//video:video", namespaces):
                loc_elem = video_elem.find("video:loc", namespaces)
                if loc_elem is None:
                    loc_elem = video_elem.find("{http://www.google.com/schemas/sitemap-video/1.1}loc")

                if loc_elem is None or not loc_elem.text:
                    continue

                url = loc_elem.text

                lastmod = None
                date_elem = video_elem.find("video:publication_date", namespaces)
                if date_elem is None:
                    date_elem = video_elem.find("{http://www.google.com/schemas/sitemap-video/1.1}publication_date")
                if date_elem is not None and date_elem.text:
                    lastmod = date_elem.text

                entries.append({
                    "url": url,
                    "lastmod": lastmod,
                })

            self._log("info", f"Found {len(entries)} article URLs in sitemap")
            return entries

        except Exception as e:
            self._log("error", f"Failed to parse sitemap: {e}")
            return []

    def fetch_sitemap(self, fetcher, url: str) -> Optional[str]:
        """
        Fetch sitemap content using the scraper fetcher.

        Args:
            fetcher: Scraping fetcher instance
            url: URL to fetch

        Returns:
            XML content or None on failure
        """
        try:
            response = fetcher.get(url)
            if response and response.status == 200:
                return response.body
            else:
                self._log("warning", f"Failed to fetch sitemap {url}: status {response.status if response else 'None'}")
                return None
        except Exception as e:
            self._log("error", f"Error fetching sitemap {url}: {e}")
            return None

    def fetch_and_parse_all_sitemaps(self, fetcher, main_sitemap_url: str) -> List[Dict]:
        """
        Fetch main sitemap index, parse all sub-sitemaps, and extract article URLs.

        Args:
            fetcher: Scraping fetcher instance
            main_sitemap_url: URL of main sitemap index

        Returns:
            List of dicts with 'url' and 'lastmod' keys
        """
        all_urls = []

        self._log("info", f"Fetching main sitemap index: {main_sitemap_url}")
        index_content = self.fetch_sitemap(fetcher, main_sitemap_url)

        if not index_content:
            self._log("error", "Could not fetch main sitemap index")
            return []

        sub_sitemaps = self.parse_sitemap_index(index_content)

        if not sub_sitemaps:
            self._log("warning", "No sub-sitemaps found, treating as regular sitemap")
            all_urls.extend(self.parse_sitemap(index_content))
            return all_urls

        for idx, sitemap_url in enumerate(sub_sitemaps):
            self._log("info", f"Fetching sub-sitemap {idx + 1}/{len(sub_sitemaps)}: {sitemap_url}")
            content = self.fetch_sitemap(fetcher, sitemap_url)

            if content:
                urls = self.parse_sitemap(content)
                all_urls.extend(urls)
                self._log("info", f"  Found {len(urls)} URLs")
            else:
                self._log("warning", f"  Failed to fetch: {sitemap_url}")

            time.sleep(0.5)

        return all_urls

    def fetch_google_news_sitemap(
        self,
        fetcher,
        google_news_url: str,
    ) -> List[Dict]:
        """
        Fetch Google News sitemap and extract article URLs.

        Args:
            fetcher: Scraping fetcher instance
            google_news_url: URL of Google News sitemap

        Returns:
            List of dicts with 'url' and 'lastmod' keys
        """
        self._log("info", f"Fetching Google News sitemap: {google_news_url}")

        content = self.fetch_sitemap(fetcher, google_news_url)

        if not content:
            self._log("warning", "Could not fetch Google News sitemap")
            return []

        urls = self.parse_sitemap(content)
        self._log("info", f"Found {len(urls)} URLs in Google News sitemap")
        return urls

    def merge_sitemap_results(
        self,
        main_urls: List[Dict],
        news_urls: List[Dict],
    ) -> List[Dict]:
        """
        Merge URLs from main sitemaps and Google News sitemap.
        Deduplicate by URL, keep most recent lastmod.

        Args:
            main_urls: URLs from main sitemaps
            news_urls: URLs from Google News sitemap

        Returns:
            Merged and deduplicated list
        """
        url_map: Dict[str, Dict] = {}

        for item in main_urls:
            url = item["url"]
            url_map[url] = {
                "url": url,
                "lastmod": item.get("lastmod"),
            }

        for item in news_urls:
            url = item["url"]
            news_lastmod = item.get("lastmod")

            if url in url_map:
                existing_lastmod = url_map[url].get("lastmod")
                if news_lastmod and existing_lastmod:
                    if news_lastmod > existing_lastmod:
                        url_map[url]["lastmod"] = news_lastmod
                elif news_lastmod and not existing_lastmod:
                    url_map[url]["lastmod"] = news_lastmod
            else:
                url_map[url] = {
                    "url": url,
                    "lastmod": news_lastmod,
                }

        self._log("info", f"Merged {len(url_map)} unique URLs from both sources")
        return list(url_map.values())