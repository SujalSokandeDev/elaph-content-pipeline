"""
Article Scraper for Elaph Crawler
Extracts content from elaph.com article pages.
"""

import hashlib
import re
from datetime import datetime
from typing import Dict, Optional


class ArticleScraper:
    """
    Extracts structured content from elaph.com article pages.
    """

    SELECTORS = {
        "title": [
            "h1.article-title",
            "h1.entry-title",
            "h1.post-title",
            "h1.title",
            ".entry-header h1",
            "article h1",
            "h1",
        ],
        "author": [
            ".author-name",
            ".author",
            ".by-author",
            ".author a",
            "[rel='author']",
            ".post-author",
            ".article-author",
            "span.author",
            ".writer-name",
        ],
        "published_date": [
            "span.date",
            "time.entry-date",
            "time.published",
            "time.post-date",
            ".post-date",
            ".article-date",
            "time[datetime]",
        ],
"content": [
            ".content-body",
            "div.body",
            "div.article-body",
            "div.entry-content",
            "div.post-content",
            "div.article-content",
            "article div.content",
            "div.post-body",
            ".article-body",
            "article",
            "main",
        ],
        "category": [
            "meta[property='article:section']",
            ".category",
            ".post-category",
            ".article-category",
            ".breadcrumbs a",
            "nav.breadcrumb a",
            ".section-name",
        ],
        "tags": [
            ".tags",
            ".post-tags",
            ".article-tags",
            ".tag-list",
        ],
        "images": [
            "div.entry-content img",
            "article img",
            ".post-content img",
            ".article-body img",
        ],
        "description": [
            "meta[name='description']",
            "meta[property='og:description']",
        ],
    }

    def __init__(self, logger=None):
        self.logger = logger

    def _log(self, level: str, message: str):
        if self.logger:
            getattr(self.logger, level)(message)

    def extract(self, response) -> Optional[Dict]:
        """
        Extract structured content from a scraped page.

        Args:
            response: Scraping response object

        Returns:
            Dict with extracted fields or None on failure
        """
        try:
            if not response or response.status != 200:
                self._log("warning", f"Invalid response status: {response.status if response else 'None'}")
                return None

            html = response.text if hasattr(response, "text") else str(response)

            title = self._extract_title(response)
            author = self._extract_author(response)
            published_date = self._extract_published_date(response)
            content = self._extract_content(response)
            category = self._extract_category(response)
            tags = self._extract_tags(response)
            images = self._extract_images(response)
            description = self._extract_description(response)

            word_count = len(content.split()) if content else 0

            return {
                "url": self._extract_url(response),
                "title": title,
                "author": author,
                "published_date": published_date,
                "content": content,
                "category": category,
                "tags": tags,
                "images": images,
                "description": description,
                "language": "ar",
                "word_count": word_count,
            }

        except Exception as e:
            self._log("error", f"Error extracting article: {e}")
            return None

    def _extract_text(self, response, selectors: list) -> Optional[str]:
        """Extract text using multiple CSS selectors with fallbacks."""
        if hasattr(response, "css"):
            for selector in selectors:
                try:
                    element = response.css(selector).get()
                    if element:
                        text = element.text.strip() if hasattr(element, "text") else str(element)
                        if text:
                            return text
                except Exception:
                    continue
        return None

    def _extract_first(self, response, selectors: list) -> Optional[str]:
        """Extract first matching element's text or attribute."""
        if hasattr(response, "css"):
            for selector in selectors:
                try:
                    element = response.css(selector).get()
                    if element:
                        if hasattr(element, "text") and element.text:
                            return element.text.strip()
                        elif hasattr(element, "attributes"):
                            for attr in ["content", "title", "src"]:
                                if attr in element.attributes:
                                    val = element.attributes[attr]
                                    if val and isinstance(val, str):
                                        return val.strip()
                        raw = element.get() if hasattr(element, "get") else str(element)
                        if raw and raw.startswith("<meta"):
                            match = re.search(r'content="([^"]+)"', raw)
                            if match:
                                return match.group(1)
                        return str(element).strip()
                except Exception:
                    continue
        return None

    def _extract_url(self, response) -> str:
        """Extract current page URL."""
        if hasattr(response, "url"):
            return response.url
        return ""

    def _extract_title(self, response) -> Optional[str]:
        """Extract article title."""
        title = self._extract_first(response, self.SELECTORS["title"])
        if title:
            return re.sub(r'<[^>]+>', '', title).strip()
        return None

    def _extract_author(self, response) -> Optional[str]:
        """Extract author name or derive from URL path."""
        author = self._extract_text(response, self.SELECTORS["author"])
        if author:
            return author
        if hasattr(response, "url"):
            url = response.url
            match = re.search(r'elaph\.com/[^/]+/([^/]+)/', url)
            if match:
                section = match.group(1)
                return section.capitalize()
        return None

    def _extract_published_date(self, response) -> Optional[str]:
        """Extract published date."""
        date_str = self._extract_first(response, self.SELECTORS["published_date"])
        if date_str:
            clean = re.sub(r'<[^>]+>', '', date_str).strip()
            return clean if clean else date_str
        if hasattr(response, "css"):
            for selector in self.SELECTORS["published_date"]:
                try:
                    elem = response.css(selector).get()
                    if elem and hasattr(elem, "attributes"):
                        if "datetime" in elem.attributes:
                            return elem.attributes["datetime"]
                except Exception:
                    continue
        return None

    def _extract_content(self, response) -> Optional[str]:
        """Extract main article content."""
        content_text = ""

        if hasattr(response, "css"):
            for selector in self.SELECTORS["content"]:
                try:
                    elements = response.css(selector).getall()
                    if elements:
                        for elem in elements:
                            raw_html = elem.get() if hasattr(elem, "get") else str(elem)
                            if raw_html:
                                text = self._strip_html(raw_html)
                                content_text += text + "\n"
                        if content_text.strip():
                            break
                except Exception:
                    continue

        content_text = self._clean_text(content_text)
        return content_text.strip() if content_text.strip() else None

    def _strip_html(self, html: str) -> str:
        """Strip HTML tags and decode entities."""
        if not html:
            return ""
        text = re.sub(r'<[^>]+>', ' ', html)
        text = re.sub(r'&nbsp;', ' ', text)
        text = re.sub(r'&amp;', '&', text)
        text = re.sub(r'&lt;', '<', text)
        text = re.sub(r'&gt;', '>', text)
        text = re.sub(r'&quot;', '"', text)
        text = re.sub(r'&#\d+;', '', text)
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    def _clean_text(self, text: str) -> str:
        """Clean extracted text."""
        if not text:
            return ""

        text = re.sub(r'\s+', ' ', text)
        text = re.sub(r'[\x00-\x08\x0b-\x0c\x0e-\x1f\x7f-\x9f]', '', text)

        lines = []
        for line in text.split('\n'):
            line = line.strip()
            if line:
                lines.append(line)

        return '\n'.join(lines)

    def _extract_category(self, response) -> Optional[str]:
        """Extract article category."""
        return self._extract_first(response, self.SELECTORS["category"])

    def _extract_tags(self, response) -> Optional[str]:
        """Extract article tags as comma-separated string."""
        tags = []

        if hasattr(response, "css"):
            for selector in self.SELECTORS["tags"]:
                try:
                    elements = response.css(selector).getall()
                    for elem in elements:
                        raw = elem.get() if hasattr(elem, "get") else str(elem)
                        if raw:
                            tag_text = self._strip_html(raw).strip()
                            if tag_text and len(tag_text) < 100:
                                tags.append(tag_text)
                except Exception:
                    continue

        return ", ".join(tags) if tags else None

    def _extract_images(self, response) -> Optional[str]:
        """Extract image URLs from content area."""
        images = []

        if hasattr(response, "css"):
            for selector in self.SELECTORS["images"]:
                try:
                    elements = response.css(selector).all()
                    for elem in elements:
                        if hasattr(elem, "attributes") and "src" in elem.attributes:
                            src = elem.attributes["src"]
                            if src and src.startswith("http"):
                                images.append(src)
                        elif hasattr(elem, "attributes") and "data-src" in elem.attributes:
                            src = elem.attributes["data-src"]
                            if src and src.startswith("http"):
                                images.append(src)
                except Exception:
                    continue

        return ",".join(images) if images else None

    def _extract_description(self, response) -> Optional[str]:
        """Extract meta description."""
        return self._extract_first(response, self.SELECTORS["description"])

    def compute_content_hash(self, content: str) -> str:
        """Compute MD5 hash of cleaned content for change detection."""
        if not content:
            return ""

        cleaned = self._clean_text(content)
        cleaned = re.sub(r'\s+', '', cleaned)

        return hashlib.md5(cleaned.encode('utf-8')).hexdigest()