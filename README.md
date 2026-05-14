# Elaph Content Pipeline

A production web crawling pipeline for extracting and indexing content from large Arabic news sites, with storage in Google BigQuery for AI ingestion.

## Features

- **Incremental Crawling** — Only processes new or changed URLs since last run
- **Full Re-crawl** — Reset and re-crawl entire site when needed
- **Resumable** — Picks up exactly where it left off if the process is interrupted
- **Auto Table Creation** — BigQuery table created automatically on first run
- **Batch Processing** — Accumulates pages and writes to BigQuery in configurable batches
- **Error Handling** — Graceful handling of failed URLs with retry logic
- **Time Limits** — Configurable limits for CI/CD environments

## Setup

### Prerequisites

- Python 3.11+
- Supabase project (for crawl state tracking)
- Google Cloud Platform project with BigQuery enabled
- GCP service account with BigQuery permissions

### Installation

```bash
pip install -r requirements.txt
```

### Configuration

```bash
cp .env.example .env
```

Edit `.env` and fill in your credentials. See `.env.example` for all required variables.

### Database Setup

Create the required Supabase tables using the provided schema before running the crawler.

## Usage

```bash
# Show crawl statistics
python elaph_crawler/pipeline.py --stats

# Check for new URLs (without crawling)
python elaph_crawler/pipeline.py --check-new

# Update sitemap state
python elaph_crawler/pipeline.py --update-sitemap

# Run incremental crawl
python elaph_crawler/pipeline.py --mode incremental

# Run full re-crawl
python elaph_crawler/pipeline.py --mode full

# Dry run (no BigQuery insert)
python elaph_crawler/pipeline.py --mode incremental --dry-run

# Limit to N URLs
python elaph_crawler/pipeline.py --max-urls 100

# Time limit in seconds
python elaph_crawler/pipeline.py --time-limit 18000
```

## Architecture

```
elaph_crawler/
├── __init__.py
├── pipeline.py              # Main CLI orchestrator
├── core/
│   ├── supabase_manager.py  # Crawl state tracking
│   ├── sitemap_parser.py    # Sitemap discovery & parsing
│   ├── article_scraper.py   # Content extraction
│   └── bigquery_manager.py  # BigQuery storage
└── utils/
    ├── config.py            # Configuration loader
    └── logger.py            # Logging setup
```

## GitHub Actions

Two workflows are included for automated crawling:

- **Main Crawl** — Manual trigger, self-chains until all URLs are processed (5.5h limit per run)
- **Daily Incremental** — Scheduled daily at midnight UTC

### Required Secrets

Configure these in your GitHub repository settings under **Settings → Secrets and variables → Actions**:

| Secret | Description |
|--------|-------------|
| `SUPABASE_URL` | Supabase project URL |
| `SUPABASE_KEY` | Supabase anonymous key |
| `GCP_PROJECT_ID` | Google Cloud project ID |
| `GCP_SERVICE_ACCOUNT_JSON` | Full JSON content of GCP service account key |

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `BATCH_SIZE` | 5 | Pages per BigQuery row |
| `REQUEST_DELAY` | 1.0 | Seconds between requests |
| `MAX_RETRIES` | 3 | HTTP retry attempts |
| `MAX_URL_FAILURES` | 5 | Errors before permanent skip |

## License

MIT License