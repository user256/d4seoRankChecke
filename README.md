# DataForSEO Rank Checker

A Python tool for bulk fetching Search Engine Results Page (SERP) data and search volume metrics from the DataForSEO API. This tool uses async/await for efficient concurrent API calls and stores results in a SQLite database.

## Features

- **Bulk SERP Data Fetching**: Submit and retrieve organic search results for multiple keywords
- **Search Volume Data**: Fetch search volume metrics for keywords
- **Async Processing**: Efficient concurrent API calls with rate limiting
- **SQLite Storage**: Local database for storing queries, tasks, and results
- **Simulator Mode**: Test the tool using DataForSEO's sandbox environment
- **Flexible Configuration**: Per-query overrides for language, location, search engine domain, and device

## Requirements

- Python 3.7+
- DataForSEO API credentials

## Installation

1. Clone the repository:
```bash
git clone git@github.com:user256/d4seoRankChecker.git
cd d4seoRankChecker
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Configuration

1. Create a `config.ini` file (see `config.ini.example` for reference):
```ini
[general]
simulator = false

[api]
username = your_username@example.com
password = your_password
language_code = EN
location_name = New York,New York,United States
se_domain = google.com
device = desktop

[files]
csv_path = queries.csv
db_path = serp_tasks.db
```

2. Create a `queries.csv` file with your keywords. Supported columns:
   - `keyword` (required) - The search query
   - `language_code` (optional) - Language code (e.g., EN, FR, DE)
   - `location_name` (optional) - Location name (e.g., "New York,New York,United States")
   - `se_domain` (optional) - Search engine domain (e.g., google.com)
   - `device` (optional) - Device type: desktop or mobile

Example `queries.csv`:
```csv
keyword,language_code,location_name,device
python3 tutorial,EN,New York,New York,United States,desktop
machine learning,EN,London,England,United Kingdom,desktop
```

## Usage

### Basic Usage

Run the full pipeline (import, submit, fetch):
```bash
python3 dataforseo.py
```

### Mode Options

- `all` (default): Import queries, submit tasks, and fetch results
- `import`: Only import queries from CSV
- `submit`: Only submit tasks for queries without tasks
- `fetch`: Only fetch results for pending tasks

Examples:
```bash
# Import queries only
python3 dataforseo.py --mode import

# Submit tasks only
python3 dataforseo.py --mode submit

# Fetch results only
python3 dataforseo.py --mode fetch
```

### Additional Options

- `--config PATH`: Specify custom config file (default: config.ini)
- `--csv PATH`: Override CSV path from config
- `--db PATH`: Override database path from config
- `--sv`: Enable search volume tasks (enabled by default)

By default, the tool uses the SQLite database path defined in
config.ini. Use the –db flag to point to a different SQLite file. If the file does
not exist, it will be created automatically.

```bash
python3 dataforseo.py --db my_project.db
```

### Simulator Mode

To test without using API credits, enable simulator mode in `config.ini`:
```ini
[general]
simulator = true
```

This uses DataForSEO's sandbox environment and generates simulated results.

## Database Schema

The tool creates the following tables:
- `queries`: Stores keyword queries with metadata
- `jobs`: Tracks job runs
- `tasks`: SERP tasks linked to queries
- `results`: SERP result data (JSON)
- `sv_tasks`: Search volume tasks
- `sv_results`: Search volume result data (JSON)

A view `organic_query_ranks` is created for easy querying of ranking data.

## Rate Limits

The tool respects DataForSEO API rate limits:
- `/task_post`: 2000 calls/minute (100 tasks per call)
- `/tasks_ready`: 20 calls/minute
- `/task_get/{id}`: 2000 calls/minute

Rate limiting is handled automatically with exponential backoff.

## License
GPL-3.0
