# kleinanzeigen-scraper

Python project for scraping listings from kleinanzeigen.de using Playwright, SQLite, SQLAlchemy, and APScheduler.

## Stack

- Python 3.12+
- Playwright + playwright-stealth
- SQLite
- SQLAlchemy (sync)
- APScheduler
- PyYAML
- Flask dashboard

## Project structure

```text
kleinanzeigen-scraper/
├── config.yaml
├── dashboard/
│   ├── app.py
│   └── templates/
│       └── index.html
├── main.py (legacy)
├── requirements.txt
├── scraper/
│   ├── __init__.py
│   └── browser.py
├── storage/
│   ├── __init__.py
│   └── database.py
└── scheduler/
    ├── __init__.py
    └── jobs.py
```

## Installation

1. Create and activate a virtual environment.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Install Playwright browser:

```bash
playwright install chromium
```

## Configuration

Edit `config.yaml` and add your search list:

```yaml
searches:
  - name: "iPhone 15 Erfurt"
    url: "https://www.kleinanzeigen.de/s-iphone-15/k0"
  - name: "RTX 4070 Thueringen"
    url: "https://www.kleinanzeigen.de/s-rtx-4070/k0"

scheduler:
  interval_minutes: 60

scraper:
  min_delay_seconds: 2
  max_delay_seconds: 6
  max_pages: 5
  headless: true
```

Important anti-ban constraints:

- `scheduler.interval_minutes` must be at least 30.
- Requests are sequential only (no parallel scraping jobs).
- Random delay is applied between page transitions.

## Run

Single entrypoint (scraper + scheduler + dashboard in one process):

```bash
cd dashboard && python app.py
```

Open in browser:

- http://localhost:5000

At startup, the app initializes DB schema, loads runtime settings from SQLite,
starts APScheduler in background mode, registers active query jobs, and exposes
management APIs/UI for queries, settings, logs, and manual scrape runs.
