Overview:
- Uses Playwright to scrape hackathon items and pymongo to store/upsert them.

Quickstart:
1. python -m venv .venv && source .venv/bin/activate
2. pip install -r requirements.txt
3. playwright install
4. copy .env.example to .env and set MONGO_URI etc.
5. python scraper/main.py --url "https://devpost.com/hackathons"

Notes:
- The scraper uses simple CSS selectors (defaults provided). If the site differs, pass selectors via CLI or edit scraper/scraper.py.
