import os
import argparse
# Attempt to import python-dotenv dynamically to avoid static import errors in environments
# where it's not installed (e.g., linters or CI). If not available, provide a no-op.
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    def load_dotenv():
        return None
from .scraper import run_scrape
from .db import upsert_hackathon
from .models import HackathonItem

load_dotenv()

def main():
    parser = argparse.ArgumentParser(description="Scrape hackathons and save to MongoDB")
    parser.add_argument("--url", help="Target URL", required=False)
    parser.add_argument("--list-selector", help="CSS selector for item container", default=None)
    parser.add_argument("--name-selector", help="CSS selector for name inside item", default=None)
    parser.add_argument("--start-selector", help="CSS selector for start date", default=None)
    parser.add_argument("--end-selector", help="CSS selector for end date", default=None)
    parser.add_argument("--tag-selector", help="CSS selector for tags", default=None)
    args = parser.parse_args()
    url = args.url or os.getenv("DEFAULT_TARGET_URL")
    if not url:
        raise SystemExit("No target URL provided. Use --url or set DEFAULT_TARGET_URL in .env")
    selectors = {}
    if args.list_selector: selectors["list_selector"] = args.list_selector
    if args.name_selector: selectors["name_sel"] = args.name_selector
    if args.start_selector: selectors["start_sel"] = args.start_selector
    if args.end_selector: selectors["end_sel"] = args.end_selector
    if args.tag_selector: selectors["tag_sel"] = args.tag_selector

    items = run_scrape(url, selectors)
    for it in items:
        if isinstance(it, HackathonItem):
            upsert_hackathon(it.to_dict())
    print(f"Upserted {len(items)} items into MongoDB")

if __name__ == "__main__":
    main()
