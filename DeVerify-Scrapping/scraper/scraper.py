# scraper/bulk_scraper.py
import time
import re
from datetime import datetime
from urllib.parse import urljoin, urlparse
from typing import List, Dict, Optional
import os

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from scraper.db import upsert_hackathon

# --- config ---
LISTING_URL_DEFAULT = "https://devpost.com/hackathons"
USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 DeVerifyBot/1.0"
REQUEST_DELAY = 1.0        # seconds between requests (politeness)
PAGE_TIMEOUT_MS = 20000    # 20s wait timeout

# --- helpers ---
def slug_from_url(url: str) -> str:
    path = urlparse(url).path.rstrip("/")
    if not path or path == "/":
        return urlparse(url).netloc.replace(".", "-")
    return path.split("/")[-1]

def normalize_url(base: str, href: str) -> str:
    return urljoin(base, href)

# --- scraping functions ---
def extract_listing_items(page) -> List[Dict[str, str]]:
    """
    Focused extractor for Devpost listing:
    - finds h3[data-v-64e017b4] nodes (title)
    - finds the nearest anchor to get the hackathon URL
    - finds a nearby div.submission-period inside the same card (if any)
    Returns list of dicts: { title, url, submission_period, begins_iso, ends_iso }
    """
    items = []
    try:
        h3_nodes = page.query_selector_all("h3[data-v-64e017b4]")
    except Exception:
        h3_nodes = []

    seen = set()
    for h3 in h3_nodes:
        try:
            title = (h3.text_content() or "").strip()
        except Exception:
            title = ""
        # find anchor: try child anchor, then walk up to find an ancestor anchor or parent that contains an <a>
        href = None
        try:
            a_el = h3.query_selector("a")
            if a_el:
                href = a_el.get_attribute("href")
            else:
                # walk up parents to find anchor or element with an anchor
                ancestor = h3
                for _ in range(4):
                    try:
                        parent = ancestor.evaluate_handle("el => el.parentElement").as_element()
                    except Exception:
                        parent = None
                    if not parent:
                        break
                    anchor = parent.query_selector("a")
                    if anchor:
                        href = anchor.get_attribute("href")
                        break
                    ancestor = parent
        except Exception:
            href = None

        if not href:
            # last-resort: look for any anchor in the h3 node's subtree
            try:
                maybe_a = h3.query_selector("a[href]")
                if maybe_a:
                    href = maybe_a.get_attribute("href")
            except Exception:
                href = None

        if not href:
            continue

        # normalize url
        try:
            if href.startswith("//"):
                href = "https:" + href
            url = normalize_url(page.url, href)
        except Exception:
            url = href

        if not url or url in seen:
            continue
        seen.add(url)

        # try to find submission-period within the nearest card/ancestor
        submission_text = None
        begins_iso = None
        ends_iso = None
        try:
            # walk up from h3 to find an ancestor element that contains a div.submission-period
            ancestor = h3
            found_sp = None
            for _ in range(6):
                try:
                    parent = ancestor.evaluate_handle("el => el.parentElement").as_element()
                except Exception:
                    parent = None
                if not parent:
                    break
                try:
                    sp = parent.query_selector("div.submission-period")
                except Exception:
                    sp = None
                if sp:
                    found_sp = sp
                    break
                ancestor = parent
            if found_sp:
                submission_text = (found_sp.text_content() or "").strip()
                # extract data-iso-date values if present (first/second)
                try:
                    dates = found_sp.query_selector_all("[data-iso-date]")
                    if dates and len(dates) >= 1:
                        begins_iso = dates[0].get_attribute("data-iso-date")
                    if dates and len(dates) >= 2:
                        ends_iso = dates[1].get_attribute("data-iso-date")
                except Exception:
                    pass
        except Exception:
            pass

        items.append({
            "title": title,
            "url": url,
            "submission_period": submission_text,
            "begins_iso": begins_iso,
            "ends_iso": ends_iso,
        })

    return items

def extract_submission_period_from_listing_card(card) -> Optional[Dict]:
    """
    Given a card element handle, try to extract submission period info from
    table/cells like: <td data-iso-date="..."> or structured cells.
    Returns dict with keys: submission_period, begins_iso, ends_iso (values can be None).
    """
    submission_period = None
    begins_iso = None
    ends_iso = None
    try:
        # Prefer explicit td[data-iso-date] nodes if present
        try:
            td_nodes = card.query_selector_all("td[data-iso-date]")
            if td_nodes:
                # If there are two date tds, treat first as begin, second as end
                if len(td_nodes) >= 1:
                    begins_iso = td_nodes[0].get_attribute("data-iso-date")
                if len(td_nodes) >= 2:
                    ends_iso = td_nodes[1].get_attribute("data-iso-date")
                # build a human readable submission_period if text available
                texts = [(n.text_content() or "").strip() for n in td_nodes]
                submission_period = " — ".join([t for t in texts if t])
                return {"submission_period": submission_period, "begins_iso": begins_iso, "ends_iso": ends_iso}
        except Exception:
            pass

        # Fallback: check first three <td> cells and detect 'submissions' label
        try:
            tds = card.query_selector_all("td")
            if tds and len(tds) >= 3:
                period_label = (tds[0].text_content() or "").strip().lower()
                if "submissions" in period_label:
                    begins_iso = tds[1].get_attribute("data-iso-date") if tds[1] else None
                    ends_iso = tds[2].get_attribute("data-iso-date") if tds[2] else None
                    submission_period = (tds[1].text_content() or "").strip()
                    if tds[2]:
                        submission_period += " — " + (tds[2].text_content() or "").strip()
                    return {"submission_period": submission_period, "begins_iso": begins_iso, "ends_iso": ends_iso}
        except Exception:
            pass

    except Exception:
        pass

    return {"submission_period": submission_period, "begins_iso": begins_iso, "ends_iso": ends_iso}

# small helper to set status from ISO dates
def determine_status_from_iso(start_iso: Optional[str], end_iso: Optional[str]) -> str:
    try:
        now = datetime.utcnow()
        if start_iso:
            s = datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
        else:
            s = None
        if end_iso:
            e = datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
        else:
            e = None
        if s and now < s:
            return "upcoming"
        if s and e and s <= now <= e:
            return "running"
        if e and now > e:
            return "ended"
    except Exception:
        pass
    return "upcoming"

# --- new: DB diagnostic helper ---
def diagnose_db():
    """
    Try to connect to MongoDB using MONGO_URI env var and print info about
    the configured DB/collection and a small sample of documents.
    """
    uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    db_name = os.getenv("MONGO_DB", "hackathons")
    coll_name = os.getenv("MONGO_COLLECTION", "hack-info")
    print("DB diagnostic:")
    print("  MONGO_URI:", uri or "(not set)")
    print("  MONGO_DB:", db_name)
    print("  MONGO_COLLECTION:", coll_name)
    if not uri:
        print("  → MONGO_URI is not set; nothing to connect to.")
        return
    try:
        import pymongo
    except Exception as e:
        print("  → pymongo not available:", e)
        return
    try:
        client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
    except Exception as e:
        print("  → Failed to connect/ping MongoDB:", e)
        return
    try:
        db = client[db_name]
        coll = db[coll_name]
        count = coll.count_documents({})
        print(f"  → Connected. Collection '{coll_name}' has {count} documents.")
        sample = list(coll.find().limit(3))
        print("  → Sample documents (up to 3):")
        for s in sample:
            # avoid printing entire nested objects
            print("    ", {k: s.get(k) for k in ("id", "name", "submission_period", "status", "_id")})
    except Exception as e:
        print("  → Error reading collection:", e)
    finally:
        try:
            client.close()
        except Exception:
            pass

# --- orchestrator ---
def scrape_all_from_listing(listing_url: str = LISTING_URL_DEFAULT, limit: int = 0):
    """
    Scrape the Devpost listing page, optionally follow each item to enrich data,
    and upsert results into MongoDB via upsert_hackathon.
    If limit > 0, only process that many items.
    """
    results = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=USER_AGENT)
        page = context.new_page()
        try:
            page.goto(listing_url, timeout=PAGE_TIMEOUT_MS)
            try:
                page.wait_for_selector("h3[data-v-64e017b4]", timeout=10000)
            except Exception:
                pass

            items = extract_listing_items(page)
            if limit and isinstance(limit, int) and limit > 0:
                items = items[:limit]

            print(f"Found {len(items)} items on listing page.")

            for idx, it in enumerate(items, start=1):
                try:
                    # follow item url to enrich
                    item_url = it.get("url")
                    name = it.get("title") or ""
                    begins_iso = it.get("begins_iso")
                    ends_iso = it.get("ends_iso")

                    # open details page to attempt to discover canonical title or more precise dates
                    if item_url:
                        detail_page = context.new_page()
                        try:
                            detail_page.goto(item_url, timeout=PAGE_TIMEOUT_MS)
                            # try to get <h1>
                            try:
                                h1 = detail_page.query_selector("h1")
                                if h1:
                                    h1_text = (h1.text_content() or "").strip()
                                    if h1_text:
                                        name = h1_text
                            except Exception:
                                pass
                            # try to find td[data-iso-date] on details page
                            try:
                                date_tds = detail_page.query_selector_all("td[data-iso-date]")
                                if date_tds and len(date_tds) >= 1:
                                    begins_iso = date_tds[0].get_attribute("data-iso-date") or begins_iso
                                if date_tds and len(date_tds) >= 2:
                                    ends_iso = date_tds[1].get_attribute("data-iso-date") or ends_iso
                            except Exception:
                                pass
                        except PlaywrightTimeoutError:
                            pass
                        finally:
                            try:
                                detail_page.close()
                            except Exception:
                                pass

                    # determine submission_period: prefer listing text, fallback to ISO dates
                    submission_period = it.get("submission_period")
                    if not submission_period:
                        parts = []
                        if begins_iso:
                            parts.append(begins_iso)
                        if ends_iso:
                            parts.append(ends_iso)
                        submission_period = " — ".join(parts) if parts else None

                    status = determine_status_from_iso(begins_iso, ends_iso)
                    doc = {
                        "id": slug_from_url(item_url) if item_url else name,
                        "name": name,
                        "submission_period": submission_period,
                        "status": status,
                    }
                    # upsert to mongodb
                    try:
                        print(f"Upserting doc id={doc.get('id')} to DB {os.getenv('MONGO_DB','hackathons')}.{os.getenv('MONGO_COLLECTION','hack-info')}")
                        upsert_hackathon(doc)
                        print(f"Upsert attempted for id={doc.get('id')}")
                    except Exception as e:
                        print(f"Warning: upsert failed for {doc.get('id')}: {e}")

                    results.append(doc)
                    # politeness delay
                    time.sleep(REQUEST_DELAY)
                except Exception as e:
                    print(f"Error processing item {idx}: {e}")
        finally:
            try:
                page.close()
            except Exception:
                pass
            try:
                context.close()
            except Exception:
                pass
            try:
                browser.close()
            except Exception:
                pass

    return results

# Add CLI entrypoint so module can be run with python -m scraper.scraper
def main():
    global REQUEST_DELAY
    import argparse
    parser = argparse.ArgumentParser(description="Bulk scrape Devpost listing and upsert hackathons to MongoDB")
    parser.add_argument("--listing-url", help="Listing URL to scrape", default=LISTING_URL_DEFAULT)
    parser.add_argument("--limit", type=int, help="Max items to process (0 = no limit)", default=0)
    parser.add_argument("--delay", type=float, help="Politeness delay between requests (seconds)", default=REQUEST_DELAY)
    parser.add_argument("--diagnose-db", action="store_true", help="Run MongoDB connection diagnostic and exit")
    parser.add_argument("--mongo-uri", help="Override MONGO_URI (manual set)", default=None)
    args = parser.parse_args()

    # allow manual override of MONGO_URI before any DB ops
    if args.mongo_uri:
        try:
            import scraper.db as _db
            _db.set_mongo_uri(args.mongo_uri)
            print("MONGO_URI overridden at runtime.")
        except Exception as e:
            print("Failed to set MONGO_URI at runtime:", e)

    # run DB diagnostic if requested
    if args.diagnose_db:
        diagnose_db()
        return

    # apply CLI overrides
    REQUEST_DELAY = float(args.delay) if args.delay is not None else REQUEST_DELAY

    # run the scraping job
    try:
        print(f"Starting scrape of listing: {args.listing_url} (limit={args.limit or 'no limit'})")
        results = scrape_all_from_listing(listing_url=args.listing_url, limit=args.limit)
        print(f"Done. Processed {len(results)} items.")
    except Exception as e:
        print("Error running scraper:", e)
        raise

if __name__ == "__main__":
    main()
