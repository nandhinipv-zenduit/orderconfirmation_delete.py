#!/usr/bin/env python3
"""
Delete 1NCE "Orderconfirmation.pdf" files from Zoho Inventory > Documents > Inbox
for Go Fleet Corporation (org 673162904). Nothing else is touched.

A file is deleted only if ALL of these are true:
  - it is in the Documents Inbox
  - its name is exactly "Orderconfirmation.pdf" (case-insensitive)
  - it was emailed in from noreply@1nce.com
  - it is not attached to any transaction

How it stays fast and under Zoho's ~100 calls/minute limit:
  - Deletes in BULK, up to 50 ids per call (DELETE /documents?document_ids=...).
    The first batch is a probe: if Zoho rejects bulk delete, or the files are
    still there afterwards, it falls back to one-by-one automatically.
  - Every call goes through a pacer (default 80 calls/minute).
  - If Zoho blocks for rate limiting anyway, it waits and retries.

Non-interactive, so it runs in GitHub Actions. Settings via environment:
  ZOHO_CLIENT_ID, ZOHO_CLIENT_SECRET, ZOHO_REFRESH_TOKEN   (required)
  ZOHO_DC            us (default), eu, in, au, jp, ca, sa, cn
  DRY_RUN            "true" (default) = scan and report only; "false" = delete
  MAX_DELETE         optional cap per run, 0 = no cap

Requires:  pip install requests
"""

import csv
import os
import re
import sys
import time
from collections import deque
from datetime import datetime

import requests

# --------------------------------------------------------------------------
# Settings
# --------------------------------------------------------------------------
ORG_ID = "673162904"
ORG_NAME_EXPECTED = "Go Fleet Corporation"
TARGET_FILENAME = "orderconfirmation.pdf"   # exact name, case-insensitive
SENDER = "noreply@1nce.com"                 # exact sender, case-insensitive
SEARCH_TEXT = "orderconfirmation"           # server-side filter to shrink the scan

DRY_RUN = os.getenv("DRY_RUN", "true").strip().lower() != "false"
MAX_DELETE = int(os.getenv("MAX_DELETE", "0") or 0)
CALLS_PER_MINUTE = 80        # Zoho allows ~100 per org per minute
BULK_SIZE = 50               # Zoho's maximum ids per bulk delete for documents
RATE_LIMIT_WAIT = 120        # seconds to wait when Zoho blocks us
PER_PAGE = 200

ACCOUNTS_HOSTS = {
    "us": "https://accounts.zoho.com", "eu": "https://accounts.zoho.eu",
    "in": "https://accounts.zoho.in", "au": "https://accounts.zoho.com.au",
    "jp": "https://accounts.zoho.jp", "ca": "https://accounts.zohocloud.ca",
    "sa": "https://accounts.zoho.sa", "cn": "https://accounts.zoho.com.cn",
}
DATA_CENTRE = os.getenv("ZOHO_DC", "us")
API_PATH = "/inventory/v1"


def norm(value):
    return str(value or "").strip().lower()


def is_attached(doc):
    return bool(doc.get("transactions")) \
        or bool(norm(doc.get("document_entity_ids"))) \
        or bool(doc.get("has_more_entity")) \
        or bool(norm(doc.get("entity_number")))


def is_rate_limited(status, text):
    low = text.lower()
    return status == 429 or "maximum number of requests" in low \
        or ("rate" in low and "limit" in low) or "blocked for some time" in low


def is_daily_limit(text):
    low = text.lower()
    return "daily" in low or "per day" in low


class DailyLimitHit(Exception):
    pass


class Pacer:
    """Keeps all calls under CALLS_PER_MINUTE in any rolling 60 seconds."""

    def __init__(self, per_minute):
        self.per_minute = per_minute
        self.stamps = deque()

    def wait(self):
        while True:
            now = time.time()
            while self.stamps and now - self.stamps[0] > 60:
                self.stamps.popleft()
            if len(self.stamps) < self.per_minute:
                self.stamps.append(now)
                return
            time.sleep(60 - (now - self.stamps[0]) + 0.05)


class Zoho:
    def __init__(self):
        self.client_id = os.environ["ZOHO_CLIENT_ID"]
        self.client_secret = os.environ["ZOHO_CLIENT_SECRET"]
        self.refresh_token = os.environ["ZOHO_REFRESH_TOKEN"]
        self.accounts_host = ACCOUNTS_HOSTS[DATA_CENTRE]
        self.api_host = "https://www.zohoapis.com"
        self.session = requests.Session()
        self.pacer = Pacer(CALLS_PER_MINUTE)
        self.token, self.expiry = None, 0.0
        self.calls = 0

    def access_token(self):
        if self.token and time.time() < self.expiry - 60:
            return self.token
        r = requests.post(f"{self.accounts_host}/oauth/v2/token", data={
            "grant_type": "refresh_token", "client_id": self.client_id,
            "client_secret": self.client_secret, "refresh_token": self.refresh_token,
        }, timeout=30)
        body = r.json()
        if "access_token" not in body:
            sys.exit(f"Zoho login failed: {body.get('error', body)}. Check credentials / ZOHO_DC.")
        self.token = body["access_token"]
        self.expiry = time.time() + int(body.get("expires_in", 3600))
        if body.get("api_domain"):
            self.api_host = body["api_domain"].rstrip("/")
        return self.token

    def request(self, method, path, params=None):
        params = dict(params or {}, organization_id=ORG_ID)
        for attempt in range(1, 8):
            self.pacer.wait()
            headers = {"Authorization": f"Zoho-oauthtoken {self.access_token()}"}
            try:
                r = self.session.request(method, f"{self.api_host}{API_PATH}{path}",
                                         headers=headers, params=params, timeout=60)
            except requests.RequestException:
                time.sleep(2 ** attempt)
                continue
            self.calls += 1
            text = r.text[:500]
            if r.status_code == 401:
                self.token = None
                continue
            if is_rate_limited(r.status_code, text):
                if is_daily_limit(text):
                    raise DailyLimitHit(text[:200])
                print(f"  Zoho rate limit hit; waiting {RATE_LIMIT_WAIT}s (attempt {attempt}/7)")
                time.sleep(RATE_LIMIT_WAIT)
                continue
            return r
        sys.exit(f"Giving up on {method} {path}: repeated rate limits or errors.")

    @staticmethod
    def body(r):
        try:
            return r.json()
        except ValueError:
            return {"code": -1, "message": r.text[:200]}

    def verify_org(self):
        orgs = self.body(self.request("GET", "/organizations")).get("organizations", [])
        name = next((o.get("name", "").strip() for o in orgs
                     if str(o.get("organization_id")) == ORG_ID), "")
        if name != ORG_NAME_EXPECTED:
            sys.exit(f"ABORT: org {ORG_ID} is '{name}', expected '{ORG_NAME_EXPECTED}'. Nothing touched.")
        return name

    def list_page(self, page):
        r = self.request("GET", "/documents", {
            "view": "inbox", "search_text": SEARCH_TEXT, "sort_column": "created_time",
            "page": page, "per_page": PER_PAGE,
        })
        b = self.body(r)
        if b.get("code") != 0:
            sys.exit(f"Listing page {page} failed: {b.get('message')}. Nothing deleted.")
        return b.get("documents", []), bool(b.get("page_context", {}).get("has_more_page"))

    def delete_many(self, ids):
        r = self.request("DELETE", "/documents", {"document_ids": ",".join(ids)})
        b = self.body(r)
        return r.status_code in (200, 204) and b.get("code", 0) == 0, r.status_code, str(b.get("message", ""))

    def delete_one(self, doc_id):
        r = self.request("DELETE", f"/documents/{doc_id}")
        b = self.body(r)
        if r.status_code == 404:
            return True, 404, "already gone"
        return r.status_code in (200, 204) and b.get("code", 0) == 0, r.status_code, str(b.get("message", ""))

    def still_exists(self, doc_id):
        return self.request("GET", f"/documents/{doc_id}").status_code != 404


# --------------------------------------------------------------------------
def scan(zoho):
    ids, skipped, seen, page = [], 0, set(), 1
    while True:
        docs, more = zoho.list_page(page)
        for d in docs:
            did = d.get("document_id")
            if did in seen:
                continue
            seen.add(did)
            if norm(d.get("file_name")) != TARGET_FILENAME or norm(d.get("from_address")) != SENDER:
                continue
            if is_attached(d):
                skipped += 1
            else:
                ids.append(did)
        if not docs or not more:
            return ids, skipped
        page += 1


def delete_all(zoho, ids, log):
    deleted = failed = 0
    size = BULK_SIZE
    bulk = None                      # None = not probed yet

    def record(batch, action, code, msg):
        for i in batch:
            log.writerow([i, action, code, msg])

    pos = 0
    while pos < len(ids):
        batch = ids[pos:pos + size]

        if bulk is None:             # probe bulk delete once
            ok, code, msg = zoho.delete_many(batch)
            if ok and not zoho.still_exists(batch[0]) and not zoho.still_exists(batch[-1]):
                bulk = True
                print(f"Bulk delete works: up to {size} files per call.")
                record(batch, "deleted_bulk", code, msg)
                deleted += len(batch)
                pos += len(batch)
                continue
            m = re.search(r"maximum of (\d+)", msg.lower())
            if code == 400 and m and 0 < int(m.group(1)) < size:
                size = int(m.group(1))
                print(f"Zoho limits bulk delete to {size} per call; adjusting.")
                continue
            bulk = False
            print(f"Bulk delete not available ({code} {msg[:80]}). Falling back to one-by-one "
                  f"(about {CALLS_PER_MINUTE} files/minute).")
            continue

        if bulk:
            ok, code, msg = zoho.delete_many(batch)
            if ok:
                record(batch, "deleted_bulk", code, msg)
                deleted += len(batch)
                pos += len(batch)
            else:                    # one bad id can sink a batch: do this batch singly
                for i in batch:
                    ok1, c1, m1 = zoho.delete_one(i)
                    record([i], "deleted" if ok1 else "failed", c1, m1)
                    deleted += ok1
                    failed += not ok1
                pos += len(batch)
        else:
            i = ids[pos]
            ok, code, msg = zoho.delete_one(i)
            record([i], "deleted" if ok else "failed", code, msg)
            deleted += ok
            failed += not ok
            pos += 1

        if pos and pos % 500 < (size if bulk else 1):
            print(f"  Progress: {pos}/{len(ids)} processed, {deleted} deleted, {failed} failed")
    return deleted, failed


def main():
    for var in ("ZOHO_CLIENT_ID", "ZOHO_CLIENT_SECRET", "ZOHO_REFRESH_TOKEN"):
        if not os.getenv(var):
            sys.exit(f"Missing environment variable {var}.")
    if DATA_CENTRE not in ACCOUNTS_HOSTS:
        sys.exit(f"ZOHO_DC must be one of: {', '.join(ACCOUNTS_HOSTS)}")

    zoho = Zoho()
    org = zoho.verify_org()
    print(f"Organisation : {org} ({ORG_ID})")
    print(f"Deleting     : Inbox files named '{TARGET_FILENAME}' from {SENDER}, not attached")
    print(f"Mode         : {'DRY RUN' if DRY_RUN else 'DELETE'}")

    start = time.time()
    try:
        ids, skipped = scan(zoho)
    except DailyLimitHit as e:
        sys.exit(f"Zoho's daily API limit was reached during the scan ({e}). Try again tomorrow.")
    print(f"Scan done: {len(ids)} to delete, {skipped} attached to transactions (kept).")
    if MAX_DELETE:
        ids = ids[:MAX_DELETE]
    if not ids:
        print("Nothing to delete.")
        return
    if DRY_RUN:
        print(f"DRY RUN: {len(ids)} files would be deleted. Set DRY_RUN=false to delete.")
        return

    log_path = f"deleted_{datetime.now():%Y%m%d_%H%M%S}.csv"
    with open(log_path, "w", newline="", encoding="utf-8") as fh:
        log = csv.writer(fh)
        log.writerow(["document_id", "action", "status_code", "message"])
        try:
            deleted, failed = delete_all(zoho, ids, log)
        except DailyLimitHit as e:
            sys.exit(f"Zoho's daily API limit was reached ({e}). The next run will continue. Log: {log_path}")

    print(f"Finished in {(time.time() - start) / 60:.1f} min. Deleted {deleted}, failed {failed}, "
          f"API calls {zoho.calls}. Log: {log_path}")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
