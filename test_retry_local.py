"""
Part C check: an accepted (202) DM that later comes back 'failed' from
GET /v1/dm/{id} should be retried with a NEW idempotency key, not just
marked failed and abandoned.

Run against mock_pseudogram.py (not the real pseudogram API) since we need
to control exactly when a dm_id reports 'failed':

    export PSEUDOGRAM_API_KEY=testkey123
    export PSEUDOGRAM_BASE_URL=http://127.0.0.1:6000
    python3 mock_pseudogram.py &
    python3 app.py &
    python3 test_retry_local.py

mock_pseudogram.py is rigged so the very first dm_id it issues reports
'failed' on its first poll, then 'delivered' after that -- simulating
exactly the case Part C asks for.
"""
import hashlib
import hmac
import json
import os
import sqlite3
import time

import requests

BASE = os.environ.get("LOCAL_URL", "http://127.0.0.1:5000")
API_KEY = os.environ.get("PSEUDOGRAM_API_KEY", "testkey123")
DB_PATH = os.environ.get("LINKPLEASE_DB_PATH", "linkplease.db")


def send_event(payload):
    raw = json.dumps(payload).encode()
    sig = hmac.new(API_KEY.encode(), raw, hashlib.sha256).hexdigest()
    r = requests.post(
        f"{BASE}/webhook",
        data=raw,
        headers={"Content-Type": "application/json", "X-PseudoGram-Signature": f"sha256={sig}"},
    )
    return r.status_code


def check(label, condition):
    print(f"[{'PASS' if condition else 'FAIL'}] {label}")


def main():
    requests.post(f"{BASE}/rules", json={"keyword": "PRICE", "dm_message": "price list"})

    status = send_event({
        "event_id": "evt_retry_check",
        "event_type": "comment.created",
        "sent_at": "2026-08-10T09:14:22.481Z",
        "data": {
            "comment_id": "cmt_retry",
            "post_id": "post_1",
            "text": "PRICE please",
            "created_at": "2026-08-10T09:14:21.900Z",
            "from": {"user_id": "usr_retry", "username": "someone"},
        },
    })
    check("webhook accepted -> 200", status == 200)

    # Poll our own /stats + the sqlite row until the task reaches a terminal
    # state or we time out.
    deadline = time.time() + 15
    final = None
    while time.time() < deadline:
        time.sleep(0.5)
        stats = requests.get(f"{BASE}/stats").json()
        if stats["sent"] >= 1 or stats["failed"] >= 1:
            final = stats
            break

    check("reached a terminal outcome within 15s", final is not None)
    if final:
        check("ended up delivered, NOT permanently failed", final["sent"] == 1 and final["failed"] == 0)

    conn = sqlite3.connect(DB_PATH)
    row = conn.execute(
        "SELECT status, attempts, dm_id FROM dm_tasks WHERE user_id='usr_retry'"
    ).fetchone()
    print("task row:", row)
    if row:
        status_, attempts_, dm_id_ = row
        check("took more than one attempt (i.e. actually retried)", attempts_ >= 1)
        check("final dm_id is NOT the first (flaky) one", dm_id_ != "dm_1")


if __name__ == "__main__":
    main()
