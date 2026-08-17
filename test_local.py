"""
Quick local smoke test. Run `python app.py` (with PSEUDOGRAM_API_KEY set) in one
terminal, then run this script in another. It does not hit the real pseudogram
API — it only exercises your /webhook, /rules and /stats endpoints and checks
that the dedup/signature/deletion logic behaves.

Set LOCAL_URL and LOCAL_API_KEY to match how you started app.py.
"""

import hashlib
import hmac
import json
import os
import time

import requests

BASE = os.environ.get("LOCAL_URL", "http://127.0.0.1:5000")
API_KEY = os.environ.get("PSEUDOGRAM_API_KEY", "testkey123")


def send_event(payload):
    raw = json.dumps(payload).encode()
    sig = hmac.new(API_KEY.encode(), raw, hashlib.sha256).hexdigest()
    r = requests.post(
        f"{BASE}/webhook",
        data=raw,
        headers={"Content-Type": "application/json", "X-PseudoGram-Signature": f"sha256={sig}"},
    )
    return r.status_code, r.text


def created_event(event_id, text, comment_id, user_id):
    return {
        "event_id": event_id,
        "event_type": "comment.created",
        "sent_at": "2026-08-10T09:14:22.481Z",
        "data": {
            "comment_id": comment_id,
            "post_id": "post_1",
            "text": text,
            "created_at": "2026-08-10T09:14:21.900Z",
            "from": {"user_id": user_id, "username": "someone"},
        },
    }


def deleted_event(event_id, comment_id):
    return {
        "event_id": event_id,
        "event_type": "comment.deleted",
        "sent_at": "2026-08-10T09:14:22.481Z",
        "data": {"comment_id": comment_id},
    }


def check(label, condition):
    print(f"[{'PASS' if condition else 'FAIL'}] {label}")


def main():
    r = requests.post(
        f"{BASE}/rules",
        json={"keyword": "PRICE", "dm_message": "Here is the price list"},
    )
    check("create rule -> 201", r.status_code == 201)

    status, _ = send_event(created_event("evt_1", "PRICE please", "cmt_1", "usr_1"))
    check("normal matching event -> 200", status == 200)

    status, _ = send_event(created_event("evt_1", "PRICE please", "cmt_1", "usr_1"))
    check("redelivered event_id -> 200 (accepted, deduped internally)", status == 200)

    status, _ = send_event(created_event("evt_2", "PRICE again", "cmt_2", "usr_1"))
    check("same user, different comment, same rule -> 200 (but should be blocked as dup)", status == 200)

    status, _ = send_event(created_event("evt_3", "price??", "cmt_3", "usr_2"))
    check("different user -> 200", status == 200)

    raw = json.dumps(created_event("evt_4", "PRICE", "cmt_4", "usr_4")).encode()
    r = requests.post(
        f"{BASE}/webhook",
        data=raw,
        headers={"Content-Type": "application/json", "X-PseudoGram-Signature": "sha256=deadbeef"},
    )
    check("invalid signature -> 401", r.status_code == 401)

    status, _ = send_event(deleted_event("evt_5", "cmt_5"))
    check("delete event -> 200", status == 200)
    status, _ = send_event(created_event("evt_6", "PRICE for cmt5", "cmt_5", "usr_5"))
    check("create event for already-deleted comment -> 200 (but no DM should be queued)", status == 200)

    time.sleep(1.5)  # let background threads process

    stats = requests.get(f"{BASE}/stats").json()
    print("\n/stats:", stats)
    check("exactly 1 duplicate blocked (usr_1 second PRICE comment)", stats["duplicates_blocked"] == 1)
    check("2 tasks reached queued/pending (usr_1's first, usr_2's) -- deleted-comment one excluded",
          stats["queued"] == 2)


if __name__ == "__main__":
    main()
