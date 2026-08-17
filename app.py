"""
LinkPlease — Instagram comment-to-DM automation on top of the pseudogram mock API.

Architecture, in one paragraph:
Every webhook POST is written to SQLite BEFORE we do anything else, and we return 200
immediately. A background "processor" thread reads unprocessed events, matches them
against rules, and creates one dm_task per (user_id, rule_id) pair — a UNIQUE constraint
on that pair is what makes "never DM the same user twice for the same rule" true even
under concurrent/duplicate delivery, because SQLite enforces it atomically regardless of
which thread got there first. A single "sender" thread drains pending dm_tasks, respecting
the 10-req/60s rate limit, retrying 429/500 with backoff, and giving up (marking failed)
after MAX_RETRIES on 500s or immediately on 400s. A "poller" thread checks accepted
(202) DMs against GET /v1/dm/{id} until they reach a terminal status, because "sent" in
/stats must mean actually delivered, not just accepted. comment.deleted events are recorded
and checked before a DM is ever sent, and cancel any not-yet-sent task for that comment.

Everything that matters (rules, raw events, dm_tasks, dedupe counters) lives on disk in
SQLite, not in memory, so a process restart does not lose in-flight work (see FAILURES.md
for the real remaining edge cases).
"""

import hashlib
import hmac
import json
import os
import random
import sqlite3
import threading
import time
import uuid
from collections import deque
from datetime import datetime, timezone

import requests
from flask import Flask, jsonify, request

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

API_KEY = os.environ.get("PSEUDOGRAM_API_KEY", "")
BASE_URL = os.environ.get("PSEUDOGRAM_BASE_URL", "https://pseudogram-api.onrender.com")
DB_PATH = os.environ.get("LINKPLEASE_DB_PATH", "linkplease.db")

MAX_RETRIES = 5          # total attempts before we give up and mark failed —
                          # this budget is shared by submission failures (429/500/
                          # network) AND by post-accept delivery failures caught by
                          # the poller (Part C reconciliation). Either kind of
                          # failure counts against the same cap.
BASE_BACKOFF_SECONDS = 1  # doubles each attempt, capped below
MAX_BACKOFF_SECONDS = 30
RATE_LIMIT_PER_WINDOW = 10
RATE_LIMIT_WINDOW_SECONDS = 60
POLL_INTERVAL_SECONDS = 2

app = Flask(__name__)

# ---------------------------------------------------------------------------
# DB — one global connection + one global lock. See FAILURES.md: this trades
# write throughput for dead-simple correctness (no partial writes, no races
# on the dedupe checks). At 500 events/10s this is not the bottleneck.
# ---------------------------------------------------------------------------

_conn = sqlite3.connect(DB_PATH, check_same_thread=False)
_conn.execute("PRAGMA journal_mode=WAL;")
db_lock = threading.Lock()


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def init_db():
    with db_lock:
        c = _conn
        c.execute("""
            CREATE TABLE IF NOT EXISTS rules (
                rule_id TEXT PRIMARY KEY,
                keyword TEXT NOT NULL,
                dm_message TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS webhook_events (
                event_id TEXT PRIMARY KEY,
                event_type TEXT NOT NULL,
                payload TEXT NOT NULL,
                received_at TEXT NOT NULL,
                processed INTEGER NOT NULL DEFAULT 0
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS deleted_comments (
                comment_id TEXT PRIMARY KEY,
                deleted_at TEXT NOT NULL
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS dm_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                rule_id TEXT NOT NULL,
                comment_id TEXT NOT NULL,
                message TEXT NOT NULL,
                status TEXT NOT NULL,       -- pending, queued, delivered, failed, cancelled
                dm_id TEXT,
                attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt_at TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(user_id, rule_id)
            )
        """)
        # Cheap migration for DBs created before next_attempt_at existed.
        cols = [r[1] for r in c.execute("PRAGMA table_info(dm_tasks)").fetchall()]
        if "next_attempt_at" not in cols:
            c.execute("ALTER TABLE dm_tasks ADD COLUMN next_attempt_at TEXT NOT NULL DEFAULT ''")
        c.execute("""
            CREATE TABLE IF NOT EXISTS counters (
                name TEXT PRIMARY KEY,
                value INTEGER NOT NULL
            )
        """)
        c.execute("INSERT OR IGNORE INTO counters (name, value) VALUES ('duplicates_blocked', 0)")
        c.commit()


# ---------------------------------------------------------------------------
# Signature verification (Part B)
# ---------------------------------------------------------------------------

def verify_signature(raw_body: bytes, header_value: str) -> bool:
    if not header_value or not header_value.startswith("sha256="):
        return False
    if not API_KEY:
        # No key configured means we cannot verify anything — fail closed.
        return False
    provided = header_value.split("=", 1)[1]
    expected = hmac.new(API_KEY.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(provided, expected)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "linkplease"}), 200


@app.route("/webhook", methods=["POST"])
def webhook():
    raw = request.get_data()  # raw bytes, needed for signature verification
    sig_header = request.headers.get("X-PseudoGram-Signature", "")

    if not verify_signature(raw, sig_header):
        # --- TEMP DEBUG: figure out what secret pseudogram actually signs with ---
        provided = sig_header.split("=", 1)[1] if "=" in sig_header else sig_header
        candidates = {
            "full_key": API_KEY,
            "after_dot": API_KEY.split(".", 1)[1] if "." in API_KEY else None,
            "before_dot": API_KEY.split(".", 1)[0] if "." in API_KEY else None,
        }
        for label, secret in candidates.items():
            if not secret:
                continue
            guess = hmac.new(secret.encode("utf-8"), raw, hashlib.sha256).hexdigest()
            result = "MATCH" if hmac.compare_digest(guess, provided) else "no match"
            print(f"[sig-debug] {label}: {result}")
        print(f"[sig-debug] provided={provided!r} raw_len={len(raw)} raw_prefix={raw[:80]!r}")
        # --- END TEMP DEBUG ---
        return jsonify({"error": "invalid_signature"}), 401

    try:
        payload = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return jsonify({"error": "invalid_json"}), 400

    event_id = payload.get("event_id")
    event_type = payload.get("event_type")
    if not event_id or not event_type:
        return jsonify({"error": "invalid_payload"}), 400

    # Durably record the event FIRST, then return 200. All real work happens
    # in the background threads. INSERT OR IGNORE on the PRIMARY KEY is what
    # makes redelivered event_ids a no-op, atomically, even under concurrency.
    with db_lock:
        _conn.execute(
            "INSERT OR IGNORE INTO webhook_events (event_id, event_type, payload, received_at, processed) "
            "VALUES (?, ?, ?, ?, 0)",
            (event_id, event_type, raw.decode("utf-8"), now_iso()),
        )
        _conn.commit()

    return jsonify({"status": "received"}), 200


@app.route("/rules", methods=["POST"])
def create_rule():
    data = request.get_json(force=True, silent=True) or {}
    keyword = data.get("keyword")
    dm_message = data.get("dm_message")
    if not keyword or not dm_message:
        return jsonify({"error": "invalid_request", "detail": "keyword and dm_message are required"}), 400

    rule_id = str(uuid.uuid4())
    with db_lock:
        _conn.execute(
            "INSERT INTO rules (rule_id, keyword, dm_message, created_at) VALUES (?, ?, ?, ?)",
            (rule_id, keyword, dm_message, now_iso()),
        )
        _conn.commit()

    return jsonify({"rule_id": rule_id, "keyword": keyword, "dm_message": dm_message}), 201


@app.route("/rules", methods=["GET"])
def list_rules():
    # Not required by the spec, but harmless and useful for testing.
    with db_lock:
        rows = _conn.execute("SELECT rule_id, keyword, dm_message, created_at FROM rules").fetchall()
    return jsonify([
        {"rule_id": r[0], "keyword": r[1], "dm_message": r[2], "created_at": r[3]} for r in rows
    ]), 200


@app.route("/stats", methods=["GET"])
def stats():
    with db_lock:
        sent = _conn.execute("SELECT COUNT(*) FROM dm_tasks WHERE status = 'delivered'").fetchone()[0]
        failed = _conn.execute("SELECT COUNT(*) FROM dm_tasks WHERE status = 'failed'").fetchone()[0]
        queued = _conn.execute(
            "SELECT COUNT(*) FROM dm_tasks WHERE status IN ('pending', 'queued')"
        ).fetchone()[0]
        dup_row = _conn.execute("SELECT value FROM counters WHERE name = 'duplicates_blocked'").fetchone()
        duplicates_blocked = dup_row[0] if dup_row else 0

    return jsonify({
        "sent": sent,
        "failed": failed,
        "queued": queued,
        "duplicates_blocked": duplicates_blocked,
    }), 200


# ---------------------------------------------------------------------------
# Background: event processor — turns webhook_events into dm_tasks
# ---------------------------------------------------------------------------

def process_event(event_id, event_type, payload_raw):
    payload = json.loads(payload_raw)
    data = payload.get("data", {}) or {}

    with db_lock:
        try:
            if event_type == "comment.deleted":
                comment_id = data.get("comment_id")
                if comment_id:
                    _conn.execute(
                        "INSERT OR IGNORE INTO deleted_comments (comment_id, deleted_at) VALUES (?, ?)",
                        (comment_id, now_iso()),
                    )
                    # Cancel anything not yet sent for this comment.
                    _conn.execute(
                        "UPDATE dm_tasks SET status = 'cancelled', updated_at = ? "
                        "WHERE comment_id = ? AND status = 'pending'",
                        (now_iso(), comment_id),
                    )

            elif event_type == "comment.created":
                comment_id = data.get("comment_id")
                text = (data.get("text") or "")
                from_block = data.get("from") or {}
                user_id = from_block.get("user_id")

                already_deleted = None
                if comment_id:
                    already_deleted = _conn.execute(
                        "SELECT 1 FROM deleted_comments WHERE comment_id = ?", (comment_id,)
                    ).fetchone()

                if user_id and comment_id and not already_deleted:
                    text_lower = text.lower()
                    rule_rows = _conn.execute("SELECT rule_id, keyword, dm_message FROM rules").fetchall()
                    for rule_id, keyword, dm_message in rule_rows:
                        if keyword.lower() in text_lower:
                            cur = _conn.execute(
                                "INSERT OR IGNORE INTO dm_tasks "
                                "(user_id, rule_id, comment_id, message, status, attempts, "
                                " next_attempt_at, created_at, updated_at) "
                                "VALUES (?, ?, ?, ?, 'pending', 0, ?, ?, ?)",
                                (user_id, rule_id, comment_id, dm_message, now_iso(), now_iso(), now_iso()),
                            )
                            if cur.rowcount == 0:
                                # (user_id, rule_id) already had a task — correctly not sending again.
                                _conn.execute(
                                    "UPDATE counters SET value = value + 1 WHERE name = 'duplicates_blocked'"
                                )

            _conn.execute("UPDATE webhook_events SET processed = 1 WHERE event_id = ?", (event_id,))
            _conn.commit()
        except Exception:
            _conn.rollback()
            raise


def processor_loop():
    while True:
        with db_lock:
            rows = _conn.execute(
                "SELECT event_id, event_type, payload FROM webhook_events WHERE processed = 0 LIMIT 50"
            ).fetchall()
        if not rows:
            time.sleep(0.2)
            continue
        for event_id, event_type, payload_raw in rows:
            try:
                process_event(event_id, event_type, payload_raw)
            except Exception as e:
                # Don't let one bad event wedge the whole loop; it stays
                # unprocessed and will be retried next pass.
                print(f"[processor] error on event {event_id}: {e}")
                time.sleep(0.1)


# ---------------------------------------------------------------------------
# Background: sender — drains pending dm_tasks, respects rate limit, retries
# ---------------------------------------------------------------------------

_send_timestamps = deque(maxlen=RATE_LIMIT_PER_WINDOW)
_rate_lock = threading.Lock()
_global_pause_until = 0.0  # epoch seconds; set when pseudogram itself 429s us


def rate_limit_wait():
    with _rate_lock:
        now = time.time()
        if len(_send_timestamps) == RATE_LIMIT_PER_WINDOW:
            oldest = _send_timestamps[0]
            wait = RATE_LIMIT_WINDOW_SECONDS - (now - oldest)
            if wait > 0:
                time.sleep(wait)
        _send_timestamps.append(time.time())


def backoff_seconds(attempts):
    return min(BASE_BACKOFF_SECONDS * (2 ** attempts), MAX_BACKOFF_SECONDS)


def mark_status(task_id, status, dm_id=None):
    with db_lock:
        if dm_id is not None:
            _conn.execute(
                "UPDATE dm_tasks SET status = ?, dm_id = ?, updated_at = ? WHERE id = ?",
                (status, dm_id, now_iso(), task_id),
            )
        else:
            _conn.execute(
                "UPDATE dm_tasks SET status = ?, updated_at = ? WHERE id = ?",
                (status, now_iso(), task_id),
            )
        _conn.commit()


def _next_attempt_iso(new_attempts):
    # Exponential backoff + a little jitter so retries (including a batch of
    # them from a burst) don't all wake up on exactly the same tick.
    delay = backoff_seconds(new_attempts) + random.uniform(0, 1)
    return datetime.fromtimestamp(time.time() + delay, tz=timezone.utc).isoformat()


def bump_attempts_and_maybe_fail(task_id, attempts):
    """Used for pre-accept failures: network errors, 429 exhaustion (not used,
    429 is handled separately), and 500s from POST /v1/dm/send itself."""
    new_attempts = attempts + 1
    with db_lock:
        if new_attempts >= MAX_RETRIES:
            _conn.execute(
                "UPDATE dm_tasks SET status = 'failed', attempts = ?, updated_at = ? WHERE id = ?",
                (new_attempts, now_iso(), task_id),
            )
        else:
            # Stay 'pending' but not eligible again until next_attempt_at —
            # non-blocking, so the sender thread keeps draining OTHER pending
            # tasks in the meantime instead of stalling behind this backoff.
            _conn.execute(
                "UPDATE dm_tasks SET attempts = ?, next_attempt_at = ?, updated_at = ? WHERE id = ?",
                (new_attempts, _next_attempt_iso(new_attempts), now_iso(), task_id),
            )
        _conn.commit()


def schedule_post_accept_retry(task_id, attempts):
    """Used for Part C reconciliation: the API accepted the DM (202) but the
    poller later saw it land in terminal status 'failed'. Re-queue for another
    real send attempt (not just another status check) if budget remains."""
    new_attempts = attempts + 1
    with db_lock:
        if new_attempts >= MAX_RETRIES:
            _conn.execute(
                "UPDATE dm_tasks SET status = 'failed', attempts = ?, updated_at = ? WHERE id = ?",
                (new_attempts, now_iso(), task_id),
            )
        else:
            _conn.execute(
                "UPDATE dm_tasks SET status = 'pending', attempts = ?, "
                "next_attempt_at = ?, updated_at = ? WHERE id = ?",
                (new_attempts, _next_attempt_iso(new_attempts), now_iso(), task_id),
            )
        _conn.commit()


def sender_loop():
    global _global_pause_until
    while True:
        remaining_pause = _global_pause_until - time.time()
        if remaining_pause > 0:
            time.sleep(min(remaining_pause, 1.0))
            continue

        with db_lock:
            row = _conn.execute(
                "SELECT id, user_id, rule_id, comment_id, message, attempts FROM dm_tasks "
                "WHERE status = 'pending' AND next_attempt_at <= ? "
                "ORDER BY created_at LIMIT 1",
                (now_iso(),),
            ).fetchone()

        if not row:
            time.sleep(0.3)
            continue

        task_id, user_id, rule_id, comment_id, message, attempts = row

        with db_lock:
            deleted = _conn.execute(
                "SELECT 1 FROM deleted_comments WHERE comment_id = ?", (comment_id,)
            ).fetchone()
        if deleted:
            mark_status(task_id, "cancelled")
            continue

        rate_limit_wait()

        # Idempotency key is scoped to THIS attempt number, not just the task.
        # Reusing the same key across a crash+restart of the same attempt is
        # exactly what protects against a duplicate real send (see FAILURES.md).
        # But reusing it for a NEW attempt after pseudogram already terminally
        # failed the previous one would just hand back the same failed dm_id
        # forever — so each attempt gets its own key.
        idempotency_key = f"task-{task_id}-{attempts}"

        try:
            resp = requests.post(
                f"{BASE_URL}/v1/dm/send",
                json={"recipient_user_id": user_id, "message": message, "comment_id": comment_id},
                headers={"X-API-Key": API_KEY, "Idempotency-Key": idempotency_key},
                timeout=10,
            )
        except requests.RequestException as e:
            print(f"[sender] network error on task {task_id}: {e}")
            bump_attempts_and_maybe_fail(task_id, attempts)
            continue

        if resp.status_code == 202:
            body = resp.json()
            mark_status(task_id, "queued", dm_id=body.get("dm_id"))
        elif resp.status_code == 429:
            # This is a signal about the WHOLE key, not just this task — pause
            # the entire sender rather than just rescheduling this one row,
            # otherwise the next loop iteration just grabs a different pending
            # task and gets 429'd again immediately.
            retry_after = int(resp.headers.get("Retry-After", "5"))
            with _rate_lock:
                _global_pause_until = max(_global_pause_until, time.time() + retry_after)
        elif resp.status_code == 500:
            bump_attempts_and_maybe_fail(task_id, attempts)
        elif resp.status_code == 400:
            mark_status(task_id, "failed")
        else:
            bump_attempts_and_maybe_fail(task_id, attempts)


# ---------------------------------------------------------------------------
# Background: poller — checks accepted (202) DMs until they reach a terminal
# status, so "sent" in /stats means actually delivered, not just accepted.
# ---------------------------------------------------------------------------

def poller_loop():
    while True:
        with db_lock:
            rows = _conn.execute(
                "SELECT id, dm_id, attempts FROM dm_tasks WHERE status = 'queued' AND dm_id IS NOT NULL LIMIT 30"
            ).fetchall()

        for task_id, dm_id, attempts in rows:
            try:
                resp = requests.get(
                    f"{BASE_URL}/v1/dm/{dm_id}",
                    headers={"X-API-Key": API_KEY},
                    timeout=10,
                )
            except requests.RequestException as e:
                print(f"[poller] network error checking dm {dm_id}: {e}")
                continue

            if resp.status_code == 200:
                status = resp.json().get("status")
                if status == "delivered":
                    mark_status(task_id, "delivered")
                elif status == "failed":
                    # Part C: pseudogram accepted this DM (202) but it later
                    # landed as 'failed'. That's not the same as our own
                    # submission failing — the request itself succeeded, the
                    # delivery didn't. Retry it as a fresh send attempt
                    # (new idempotency key) instead of giving up, as long as
                    # budget remains; schedule_post_accept_retry marks it
                    # terminally 'failed' once MAX_RETRIES is hit.
                    schedule_post_accept_retry(task_id, attempts)
                # if still "queued", leave it — we'll check again next pass

        time.sleep(POLL_INTERVAL_SECONDS)


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------

_threads_started = False
_start_lock = threading.Lock()


def start_background_threads():
    global _threads_started
    with _start_lock:
        if _threads_started:
            return
        init_db()
        threading.Thread(target=processor_loop, daemon=True, name="processor").start()
        threading.Thread(target=sender_loop, daemon=True, name="sender").start()
        threading.Thread(target=poller_loop, daemon=True, name="poller").start()
        _threads_started = True


start_background_threads()  # runs on import, so gunicorn picks it up too

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, threaded=True)
