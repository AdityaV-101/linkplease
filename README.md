# LinkPlease

Comment-to-DM automation on top of the pseudogram mock Instagram API.

Implements **Part A** (rules, matching, no double-DM, no silent DM loss), **Part B**
(webhook signature verification, accurate live `/stats`), and **Part C** (delivery
reconciliation *with retry*, `comment.deleted` handling, rate-limit safety under a
500-event burst) — see `FAILURES.md` for what's not fully covered.

## How it works

- `POST /webhook` writes the event to SQLite and returns `200` immediately — all real work
  happens in three background threads (processor, sender, poller), so nothing blocks the
  5-second webhook deadline.
- **Processor** matches `comment.created` events against rules and creates one `dm_task` per
  `(user_id, rule_id)` pair. A SQLite `UNIQUE` constraint on that pair is what actually
  guarantees "never DM the same user twice for the same rule" — it's enforced atomically by
  the DB, not by application logic that could race.
- **Sender** drains pending tasks one at a time, respects the 10-req/60s rate limit, retries
  `500`s with exponential backoff (up to 5 attempts), respects `Retry-After` on `429`, and
  gives up immediately on `400` (no point retrying a malformed request).
- **Poller** checks every accepted (`202`) DM against `GET /v1/dm/{id}`. If it reaches
  `delivered`, the task is done. If it reaches `failed` — pseudogram accepted the request
  but the DM didn't land — **the task is retried as a brand-new send attempt** (Part C),
  not just marked dead. `sent` in `/stats` means *delivered*, never just accepted.
- Each send attempt gets its own `Idempotency-Key` (`task-{id}-{attempts}`), not one key
  per task. That's what makes the retry-after-delivery-failure case correct: reusing the
  same key on a retry would just hand back the same already-failed `dm_id` forever.
- Retries (both pre-accept submission failures and post-accept delivery failures) are
  scheduled via a `next_attempt_at` column and picked up non-blockingly by the sender loop
  — a task in backoff doesn't stall every other pending task behind it, which matters
  under the 500-events/10s burst test.
- A real `429` from pseudogram pauses the *entire* sender (it's a signal about the whole
  API key, not one task) rather than just rescheduling the one request that got limited.
- `comment.deleted` events are recorded and checked before any DM is created or sent; a
  not-yet-sent task for a deleted comment is cancelled.
- Every task gets up to `MAX_RETRIES` (5) real send attempts total, whether the failures
  are pre-accept (429/500/network) or post-accept (delivered check comes back `failed`).
  After that it's marked terminally `failed` and counted as such in `/stats`.

Everything lives in SQLite on disk (`linkplease.db`), not in memory, so a process restart
doesn't silently lose in-flight work (with the caveats in `FAILURES.md`).

## Setup

```bash
pip install -r requirements.txt
export PSEUDOGRAM_API_KEY=your_key_here
export PSEUDOGRAM_BASE_URL=https://pseudogram-api.onrender.com   # default, can omit
python app.py            # runs on :5000, or set PORT
```

Getting a key:

```bash
curl -X POST https://pseudogram-api.onrender.com/v1/apply \
  -H "Content-Type: application/json" \
  -d '{"name":"you","email":"you@example.com","phone":"+91...","linkedin_url":"https://linkedin.com/in/you"}'

curl -X POST https://pseudogram-api.onrender.com/v1/keygen \
  -H "Content-Type: application/json" \
  -d '{"email":"you@example.com"}'
```

## Deploying (Render)

- Build command: `pip install -r requirements.txt`
- Start command: use the one in `Procfile` — **`gunicorn -w 1 --worker-class gthread --threads 8 --timeout 60 app:app`**
- Set env vars `PSEUDOGRAM_API_KEY` and (optionally) `PSEUDOGRAM_BASE_URL`.
- **`-w 1` is not optional.** The dedup logic and background threads assume one process
  owns the SQLite file. Running multiple workers/instances will cause duplicate sends —
  see `FAILURES.md`.

## Testing

1. **Create a rule:**
   ```bash
   curl -X POST https://your-app.example.com/rules \
     -H "Content-Type: application/json" \
     -d '{"keyword":"PRICE","dm_message":"Here is the price list: ..."}'
   ```

2. **Sanity-check the webhook by hand** (optional — see `test_local.py` below for an
   automated version): send a `comment.created` event with a valid HMAC-SHA256 signature
   (your API key as secret, over the raw JSON body) and confirm `200`.

3. **Run the real simulation:**
   ```bash
   curl -X POST https://pseudogram-api.onrender.com/v1/simulate/start \
     -H "X-API-Key: $PSEUDOGRAM_API_KEY" -H "Content-Type: application/json" \
     -d '{"webhook_url":"https://your-app.example.com/webhook","count":500,"duration_seconds":10}'
   ```
   This returns a `run_id`.

4. **Watch it drain** — `/stats` should settle (queued → 0) within a minute or two after the
   burst ends, since the rate limit caps throughput to 10 sends/60s:
   ```bash
   watch -n 2 curl -s https://your-app.example.com/stats
   ```

5. **Compare against ground truth:**
   ```bash
   curl -s https://pseudogram-api.onrender.com/v1/simulate/$RUN_ID/truth \
     -H "X-API-Key: $PSEUDOGRAM_API_KEY" | less
   ```
   Cross-check `sent + failed + duplicates_blocked` against what the truth endpoint says
   should have matched, and check no `queued` count is stuck non-zero indefinitely.

### `test_local.py`

A small script (included) that hits a running instance locally with signed webhook events
to exercise: normal match, redelivered `event_id`, duplicate user+rule, invalid signature,
and `comment.deleted` racing a `comment.created`. Run it against a local `python app.py`
before deploying — it's what I used to verify the behavior in the Loom.

```bash
python app.py &                 # in one terminal, with PSEUDOGRAM_API_KEY set
python test_local.py            # in another
```
