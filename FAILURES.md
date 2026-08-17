# Known failure modes

Honest list, most to least likely to actually bite:

- **Post-accept retry (Part C) still has a race at the very edge of the retry budget.**
  `schedule_post_accept_retry` reads `attempts`, decides whether to retry or give up, and
  writes the new state — all under `db_lock`, so it's atomic against the sender/processor.
  But the *decision to stop retrying* is just "attempts >= MAX_RETRIES (5)", shared with
  pre-accept submission failures. A DM that fails 500 twice on submission and then gets
  accepted-but-fails-delivery three more times hits the same cap as one that fails
  delivery five times in a row. That's a deliberate simplification (one retry budget, not
  two separate ones) — documented here in case it reads as a bug: it isn't, but it does
  mean the failure reason in `FAILURES.md`-style debugging ("why did this stop retrying")
  isn't visible from `/stats` alone, only from the `dm_tasks` table.

- **Two processes/workers would double-send.** Dedup (`UNIQUE(user_id, rule_id)` in SQLite,
  `PRIMARY KEY(event_id)` on webhook events) is correct *within one process* because it's
  enforced by SQLite's own locking. If this were ever deployed with more than one gunicorn
  worker, or scaled to more than one instance, each process would run its own background
  threads against the same DB file. SQLite would still stop duplicate rows from being
  *created*, but two processes could each pick up the same `pending` dm_task in the small
  window between `SELECT` and marking it `sending`, and both call `/v1/dm/send` for it. I
  deploy with exactly one worker (`Procfile` enforces `-w 1`) specifically to avoid this,
  which is a real scaling limit, not just a config note.

- **A crash between "API accepted the DM" and "I wrote down the dm_id" loses the record but
  not the DM.** If the process dies in the few milliseconds after `POST /v1/dm/send` returns
  202 but before `mark_status(..., dm_id=...)` commits, pseudogram has genuinely sent (or
  will send) that DM, but my DB still shows the task as `pending`. On restart the sender will
  pick it up again and send a *second* real DM to that user, because from my side it looks
  like it never went out. The `Idempotency-Key` I send is scoped per *attempt*
  (`task-{id}-{attempts}`), not just per task — this is what makes Part C's retry-after-
  delivery-failure safe (retrying with the SAME key as a terminally-failed attempt would
  just hand back the same failed `dm_id` forever, so each attempt needs its own key). That
  also means a crash-restart of one specific attempt reuses that attempt's key and is
  protected, exactly as before — I still haven't verified how long pseudogram holds
  idempotency keys, so I'm not relying on that window being long, just noting it's the same
  mechanism as before, now applied per-attempt instead of per-task.

- **SQLite disk on Render's free tier is not guaranteed to survive a redeploy.** If the
  service redeploys mid-run, `linkplease.db` can come back empty: rules are gone, and any
  webhook events/dm_tasks that were in `pending`/`queued` are gone with it. DMs that
  pseudogram already delivered stay delivered on their side, but my `/stats` would no longer
  know about them. For a real deployment this needs a persistent volume or a real Postgres
  instance, not local SQLite.

- **`comment.deleted` racing a not-yet-processed `comment.created` for the same comment is
  handled, but not stress-tested against adversarial interleaving.** The check is "is this
  comment_id in `deleted_comments` at the moment I'm about to create the dm_task" — that
  covers delete-before-create and delete-after-task-created-but-before-sent (I cancel
  `pending` tasks on delete). What I have *not* tested is a delete event landing in the
  half-second window between the sender thread reading a task as `pending` and it actually
  making the outbound HTTP call — in that narrow window a DM could still go out for a comment
  that's technically already deleted. Rare, but real.

- **One global `threading.Lock()` around all SQLite access serializes every read and write.**
  This is intentional (see the Loom) — it removes an entire class of race conditions for the
  cost of some throughput — but it means DB contention, not the mock API's rate limit, would
  be the actual bottleneck if load went materially above what was tested (500 events / 10s).

- **Pseudogram signs webhooks with the base64-decoded email from the API key, not the
  literal API key string** — contradicts the docs ("HMAC-SHA256 of the raw request body
  using your API key as the secret"). Found via debug logging that tried the full key, and
  the substrings before/after the `.`, against the real signature on 5 separate real
  requests — only the decoded email matched, every time. verify_signature() now derives
  the signing secret by decoding the email from the key's prefix.

- **`/v1/dm/send` actually returns HTTP `200` with an already-terminal status embedded
  in the body** (e.g. `{"dm_id":...,"status":"delivered"}`), not `202` with
  `{"status":"queued"}` as documented. My original code only recognized `202` as success,
  so every real `200` response fell into the retry path — meaning a task that had ALREADY
  been delivered got retried anyway, sending a genuine duplicate real DM to the same
  user. Confirmed via debug logging showing 3 separate real dm_ids issued for what should
  have been 1 send. Fixed by checking both status codes and always reading `status` from
  the body.
