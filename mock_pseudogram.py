"""
Tiny stand-in for pseudogram, just enough to exercise the Part C retry path:
a DM gets accepted (202), then GET shows it as 'failed' on the first two
polls for one specific dm_id, then 'delivered' after that -- simulating
"accepted, but later failed, then a retry succeeds."
"""
import itertools
from flask import Flask, jsonify, request

app = Flask(__name__)
_counter = itertools.count(1)
_dms = {}          # dm_id -> {"status": ..., "poll_count": 0}
_force_fail_first_n = 2  # first dm_id created will fail this many polls


@app.route("/v1/dm/send", methods=["POST"])
def send():
    dm_id = f"dm_{next(_counter)}"
    # Make the FIRST dm_id ever created simulate "accepted then fails twice
    # before eventually delivering on retry" -- everything else delivers
    # immediately on first poll.
    is_flaky = (dm_id == "dm_1")
    _dms[dm_id] = {"status": "queued", "poll_count": 0, "flaky": is_flaky}
    return jsonify({"dm_id": dm_id, "status": "queued"}), 202


@app.route("/v1/dm/<dm_id>", methods=["GET"])
def get_dm(dm_id):
    d = _dms.get(dm_id)
    if not d:
        return jsonify({"error": "not_found"}), 404
    d["poll_count"] += 1
    if d["flaky"] and d["poll_count"] <= 1:
        status = "failed"
    else:
        status = "delivered"
    return jsonify({"dm_id": dm_id, "status": status, "recipient_user_id": "usr_x",
                     "updated_at": "2026-08-10T09:14:31.002Z"}), 200


if __name__ == "__main__":
    app.run(port=6000)
