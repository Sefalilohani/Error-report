import os
import requests
from datetime import datetime, timezone, timedelta
from collections import defaultdict

# ── CONFIG ─────────────────────────────────────────────

SLACK_TOKEN = os.environ["SLACK_BOT_TOKEN"]
REDASH_API_KEY = os.environ["REDASH_API_KEY"]

REDASH_QUERY_ID = 1528
REDASH_BASE = "https://redash.springworks.in"

OPS_CHANNEL_ID = "C0AGRE19V6U"   # #testing-sefali

REPORT_TYPE = os.environ.get("REPORT_TYPE", "9am")

THREAD_FILE = "thread_ts.txt"

IST = timezone(timedelta(hours=5, minutes=30))


# ── FETCH REDASH DATA ──────────────────────────────────

def fetch_redash():

    url = f"{REDASH_BASE}/api/queries/{REDASH_QUERY_ID}/results"

    headers = {
        "Authorization": f"Key {REDASH_API_KEY}",
        "Content-Type": "application/json"
    }

    payload = {
        "parameters": {
            "p_check_type": ["ALL"],
            "p_created_at": "2026-02-01 00:00:00--2026-03-08 23:59:59",
            "p_department": ["OPERATIONS"],
            "p_error_status": ["NEW","UNDER_DISCUSSION"],
            "p_user_email": ["ALL"]
        }
    }

    r = requests.post(url, headers=headers, json=payload)

    r.raise_for_status()

    data = r.json()

    return data["query_result"]["data"]["rows"]


# ── SLACK USERS ────────────────────────────────────────

def get_slack_users():

    url = "https://slack.com/api/users.list"

    r = requests.get(
        url,
        headers={"Authorization": f"Bearer {SLACK_TOKEN}"}
    )

    data = r.json()

    users = {}

    for u in data["members"]:
        email = u.get("profile", {}).get("email")

        if email:
            users[email.lower()] = u["id"]

    return users


# ── POST TO SLACK ──────────────────────────────────────

def post_slack(text, thread_ts=None):

    payload = {
        "channel": OPS_CHANNEL_ID,
        "text": text
    }

    if thread_ts:
        payload["thread_ts"] = thread_ts

    r = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={
            "Authorization": f"Bearer {SLACK_TOKEN}",
            "Content-Type": "application/json"
        },
        json=payload
    )

    r.raise_for_status()

    return r.json()["ts"]


# ── BUILD REPORT MESSAGE ───────────────────────────────

def build_report(rows, slack_users):

    counts = defaultdict(int)

    for r in rows:
        email = (r.get("user_email") or "").lower()

        counts[email] += 1

    sorted_agents = sorted(
        counts.items(),
        key=lambda x: x[1],
        reverse=True
    )

    lines = []

    for i, (email, count) in enumerate(sorted_agents, 1):

        slack_id = slack_users.get(email)

        if slack_id:
            mention = f"<@{slack_id}>"
        else:
            mention = email

        lines.append(f"{i}. {mention} - {count}")

    total = sum(counts.values())

    today = datetime.now(IST).strftime("%d %b %Y")

    text = (
        f"🚨 *Daily Error Report – {today}*\n\n"
        f"Status: NEW & UNDER_DISCUSSION | Department: OPERATIONS\n\n"
        + "\n".join(lines)
        + f"\n\nTotal - {total}\n"
    )

    return text


# ── MAIN REPORT LOGIC ──────────────────────────────────

def run_report():

    print("Fetching Redash data...")

    rows = fetch_redash()

    print("Fetching Slack users...")

    slack_users = get_slack_users()

    message = build_report(rows, slack_users)

    if REPORT_TYPE == "9am":

        print("Posting new Slack message")

        ts = post_slack(message)

        with open(THREAD_FILE, "w") as f:
            f.write(ts)

    else:

        print("Replying in Slack thread")

        with open(THREAD_FILE) as f:
            ts = f.read().strip()

        post_slack(message, ts)


# ── ENTRY ──────────────────────────────────────────────

if __name__ == "__main__":

    run_report()
